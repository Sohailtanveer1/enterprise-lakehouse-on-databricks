-- Unity Catalog column masks and row filters.
--
-- This is where Unity Catalog stops being a catalogue and becomes an
-- enforcement point. The policy lives with the data, so it applies to every
-- query through every tool — not just the ones that remembered to filter.
-- That is the substantive difference from Dataplex, where enforcement still
-- happens in IAM on the underlying resource (ARCHITECTURE.md §11).
--
-- Applied to GOLD only. Silver stays unmasked so engineering and audit paths
-- are unimpeded, and Bronze is not readable by analysts at all.
--
-- Usage:  spark.sql(...) via notebooks/ops/apply_governance.py
--         :catalog is substituted at apply time.

USE CATALOG ${catalog};

-- ---------------------------------------------------------------- functions

-- is_account_group_member() is evaluated per query against the caller's
-- identity. A mask that took the role as an argument could be bypassed by
-- passing a different one.
CREATE OR REPLACE FUNCTION gold.mask_email(email STRING)
RETURN CASE
  WHEN is_account_group_member('northpeak_pii_readers') THEN email
  -- Domain preserved deliberately: "which email providers do our customers
  -- use?" stays answerable while the identity does not. Full redaction would
  -- destroy analytical value for no extra protection.
  WHEN email IS NULL THEN NULL
  WHEN email LIKE '%@%' THEN
    concat(substr(sha2(email, 256), 1, 12), '@', split_part(email, '@', 2))
  ELSE '***'
END;

CREATE OR REPLACE FUNCTION gold.mask_name(full_name STRING)
RETURN CASE
  WHEN is_account_group_member('northpeak_pii_readers') THEN full_name
  WHEN full_name IS NULL THEN NULL
  -- Initials only. Enough to distinguish two customers in a support ticket,
  -- not enough to identify one.
  ELSE concat_ws(' ', transform(split(full_name, ' '), p -> concat(substr(p, 1, 1), '.')))
END;

CREATE OR REPLACE FUNCTION gold.mask_postal(postal_code STRING)
RETURN CASE
  WHEN is_account_group_member('northpeak_pii_readers') THEN postal_code
  WHEN postal_code IS NULL THEN NULL
  -- First 3 characters keep regional analysis viable; the full code is a
  -- household-level identifier when combined with anything else.
  ELSE concat(substr(postal_code, 1, 3), '**')
END;

-- Product cost is P2: not personal, but commercially confidential. Exposing it
-- exposes margin on every SKU.
CREATE OR REPLACE FUNCTION gold.mask_cost(cost DECIMAL(10,2))
RETURN CASE
  WHEN is_account_group_member('northpeak_finance')
    OR is_account_group_member('northpeak_data_engineers') THEN cost
  ELSE NULL
END;

-- Row filter: soft-deleted customers disappear for analysts.
--
-- SIMULATED right-to-erasure. A genuine implementation must hard-delete across
-- Bronze, Silver, Gold AND all time-travel history — which conflicts directly
-- with immutable Bronze. That tension is a real design problem, discussed in
-- SECURITY.md §9 rather than pretended solved.
CREATE OR REPLACE FUNCTION gold.filter_deleted_customers(is_deleted BOOLEAN)
RETURN is_account_group_member('northpeak_data_engineers')
    OR is_account_group_member('northpeak_pii_readers')
    OR NOT coalesce(is_deleted, false);

-- ------------------------------------------------------------------- apply

ALTER TABLE gold.dim_customers ALTER COLUMN email        SET MASK gold.mask_email;
ALTER TABLE gold.dim_customers ALTER COLUMN postal_code  SET MASK gold.mask_postal;
ALTER TABLE gold.dim_products  ALTER COLUMN cost         SET MASK gold.mask_cost;

ALTER TABLE gold.dim_customers SET ROW FILTER gold.filter_deleted_customers ON (is_deleted);

-- -------------------------------------------------------- curated views

-- business_user reads views, never base tables. A view is a stable contract:
-- the fact can be restructured without breaking every dashboard, and columns
-- that should never reach a dashboard simply are not selected.
CREATE OR REPLACE VIEW gold.v_sales_summary
COMMENT 'Business-facing sales. No PII columns selected at all.'
AS SELECT
  d.full_date, d.year, d.month_name, d.quarter,
  c.region, c.customer_segment,
  p.product_name, p.brand,
  cat.category_name,
  f.quantity, f.gross_amount, f.discount_amount, f.net_amount
FROM gold.fact_sales f
JOIN gold.dim_date       d   ON f.order_date_sk = d.date_sk
LEFT JOIN gold.dim_customers  c   ON f.customer_sk  = c.customers_sk
LEFT JOIN gold.dim_products   p   ON f.product_sk   = p.products_sk
LEFT JOIN gold.dim_categories cat ON f.category_sk  = cat.categories_sk;

CREATE OR REPLACE VIEW gold.v_inventory_health
COMMENT 'Stock position. quantity_on_hand is NON-ADDITIVE over time - average across days, never sum.'
AS SELECT
  d.full_date, s.store_name, s.region, p.product_name, p.brand,
  i.quantity_on_hand, i.quantity_available, i.reorder_point,
  i.is_stockout, i.is_below_reorder
FROM gold.fact_inventory_snapshot i
JOIN gold.dim_date     d ON i.snapshot_date_sk = d.date_sk
LEFT JOIN gold.dim_products p ON i.product_sk = p.products_sk
LEFT JOIN gold.dim_stores   s ON i.store_sk   = s.stores_sk;
