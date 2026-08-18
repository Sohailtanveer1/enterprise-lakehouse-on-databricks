-- Unity Catalog tags and comments — data classification and discovery.
--
-- Tags are how "which tables contain PII?" becomes a query instead of a
-- meeting. They are also what a masking policy should be driven from in a
-- larger estate: tag the column once, and policy follows the tag rather than
-- an ever-growing list of ALTER statements.
--
-- Classification levels are defined in SECURITY.md §1:
--   P0  direct identifier          masked for everyone but pii_reader
--   P1  indirect identifier        masked or truncated in Gold
--   P2  sensitive commercial       finance and merch only
--   P3  non-sensitive              unrestricted

USE CATALOG ${catalog};

-- ------------------------------------------------------- table-level tags

ALTER TABLE gold.dim_customers SET TAGS (
  'data_classification' = 'P0',
  'contains_pii'        = 'true',
  'domain'              = 'customer',
  'scd_type'            = 'type_2'
);

ALTER TABLE gold.dim_products SET TAGS (
  'data_classification' = 'P2',
  'contains_pii'        = 'false',
  'domain'              = 'product',
  'scd_type'            = 'type_2'
);

ALTER TABLE gold.dim_stores SET TAGS (
  'data_classification' = 'P3', 'domain' = 'reference', 'scd_type' = 'type_2'
);
ALTER TABLE gold.dim_date SET TAGS (
  'data_classification' = 'P3', 'domain' = 'reference'
);

ALTER TABLE gold.fact_sales SET TAGS (
  'data_classification' = 'P3',
  'domain'              = 'sales',
  'fact_type'           = 'transaction',
  -- Grain as a tag, not just a comment: it is the first thing anyone needs to
  -- know before writing an aggregate, and the first thing they get wrong.
  'grain'               = 'one order line'
);

ALTER TABLE gold.fact_order_fulfillment SET TAGS (
  'data_classification' = 'P3', 'domain' = 'sales',
  'fact_type' = 'accumulating_snapshot', 'grain' = 'one order'
);

ALTER TABLE gold.fact_inventory_snapshot SET TAGS (
  'data_classification' = 'P3', 'domain' = 'inventory',
  'fact_type' = 'periodic_snapshot',
  'grain' = 'one product x one location x one day',
  -- The single most common inventory reporting error, flagged where a query
  -- author will actually see it.
  'warning' = 'quantity measures are NON-ADDITIVE over time - average, never sum'
);

ALTER TABLE gold.fact_customer_events SET TAGS (
  'data_classification' = 'P1', 'domain' = 'clickstream',
  'fact_type' = 'transaction', 'grain' = 'one event'
);

-- ------------------------------------------------------ column-level tags

ALTER TABLE gold.dim_customers ALTER COLUMN email
  SET TAGS ('data_classification' = 'P0', 'pii_type' = 'email', 'masked' = 'true');
ALTER TABLE gold.dim_customers ALTER COLUMN postal_code
  SET TAGS ('data_classification' = 'P1', 'pii_type' = 'location', 'masked' = 'true');
ALTER TABLE gold.dim_customers ALTER COLUMN city
  SET TAGS ('data_classification' = 'P1', 'pii_type' = 'location');
ALTER TABLE gold.dim_customers ALTER COLUMN customer_id
  SET TAGS ('data_classification' = 'P1', 'pii_type' = 'pseudonymous_key');

ALTER TABLE gold.dim_products ALTER COLUMN cost
  SET TAGS ('data_classification' = 'P2', 'sensitivity' = 'commercial', 'masked' = 'true');
ALTER TABLE gold.dim_products ALTER COLUMN supplier
  SET TAGS ('data_classification' = 'P2', 'sensitivity' = 'commercial');

-- ------------------------------------------------------------- comments

COMMENT ON TABLE gold.fact_sales IS
  'Transaction fact. GRAIN: one line of one order. Order-level revenue is a rollup - there is deliberately no header fact, because it would be nothing but a SUM of these lines and the two would eventually disagree. Money is DECIMAL(18,2); net_amount excludes tax.';

COMMENT ON TABLE gold.fact_order_fulfillment IS
  'Accumulating snapshot. GRAIN: one order, updated as milestones occur. Carries the timestamps that cannot be derived from the lines. hours_to_ship and hours_to_deliver are semi-additive: average them, never sum them.';

COMMENT ON TABLE gold.fact_inventory_snapshot IS
  'Periodic snapshot. GRAIN: one product x one location x one day. quantity_on_hand is NON-ADDITIVE over time - summing 30 daily snapshots of 100 units yields 3,000 units that never existed. Additive across products on a single day.';

COMMENT ON TABLE gold.dim_customers IS
  'SCD Type 2 on city, state, region and customer_segment. Facts must join AS OF their event date using effective_start_ts and effective_end_ts, NOT on is_current - joining on is_current attributes historical orders to the customer''s present region and silently restates history.';

COMMENT ON TABLE gold.dim_products IS
  'SCD Type 2 on price, category_id and status, so a historical order keeps the price that applied when it was placed.';

-- -------------------------------------------------------- discovery query

-- The payoff. "Which columns contain PII, and are they masked?" — one query.
CREATE OR REPLACE VIEW gold.v_pii_inventory
COMMENT 'Governance discovery: every classified column, its level and masking status.'
AS SELECT
  t.catalog_name, t.schema_name, t.table_name, t.column_name,
  max(CASE WHEN t.tag_name = 'data_classification' THEN t.tag_value END) AS classification,
  max(CASE WHEN t.tag_name = 'pii_type'            THEN t.tag_value END) AS pii_type,
  max(CASE WHEN t.tag_name = 'masked'              THEN t.tag_value END) AS is_masked
FROM information_schema.column_tags t
GROUP BY t.catalog_name, t.schema_name, t.table_name, t.column_name;
