-- Business dashboards — one view per business question.
--
-- Each maps to a numbered question in BUSINESS_REQUIREMENTS.md §4. Metric
-- definitions are NOT redefined here: they are applied once in Gold, so every
-- dashboard reads the same number. A query that recomputes "net revenue" its
-- own way is how two departments end up publishing different figures.

USE CATALOG ${catalog};

-- Q1 · Revenue by day and month
CREATE OR REPLACE VIEW gold.v_q01_revenue_by_period AS
SELECT year, month_number, full_date,
       sum(orders) AS orders, sum(net_revenue) AS net_revenue,
       sum(gross_revenue) AS gross_revenue, sum(discount_total) AS discount_total
FROM gold.agg_daily_sales
GROUP BY ALL;

-- Q2 · Revenue by region
CREATE OR REPLACE VIEW gold.v_q02_revenue_by_region AS
SELECT region, year, month_number,
       sum(net_revenue) AS net_revenue, sum(orders) AS orders,
       round(sum(net_revenue) / nullif(sum(orders), 0), 2) AS avg_order_value
FROM gold.agg_daily_sales
WHERE region IS NOT NULL
GROUP BY ALL;

-- Q3 · Revenue by product and category
CREATE OR REPLACE VIEW gold.v_q03_revenue_by_product AS
SELECT c.category_name, p.brand, p.product_name,
       sum(f.quantity) AS units_sold, sum(f.net_amount) AS net_revenue,
       count(DISTINCT f.order_id) AS orders
FROM gold.fact_sales f
LEFT JOIN gold.dim_products p   ON f.product_sk  = p.products_sk
LEFT JOIN gold.dim_categories c ON f.category_sk = c.categories_sk
GROUP BY ALL;

-- Q4 · Average order value
-- AOV is order-grain, so fact_sales must be rolled up first. Averaging
-- net_amount at line grain answers a different and useless question.
CREATE OR REPLACE VIEW gold.v_q04_average_order_value AS
WITH per_order AS (
  SELECT order_id, order_date_sk, customer_sk, sum(net_amount) AS order_net
  FROM gold.fact_sales
  WHERE order_status <> 'CANCELLED'
  GROUP BY order_id, order_date_sk, customer_sk
)
SELECT d.year, d.month_number, c.region, c.customer_segment,
       count(*) AS orders,
       round(avg(o.order_net), 2) AS avg_order_value,
       round(percentile_approx(o.order_net, 0.5), 2) AS median_order_value
FROM per_order o
JOIN gold.dim_date d ON o.order_date_sk = d.date_sk
LEFT JOIN gold.dim_customers c ON o.customer_sk = c.customers_sk
GROUP BY ALL;

-- Q5 · Most valuable customers
CREATE OR REPLACE VIEW gold.v_q05_customer_lifetime_value AS
SELECT customer_id, customer_segment, region,
       total_orders, lifetime_value, avg_order_value,
       first_order_date, last_order_date, days_since_last_order,
       ntile(10) OVER (ORDER BY lifetime_value DESC) AS value_decile
FROM gold.agg_customer_lifetime;

-- Q6 · Return rate
-- Measured against the SHIPMENT cohort, not the calendar month the return
-- arrived. Measuring by return date understates the rate in growth periods,
-- because the denominator has grown since the item shipped.
CREATE OR REPLACE VIEW gold.v_q06_return_rate AS
SELECT p.category_name, p.brand, p.product_name, r.return_reason,
       sum(p.units_sold)     AS units_sold,
       sum(p.units_returned) AS units_returned,
       round(sum(p.units_returned) / nullif(sum(p.units_sold), 0) * 100, 2) AS return_rate_pct
FROM gold.agg_product_performance p
LEFT JOIN gold.fact_returns r ON p.product_sk = r.product_sk
GROUP BY ALL;

-- Q7 · Inventory availability
-- quantity_on_hand is NON-ADDITIVE over time: averaged across days, summed
-- across products. Summing 30 daily snapshots invents stock that never existed.
CREATE OR REPLACE VIEW gold.v_q07_inventory_availability AS
SELECT d.full_date, s.store_name, s.region, c.category_name,
       sum(i.quantity_available)           AS units_available,
       round(avg(i.quantity_available), 1) AS avg_units_per_sku,
       sum(CASE WHEN i.is_stockout THEN 1 ELSE 0 END) AS skus_out_of_stock,
       count(*)                            AS skus_tracked
FROM gold.fact_inventory_snapshot i
JOIN gold.dim_date d ON i.snapshot_date_sk = d.date_sk
LEFT JOIN gold.dim_stores s     ON i.store_sk    = s.stores_sk
LEFT JOIN gold.dim_products p   ON i.product_sk  = p.products_sk
LEFT JOIN gold.dim_categories c ON p.category_sk = c.categories_sk
GROUP BY ALL;

-- Q8 · Frequently out-of-stock products
CREATE OR REPLACE VIEW gold.v_q08_stockout_frequency AS
SELECT p.product_name, p.brand, s.store_name,
       count(*)                                      AS days_tracked,
       sum(CASE WHEN i.is_stockout THEN 1 ELSE 0 END) AS days_out_of_stock,
       round(sum(CASE WHEN i.is_stockout THEN 1 ELSE 0 END) / count(*) * 100, 1)
                                                      AS stockout_rate_pct
FROM gold.fact_inventory_snapshot i
LEFT JOIN gold.dim_products p ON i.product_sk = p.products_sk
LEFT JOIN gold.dim_stores s   ON i.store_sk   = s.stores_sk
GROUP BY ALL
HAVING sum(CASE WHEN i.is_stockout THEN 1 ELSE 0 END) > 0;

-- Q9 · Customer acquisition and retention
CREATE OR REPLACE VIEW gold.v_q09_cohort_retention AS
SELECT cohort_month, months_since_signup, active_customers, orders, net_revenue,
       round(active_customers / nullif(
         first_value(active_customers) OVER (
           PARTITION BY cohort_month ORDER BY months_since_signup
         ), 0) * 100, 1) AS retention_pct
FROM gold.agg_customer_cohort;

-- Q10 · Conversion rate
-- Session-level, not event-level. A session with 8 product views and one order
-- converted once; counting events would report a conversion rate above 100%.
CREATE OR REPLACE VIEW gold.v_q10_conversion_rate AS
WITH sessions AS (
  SELECT session_id, channel, event_date_sk,
         max(CASE WHEN event_type = 'PRODUCT_VIEW'  THEN 1 ELSE 0 END) AS viewed,
         max(CASE WHEN event_type = 'CART_ADD'      THEN 1 ELSE 0 END) AS added_to_cart,
         max(CASE WHEN event_type = 'ORDER_CREATED' THEN 1 ELSE 0 END) AS ordered
  FROM gold.fact_customer_events
  GROUP BY session_id, channel, event_date_sk
)
SELECT d.full_date, s.channel,
       sum(s.viewed)        AS sessions_with_view,
       sum(s.added_to_cart) AS sessions_with_cart,
       sum(s.ordered)       AS sessions_with_order,
       round(sum(s.ordered) / nullif(sum(s.viewed), 0) * 100, 2)       AS conversion_rate_pct,
       round(sum(s.added_to_cart) / nullif(sum(s.viewed), 0) * 100, 2) AS cart_rate_pct
FROM sessions s
JOIN gold.dim_date d ON s.event_date_sk = d.date_sk
GROUP BY ALL;

-- Q11 · Order fulfilment time
-- hours_to_ship is semi-additive: averaged, never summed.
CREATE OR REPLACE VIEW gold.v_q11_fulfilment_time AS
SELECT d.year, d.month_number, s.store_name, s.region,
       count(*)                                           AS orders,
       round(avg(f.hours_to_ship), 1)                     AS avg_hours_to_ship,
       round(percentile_approx(f.hours_to_ship, 0.95), 1) AS p95_hours_to_ship,
       round(avg(f.hours_to_deliver), 1)                  AS avg_hours_to_deliver
FROM gold.fact_order_fulfillment f
JOIN gold.dim_date d ON f.order_date_sk = d.date_sk
LEFT JOIN gold.dim_stores s ON f.store_sk = s.stores_sk
WHERE f.final_status <> 'CANCELLED' AND f.hours_to_ship IS NOT NULL
GROUP BY ALL;

-- Q12 · Promotion impact
-- Reported per campaign with the effective discount, so a campaign that bought
-- revenue at a ruinous margin is visible. Comparing promoted revenue to total
-- revenue would flatter every campaign, since promoted items are chosen
-- precisely because they already sell.
CREATE OR REPLACE VIEW gold.v_q12_promotion_impact AS
SELECT pr.promotion_name, pr.promo_type, pr.discount_value,
       count(DISTINCT f.order_id) AS orders,
       sum(f.quantity)            AS units,
       sum(f.gross_amount)        AS gross_revenue,
       sum(f.discount_amount)     AS discount_given,
       sum(f.net_amount)          AS net_revenue,
       round(sum(f.discount_amount) / nullif(sum(f.gross_amount), 0) * 100, 2)
                                  AS effective_discount_pct,
       round(sum(f.net_amount) / nullif(count(DISTINCT f.order_id), 0), 2)
                                  AS aov_on_promotion
FROM gold.fact_sales f
JOIN gold.dim_promotions pr ON f.promotion_sk = pr.promotions_sk
GROUP BY ALL;

-- Q13 · Latest completed day vs trailing 28-day trend
-- Replaces the R1 real-time comparison. Answers the same underlying question,
-- "is today normal?", on a batch cadence.
CREATE OR REPLACE VIEW gold.v_q13_latest_vs_trend AS
WITH daily AS (
  SELECT full_date, region, sum(net_revenue) AS net_revenue, sum(orders) AS orders
  FROM gold.agg_daily_sales
  GROUP BY full_date, region
),
with_trend AS (
  SELECT full_date, region, net_revenue, orders,
         avg(net_revenue) OVER (
           PARTITION BY region ORDER BY full_date
           ROWS BETWEEN 28 PRECEDING AND 1 PRECEDING
         ) AS trailing_28d_avg
  FROM daily
)
SELECT full_date, region, net_revenue, orders,
       round(trailing_28d_avg, 2) AS trailing_28d_avg,
       round((net_revenue - trailing_28d_avg) / nullif(trailing_28d_avg, 0) * 100, 1)
         AS variance_pct
FROM with_trend;

-- Executive KPI strip. One row: the numbers a CFO opens first.
CREATE OR REPLACE VIEW gold.v_executive_kpis AS
SELECT
  (SELECT sum(net_revenue) FROM gold.agg_daily_sales)   AS total_net_revenue,
  (SELECT sum(orders) FROM gold.agg_daily_sales)        AS total_orders,
  (SELECT round(sum(net_revenue) / nullif(sum(orders), 0), 2)
     FROM gold.agg_daily_sales)                         AS avg_order_value,
  (SELECT count(*) FROM gold.agg_customer_lifetime)     AS customers_with_orders,
  (SELECT count(*) FROM gold.agg_customer_lifetime
    WHERE is_repeat_customer)                           AS repeat_customers,
  (SELECT round(sum(units_returned) / nullif(sum(units_sold), 0) * 100, 2)
     FROM gold.agg_product_performance)                 AS return_rate_pct;
