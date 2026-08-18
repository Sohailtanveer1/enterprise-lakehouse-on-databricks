-- Operations dashboard — queries over the audit tables (NFR-OBS-06).
--
-- Answers the four questions an operator actually asks, in order of urgency:
--   did it run, is it getting slower, is the data still good, did we lose any.
--
-- Built on our own audit tables rather than Databricks system tables, because
-- Free Edition's system table access is limited. In a paid workspace these
-- would join against system.billing and system.access.audit instead.

USE CATALOG ${catalog};

-- 1. Last 7 days of runs. The first thing anyone looks at.
CREATE OR REPLACE VIEW audit.v_run_status AS
SELECT
  date(start_ts) AS run_date,
  task_name, entity, layer, status,
  count(*)                  AS runs,
  round(avg(duration_s), 1) AS avg_duration_s,
  round(max(duration_s), 1) AS max_duration_s,
  sum(rows_in)              AS rows_in,
  sum(rows_out)             AS rows_out,
  sum(rows_rejected)        AS rows_rejected
FROM audit.pipeline_run_audit
WHERE start_ts >= current_timestamp() - INTERVAL 7 DAYS
GROUP BY ALL;

-- 2. Duration trend. A pipeline does not fail suddenly; it gets slower first,
--    and the small-file problem is usually why.
CREATE OR REPLACE VIEW audit.v_duration_trend AS
SELECT
  entity, layer, date(start_ts) AS run_date,
  round(avg(duration_s), 1) AS duration_s,
  round(avg(avg(duration_s)) OVER (
    PARTITION BY entity, layer ORDER BY date(start_ts)
    ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
  ), 1) AS rolling_7d_avg
FROM audit.pipeline_run_audit
WHERE status = 'SUCCESS'
GROUP BY entity, layer, date(start_ts);

-- 3. Data quality pass rate by rule. Which rule actually fires, and whether a
--    WARN is quietly becoming the norm.
CREATE OR REPLACE VIEW audit.v_dq_summary AS
SELECT
  entity, rule_name, rule_type, severity,
  count(*)                                AS evaluations,
  sum(CASE WHEN passed THEN 0 ELSE 1 END) AS failures,
  round(avg(pass_rate) * 100, 3)          AS avg_pass_rate_pct,
  max(evaluated_at)                       AS last_evaluated
FROM audit.data_quality_results
WHERE evaluated_at >= current_timestamp() - INTERVAL 30 DAYS
GROUP BY ALL;

-- 4. Reconciliation. Catches silent data loss, so a failure here outranks
--    everything above it.
CREATE OR REPLACE VIEW audit.v_reconciliation_status AS
SELECT
  date(evaluated_at) AS check_date,
  check_name, entity, source_layer, target_layer,
  source_value, target_value, difference, passed, tolerance
FROM audit.reconciliation_results
WHERE evaluated_at >= current_timestamp() - INTERVAL 7 DAYS;

-- 5. Freshness SLA. A job that succeeds while ingesting a stale file is a
--    silent failure — success and freshness are different questions.
CREATE OR REPLACE VIEW audit.v_freshness AS
SELECT
  entity, layer,
  max(end_ts) AS last_success,
  round((unix_timestamp(current_timestamp())
         - unix_timestamp(max(end_ts))) / 3600.0, 1) AS hours_since_success,
  CASE
    WHEN max(end_ts) >= current_timestamp() - INTERVAL 26 HOURS THEN 'OK'
    WHEN max(end_ts) >= current_timestamp() - INTERVAL 48 HOURS THEN 'WARN'
    ELSE 'BREACH'
  END AS sla_status
FROM audit.pipeline_run_audit
WHERE status = 'SUCCESS'
GROUP BY entity, layer;

-- 6. Single-row health summary for the top of the dashboard.
CREATE OR REPLACE VIEW audit.v_pipeline_health AS
SELECT
  (SELECT count(*) FROM audit.pipeline_run_audit
    WHERE status = 'FAILED' AND start_ts >= current_timestamp() - INTERVAL 24 HOURS)
    AS failures_24h,
  (SELECT count(*) FROM audit.data_quality_results
    WHERE NOT passed AND severity = 'fatal'
      AND evaluated_at >= current_timestamp() - INTERVAL 24 HOURS)
    AS fatal_dq_24h,
  (SELECT count(*) FROM audit.reconciliation_results
    WHERE NOT passed AND evaluated_at >= current_timestamp() - INTERVAL 24 HOURS)
    AS recon_failures_24h,
  (SELECT count(*) FROM audit.v_freshness WHERE sla_status = 'BREACH')
    AS sla_breaches;
