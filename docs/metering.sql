-- Read-only measurement templates: no provisioning, grants, refreshes or cleanup.
-- Replace EVERY __START_UTC__ and __END_UTC__ with explicit ISO timestamps such as
-- 2026-01-01T00:00:00+00:00 and 2026-01-02T00:00:00+00:00 before running a section.
-- Use UTC hour boundaries for hourly meters and UTC midnight boundaries for daily
-- storage/currency meters. Keep the interval bounded and end-exclusive.
-- Replace generic names with the exact resources from your own ownership manifest.
-- Use an authorized monitoring role and an existing observer warehouse; these
-- SELECTs consume compute. Keep observer usage separate from the workload.
--
-- Provenance: all six sections compiled and executed in the trial on 2026-11-06
-- with owned resource names and a bounded interval. A shared database was listed
-- once in section D. Warehouse/HP sections returned partial meter records; QAS,
-- Online and daily-storage sections did not establish charges for the interval.
-- Reading billing views does not validate a running Online Feature Store backend.
-- Empty/delayed results or permission errors NEVER establish zero charges.
-- Do not add the same service from multiple metering views twice.

-- A. Warehouse usage, separately labelled; includes idle/setup/observer activity.
-- Delay: up to 3h; cloud-services column up to 6h. Totals are unadjusted.
-- https://docs.snowflake.com/en/sql-reference/account-usage/warehouse_metering_history
WITH bounds AS (
  SELECT TO_TIMESTAMP_TZ('__START_UTC__') AS start_utc,
         TO_TIMESTAMP_TZ('__END_UTC__') AS end_utc
), resources(component, warehouse_name) AS (
  SELECT * FROM VALUES
    ('dynamic_table_lab_not_pure_refresh', 'TIMESERIES_DT_WH'),
    ('online_offline_processing', 'TIMESERIES_ONLINE_WH'),
    ('interactive_serving_only', 'TIMESERIES_INTERACTIVE_WH'),
    ('quickstart_validation_only', 'SENSOR_TIMESERIES_DEMO_WH')
)
SELECT r.component, r.warehouse_name,
       COUNT(m.start_time) AS meter_rows,
       IFF(COUNT(m.start_time)=0, 'NO_ROWS_REPORTED_NOT_ZERO', 'ROWS_REPORTED_PARTIAL') AS coverage,
       MIN(m.start_time) AS first_meter_hour, MAX(m.end_time) AS last_meter_hour_end,
       SUM(m.credits_used_compute) AS compute_credits,
       SUM(m.credits_used_cloud_services) AS cloud_services_credits,
       SUM(m.credits_used) AS total_unadjusted_credits
FROM resources r CROSS JOIN bounds b
LEFT JOIN SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY m
  ON m.warehouse_name=r.warehouse_name
 AND m.start_time>=b.start_utc AND m.start_time<b.end_utc
GROUP BY r.component,r.warehouse_name
ORDER BY r.component;

-- B. High-performance Streaming, attributed to explicitly named pipes.
-- The identity/name join follows Snowflake's HP billing guide. Do not substitute
-- Classic client/file-migration history for the HP ingestion meter.
-- Metadata can lag ~3h, streaming credit reporting up to 12h. A missing pipe row
-- can also mean the role cannot see that metadata. No bytes/credits are set to 0.
-- If a pipe name was reused, retain separate PIPE_ID rows; do not merge lifetimes.
-- BYTES is the meter's reported volume; it is not stored-table or wire size.
-- https://docs.snowflake.com/en/user-guide/snowpipe-streaming/snowpipe-streaming-high-performance-cost
-- https://docs.snowflake.com/en/sql-reference/account-usage/pipes
-- https://docs.snowflake.com/en/sql-reference/account-usage/metering_history
WITH bounds AS (
  SELECT TO_TIMESTAMP_TZ('__START_UTC__') AS start_utc,
         TO_TIMESTAMP_TZ('__END_UTC__') AS end_utc
), resources(component, pipe_catalog, pipe_schema, pipe_name) AS (
  SELECT * FROM VALUES
    ('dynamic_table_lab_ingestion', 'TIMESERIES_LAB', 'DT', 'SENSOR_PIPE'),
    ('quickstart_ingestion', 'SENSOR_TIMESERIES_DEMO', 'SENSORS', 'SENSOR_PIPE')
)
SELECT r.component,r.pipe_catalog,r.pipe_schema,r.pipe_name,p.pipe_id,
       COUNT(m.start_time) AS meter_rows,
       CASE WHEN p.pipe_id IS NULL THEN 'PIPE_METADATA_MISSING_NOT_ZERO'
            WHEN COUNT(m.start_time)=0 THEN 'NO_ROWS_REPORTED_NOT_ZERO'
            ELSE 'ROWS_REPORTED_PARTIAL' END AS coverage,
       MIN(m.start_time) AS first_meter_hour,MAX(m.end_time) AS last_meter_hour_end,
       SUM(m.bytes) AS reported_meter_bytes,
       SUM(m.credits_used) AS total_unadjusted_credits
FROM resources r CROSS JOIN bounds b
LEFT JOIN SNOWFLAKE.ACCOUNT_USAGE.PIPES p
  ON p.pipe_catalog=r.pipe_catalog AND p.pipe_schema=r.pipe_schema AND p.pipe_name=r.pipe_name
LEFT JOIN SNOWFLAKE.ACCOUNT_USAGE.METERING_HISTORY m
  ON m.entity_id=p.pipe_id AND m.name=p.pipe_name
 AND m.service_type='SNOWPIPE_STREAMING'
 AND m.start_time>=b.start_utc AND m.start_time<b.end_utc
GROUP BY r.component,r.pipe_catalog,r.pipe_schema,r.pipe_name,p.pipe_id
ORDER BY r.component,p.pipe_id;

-- C. OPTIONAL dedicated Query Acceleration meter; Enterprise feature.
-- QAS is additional to warehouse credits. Do not also add QUERY_ACCELERATION
-- entries from METERING_HISTORY to these same charges. Delay can be up to 3h.
-- An unavailable view/privilege is an unavailable measurement, not zero usage.
-- https://docs.snowflake.com/en/sql-reference/account-usage/query_acceleration_history
WITH bounds AS (
  SELECT TO_TIMESTAMP_TZ('__START_UTC__') AS start_utc,
         TO_TIMESTAMP_TZ('__END_UTC__') AS end_utc
), resources(component, warehouse_name) AS (
  SELECT * FROM VALUES
    ('dynamic_table_lab_qas', 'TIMESERIES_DT_WH'),
    ('quickstart_qas', 'SENSOR_TIMESERIES_DEMO_WH')
)
SELECT r.component,r.warehouse_name,COUNT(q.start_time) AS meter_rows,
       IFF(COUNT(q.start_time)=0,'NO_ROWS_REPORTED_NOT_ZERO','ROWS_REPORTED_PARTIAL') AS coverage,
       SUM(q.credits_used) AS qas_credits
FROM resources r CROSS JOIN bounds b
LEFT JOIN SNOWFLAKE.ACCOUNT_USAGE.QUERY_ACCELERATION_HISTORY q
  ON q.warehouse_name=r.warehouse_name
 AND q.start_time>=b.start_utc AND q.start_time<b.end_utc
GROUP BY r.component,r.warehouse_name
ORDER BY r.component;

-- D. Daily database bytes, not an exact database-level storage invoice.
-- Daily averages are deliberately not summed and called retained TB-months.
-- If components share a database, use one shared entry; this cannot split its bytes.
-- No days returned means unknown here, not zero bytes/cost. Delay can be up to 3h.
-- https://docs.snowflake.com/en/sql-reference/account-usage/database_storage_usage_history
WITH bounds AS (
  SELECT CONVERT_TIMEZONE('UTC',TO_TIMESTAMP_TZ('__START_UTC__'))::DATE AS start_day,
         CONVERT_TIMEZONE('UTC',TO_TIMESTAMP_TZ('__END_UTC__'))::DATE AS end_day
), resources(component, database_name) AS (
  SELECT * FROM VALUES
    ('dynamic_table_lab_storage', 'TIMESERIES_LAB'),
    ('online_offline_storage', 'TIMESERIES_ONLINE_LAB'),
    ('quickstart_storage', 'SENSOR_TIMESERIES_DEMO')
)
SELECT r.component,r.database_name,s.usage_date,
       IFF(s.usage_date IS NULL,'NO_ROWS_REPORTED_NOT_ZERO','ROW_REPORTED_PARTIAL') AS coverage,
       s.average_database_bytes,s.average_failsafe_bytes,
       s.average_hybrid_table_storage_bytes
FROM resources r CROSS JOIN bounds b
LEFT JOIN SNOWFLAKE.ACCOUNT_USAGE.DATABASE_STORAGE_USAGE_HISTORY s
  ON s.database_name=r.database_name
 AND s.usage_date>=b.start_day AND s.usage_date<b.end_day
ORDER BY r.component,s.usage_date;

-- E. OPTIONAL Online compute: ACCOUNT TOTAL, not attributed to a particular store.
-- Keep this context separate from the per-warehouse components above. Do not
-- attribute unrelated online services to your experiment. The trial could not
-- provision its service, so the repository claims no live Online benchmark bill.
-- Online storage is a different billing component (section F). Delay up to 3h.
-- https://docs.snowflake.com/en/developer-guide/snowflake-ml/feature-store/online-feature-store#monitor-online-service-cost
WITH bounds AS (
  SELECT TO_TIMESTAMP_TZ('__START_UTC__') AS start_utc,
         TO_TIMESTAMP_TZ('__END_UTC__') AS end_utc
), components(service_type) AS (
  SELECT * FROM VALUES ('ONLINE_FEATURE_STORE_COMPUTE'),('ONLINE_FEATURE_STORE_AUTOSCALE_COMPUTE')
)
SELECT 'ACCOUNT_CONTEXT_NOT_STORE_ATTRIBUTED' AS attribution,c.service_type,
       COUNT(m.start_time) AS meter_rows,
       IFF(COUNT(m.start_time)=0,'NO_ROWS_REPORTED_NOT_ZERO','ROWS_REPORTED_PARTIAL') AS coverage,
       SUM(m.credits_used) AS total_unadjusted_credits
FROM components c CROSS JOIN bounds b
LEFT JOIN SNOWFLAKE.ACCOUNT_USAGE.METERING_HISTORY m
  ON m.service_type=c.service_type
 AND m.start_time>=b.start_utc AND m.start_time<b.end_utc
GROUP BY c.service_type
ORDER BY c.service_type;

-- F. OPTIONAL organization billing view: current-account Online storage only.
-- Requires access to ORGANIZATION_USAGE; may be unavailable in a trial or under
-- some contracts/roles. No permission changes are part of this template.
-- Delay up to 72h; records can change before month close. Empty is not zero.
-- This is account-level storage cost, not a per-store or per-query allocation.
-- Group by billing dimensions/currency; do not sum credits and storage units.
-- https://docs.snowflake.com/en/sql-reference/organization-usage/usage_in_currency_daily
WITH bounds AS (
  SELECT CONVERT_TIMEZONE('UTC',TO_TIMESTAMP_TZ('__START_UTC__'))::DATE AS start_day,
         CONVERT_TIMEZONE('UTC',TO_TIMESTAMP_TZ('__END_UTC__'))::DATE AS end_day
)
SELECT 'ACCOUNT_CONTEXT_NOT_STORE_ATTRIBUTED' AS attribution,
       u.usage_date,u.service_type,u.rating_type,u.billing_type,u.is_adjustment,u.currency,
       SUM(u.usage) AS reported_usage,SUM(u.usage_in_currency) AS reported_currency_amount
FROM SNOWFLAKE.ORGANIZATION_USAGE.USAGE_IN_CURRENCY_DAILY u CROSS JOIN bounds b
WHERE u.organization_name=CURRENT_ORGANIZATION_NAME()
  AND u.account_name=CURRENT_ACCOUNT_NAME()
  AND u.service_type='ONLINE_FEATURE_STORE_STORAGE'
  AND u.usage_date>=b.start_day AND u.usage_date<b.end_day
GROUP BY u.usage_date,u.service_type,u.rating_type,u.billing_type,u.is_adjustment,u.currency
ORDER BY u.usage_date,u.currency,u.billing_type,u.is_adjustment;
