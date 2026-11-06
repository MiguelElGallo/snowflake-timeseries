-- Executed by demo.py after it creates an isolated database and warehouse.
CREATE TABLE sensor_raw (
  event_id VARCHAR NOT NULL,
  sensor_id VARCHAR NOT NULL,
  event_ts TIMESTAMP_NTZ(6) NOT NULL,
  value NUMBER(38,6),
  source_value VARCHAR,
  sent_at TIMESTAMP_NTZ(6) NOT NULL
) ROW_TIMESTAMP=TRUE CHANGE_TRACKING=TRUE;

CREATE PIPE sensor_pipe AS COPY INTO sensor_raw FROM (
  SELECT $1:event_id::VARCHAR,
         $1:sensor_id::VARCHAR,
         TO_TIMESTAMP_NTZ($1:event_ts::VARCHAR),
         TRY_TO_DECIMAL($1:value::VARCHAR, 38, 6),
         $1:value::VARCHAR,
         TO_TIMESTAMP_NTZ($1:sent_at::VARCHAR)
  FROM TABLE(DATA_SOURCE(TYPE=>'STREAMING'))
);

CREATE DYNAMIC TABLE sensor_readings_deduplicated
  TARGET_LAG=DOWNSTREAM
  WAREHOUSE=__WAREHOUSE__
  REFRESH_MODE=INCREMENTAL
  INITIALIZE=ON_CREATE
AS
SELECT event_id, sensor_id, event_ts, value, source_value, sent_at
FROM sensor_raw
QUALIFY ROW_NUMBER() OVER (
  PARTITION BY sensor_id, event_id ORDER BY sent_at DESC
)=1;

CREATE DYNAMIC TABLE sensor_minute
  TARGET_LAG='60 seconds'
  WAREHOUSE=__WAREHOUSE__
  REFRESH_MODE=INCREMENTAL
  INITIALIZE=ON_CREATE
AS
SELECT sensor_id,
       DATE_TRUNC('MINUTE', event_ts) AS minute_start,
       COUNT(*) AS received_readings,
       COUNT(value) AS valid_readings,
       AVG(value) AS average,
       STDDEV_POP(value) AS population_sd,
       STDDEV_SAMP(value) AS sample_sd
FROM sensor_readings_deduplicated
GROUP BY sensor_id, minute_start;
