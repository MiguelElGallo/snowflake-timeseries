-- Optional: demo.py moving-window
CREATE DYNAMIC TABLE sensor_last_minute
  TARGET_LAG='60 seconds'
  WAREHOUSE=__WAREHOUSE__
  REFRESH_MODE=INCREMENTAL
  INITIALIZE=ON_CREATE
AS
SELECT sensor_id,
       COUNT(*) AS received_readings,
       COUNT(value) AS valid_readings,
       AVG(value) AS average,
       STDDEV_POP(value) AS population_sd,
       STDDEV_SAMP(value) AS sample_sd
FROM sensor_readings_deduplicated
WHERE event_ts > DATEADD('SECOND', -60,
        CONVERT_TIMEZONE('UTC', CURRENT_TIMESTAMP())::TIMESTAMP_NTZ)
  AND event_ts <= CONVERT_TIMEZONE('UTC', CURRENT_TIMESTAMP())::TIMESTAMP_NTZ
GROUP BY sensor_id;
