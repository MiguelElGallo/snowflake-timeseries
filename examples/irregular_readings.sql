-- Run this whole file in ONE session with an existing standard warehouse,
-- database and writable schema selected. Every table below is session TEMP.
-- Fresh names avoid replacing or reusing an existing example table.
-- All TIMESTAMP_NTZ values in this example represent UTC.

SET ts_example_readings = (SELECT 'TS_GAPS_' || REPLACE(UUID_STRING(), '-', ''));
SET ts_example_calibration = (SELECT 'TS_CALIBRATION_' || REPLACE(UUID_STRING(), '-', ''));

CREATE TEMP TABLE IDENTIFIER($ts_example_readings) (
  sensor_id VARCHAR,
  observed_at TIMESTAMP_NTZ(6),
  observed_value NUMBER(38,6)
);

INSERT INTO IDENTIFIER($ts_example_readings) VALUES
  ('A', '2000-01-01 12:00:00', 10),
  ('A', '2000-01-01 12:00:02', 30);

-- Two measurements become three display rows. The middle 20 is an ESTIMATE.
-- Original values and provenance flags remain visible.
WITH filled AS (
  SELECT sensor_id, observed_at, observed_value, is_generated_row,
         INTERPOLATE_LINEAR(observed_value)
           OVER (PARTITION BY sensor_id ORDER BY observed_at) AS display_value
  FROM IDENTIFIER($ts_example_readings)
    RESAMPLE(
      USING observed_at
      INCREMENT BY INTERVAL '1 SECOND'
      PARTITION BY sensor_id
      METADATA_COLUMNS IS_GENERATED() AS is_generated_row)
)
SELECT sensor_id, observed_at, observed_value, display_value, is_generated_row,
       observed_value IS NULL AND display_value IS NOT NULL AS is_estimated_value
FROM filled
ORDER BY sensor_id, observed_at;

-- Keep measurement counts separate from the size of the display grid.
SELECT COUNT(*) AS display_rows,
       COUNT_IF(NOT is_generated_row) AS observed_rows,
       COUNT(observed_value) AS valid_observed_values,
       COUNT_IF(is_generated_row) AS generated_rows
FROM IDENTIFIER($ts_example_readings)
  RESAMPLE(
    USING observed_at
    INCREMENT BY INTERVAL '1 SECOND'
    PARTITION BY sensor_id
    METADATA_COLUMNS IS_GENERATED() AS is_generated_row);

CREATE TEMP TABLE IDENTIFIER($ts_example_calibration) (
  sensor_id VARCHAR,
  valid_from TIMESTAMP_NTZ(6),
  offset_c NUMBER(38,6)
);

INSERT INTO IDENTIFIER($ts_example_calibration) VALUES
  ('A', '2000-01-01 11:59:00', 0.5),
  ('A', '2000-01-01 12:00:01', 1.0);

-- This must return NO rows. Resolve calibration key/time conflicts before ASOF.
SELECT sensor_id, valid_from, COUNT(*) AS calibration_versions
FROM IDENTIFIER($ts_example_calibration)
GROUP BY sensor_id, valid_from
HAVING COUNT(*) > 1;

-- Apply the most recent calibration at or before each OBSERVED reading.
-- Expected calibrated values: 10.5 and 31.0; no interpolated row is joined.
SELECT r.sensor_id, r.observed_at, r.observed_value,
       c.valid_from AS calibration_valid_from, c.offset_c,
       r.observed_value + c.offset_c AS calibrated_value
FROM IDENTIFIER($ts_example_readings) r
  ASOF JOIN IDENTIFIER($ts_example_calibration) c
    MATCH_CONDITION(r.observed_at >= c.valid_from)
    ON r.sensor_id = c.sensor_id
ORDER BY r.sensor_id, r.observed_at;

DROP TABLE IDENTIFIER($ts_example_calibration);
DROP TABLE IDENTIFIER($ts_example_readings);
UNSET (ts_example_readings, ts_example_calibration);
