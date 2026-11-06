-- Five distinct, exactly representable FLOAT inputs. Expected population SD = SQRT(2).
WITH readings AS (
  SELECT (1000000000000 + column1)::FLOAT AS value
  FROM VALUES (-2), (-1), (0), (1), (2)
)
SELECT STDDEV_POP(value) AS float_sd,
       STDDEV_POP(value::NUMBER(38,6)) AS fixed_point_sd,
       STDDEV_POP(value - 1000000000000) AS centered_sd,
       SQRT(2) AS expected_sd
FROM readings;
