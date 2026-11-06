# Irregular readings: estimates and calibration

Two sensor readings do not become three measurements just because a chart needs a point every second. The [complete SQL example](../examples/irregular_readings.sql) keeps that distinction visible while demonstrating gap filling and time-based calibration.

Run the whole file in one SQL session with an existing **standard warehouse**, database and writable schema selected. It creates two randomly named session TEMP tables and drops them at the end. It creates no persistent resources and does not modify any existing sensor table. Closing the session also removes its temporary tables if execution stops early.

## Fill a display grid without inventing observations

The input contains values 10 and 30, two seconds apart. `RESAMPLE` adds the missing one-second position; `INTERPOLATE_LINEAR` estimates 20 between the two observations. Both operations partition by sensor, preventing values from different sensors from being mixed. Snowflake currently documents gap filling and interpolation as **Open Preview**, available to all accounts. See [time-series analysis](https://docs.snowflake.com/en/user-guide/querying-time-series-data#filling-gaps-in-time-series-data), [RESAMPLE](https://docs.snowflake.com/en/sql-reference/constructs/resample) and [interpolation functions](https://docs.snowflake.com/en/sql-reference/functions/interpolate_bfill).

| Time | Observed value | Display value | Generated row | Estimated value |
|---|---:|---:|---|---|
| 12:00:00 | 10 | 10 | false | false |
| 12:00:01 | NULL | 20 | true | true |
| 12:00:02 | 30 | 30 | false | false |

The count query returns **3 display rows, 2 observed rows, 2 valid observed values and 1 generated row**. Calculate measurement counts and measurement statistics from observed input. Including 20 changes both sample size and variance; it represents a modeling choice, not new sensor evidence.

Keep the original value and a separate estimated-value flag. A generated-row flag alone is not enough for every dataset: interpolation can also fill a NULL in an existing row. An existing row with a missing value still counts as an observed event, but its interpolated numeric value is an estimate. Avoid filling across outages or long gaps without an explicit maximum-gap policy. The tiny example does not implement such a policy.

## Use the calibration valid at the reading time

`ASOF JOIN` chooses the latest calibration at or before each reading because the match condition uses `reading_time >= valid_from`. The equality condition also requires the same sensor. The two observed values become **10.5 and 31.0**; the estimated display row is not passed into this calibration query. A missing preceding calibration leaves the calibration and calibrated value NULL instead of silently substituting an offset. [ASOF JOIN semantics](https://docs.snowflake.com/en/sql-reference/constructs/asof-join).

Require one authoritative calibration per `(sensor_id, valid_from)`. The example's duplicate check must return no rows. Ties on the right side of an ASOF join can select different matching rows on different executions. If calibrations have revisions, choose a deterministic winner using a documented version rule before joining; declaring an intended key alone does not resolve conflicting rows. [Documented tie behavior](https://docs.snowflake.com/en/sql-reference/constructs/asof-join#expected-behavior-when-ties-exist-in-the-right-table).

ASOF's match condition accepts one directional comparison (`>=`, `>`, `<=` or `<`); equality grouping belongs in `ON`, whose conditions must be joined with `AND`, not `OR`. This example uses compatible UTC `TIMESTAMP_NTZ` columns. The simple “valid until the next calibration” rule does not impose a maximum calibration age; add and test an explicit validity policy when needed.

These are ordinary SQL examples. They do not show aggregation inside a streaming pipe, continuous gap maintenance, or a sustained workload benchmark.

The exact published SQL was executed live in a separate session on a standard warehouse. It returned the rows and counts shown above, the calibration uniqueness check was empty, and the calibrated values were 10.5 and 31.0. Both temporary-table names were verified absent after the final drops, and the session was closed. No implementation correction or rerun was needed.
