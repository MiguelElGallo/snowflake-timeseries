# Recheck the aggregation boundary in your account

The standard high-performance streaming-pipe definitions below were rejected in the trial experiment. They test three separate questions: an aggregate over messages, population standard deviation, and an unframed window aggregate. The complete working per-message pipe and producer are in the [quickstart](../quickstart/README.md).

Snowflake's [Streaming Transformations announcement](https://www.snowflake.com/en/blog/real-time-analytics-snowflake/) describes a **private-preview** path that includes aggregation. These probes do not establish that preview's exact syntax or capabilities. Ask for its documented examples and enablement before evaluating it; do not assume that changing the account entitlement makes these exact definitions the preview interface.

After running the quickstart's `setup`, use its disposable database and schema. If you chose another database name, substitute it below. Run each statement individually so one expected error does not hide the remaining probes. The destination is VARIANT to match the original probes; the rejected definitions did not ingest data.

```sql
USE DATABASE SENSOR_TIMESERIES_DEMO;
USE SCHEMA SENSORS;
USE WAREHOUSE SENSOR_TIMESERIES_DEMO_WH;

CREATE TABLE probe_average (value VARIANT);
CREATE TABLE probe_deviation (value VARIANT);
CREATE TABLE probe_window (value VARIANT);

CREATE PIPE probe_average_pipe AS COPY INTO probe_average FROM (
  SELECT AVG($1:v::FLOAT)
  FROM TABLE(DATA_SOURCE(TYPE=>'STREAMING'))
);

CREATE PIPE probe_deviation_pipe AS COPY INTO probe_deviation FROM (
  SELECT STDDEV_POP($1:v::FLOAT)
  FROM TABLE(DATA_SOURCE(TYPE=>'STREAMING'))
);

CREATE PIPE probe_window_pipe AS COPY INTO probe_window FROM (
  SELECT AVG($1:v::FLOAT) OVER (PARTITION BY $1:sensor_id::STRING)
  FROM TABLE(DATA_SOURCE(TYPE=>'STREAMING'))
);
```

Record the complete SQL and Snowflake error for each statement. If an account accepts a definition, that is only the first capability check: send known messages and independently verify the output, grouping, replay and late-arrival rules. In particular, the unframed `OVER` expression does not define a 60-second event-time window.

The original errors distinguish unsupported operators from a disabled feature gate:

| Probe | CREATE result in the tested account |
|---|---|
| `AVG` | Error `091136`: unsupported operators in the pipe |
| `STDDEV_POP` | Error `091136`: unsupported operators in the pipe |
| Unframed window `AVG ... OVER` | Error `002364`: streaming window functions were disabled by feature gates |

These examples expect a numeric `v` field and a `sensor_id` field. The quickstart's `seed` command targets `sensor_pipe` using its different `value` field; it does not send messages to these probe pipes. Any follow-up ingestion test must explicitly select the accepted probe pipe and use its expected input shape.

The broader set verified **nine per-message expressions with live input**: extraction, arithmetic, conditional expressions, numeric conversion, `DATE_TRUNC`, `TIME_SLICE`, array size, square root and a scalar SQL UDF. Three needed matching destination types: the conditional output used VARCHAR, and the two time-bucketing outputs used TIMESTAMP_NTZ. Their first VARIANT-target attempts failed type checks; that was not evidence that the expressions were unsupported.

The **ten aggregate or relational definitions**—`AVG`, `STDDEV_POP`, grouping, an unframed window, filtering, DISTINCT, ordering, limit, join and flatten—were rejected in that account. These results describe the definitions tested, not every possible syntax or every product release. See the [standard streaming-pipe guide](https://docs.snowflake.com/en/user-guide/snowpipe-streaming/snowpipe-streaming-pipe-object) for its supported transformation rules.

The quickstart's ownership-checked cleanup drops its disposable database, including these probe tables and any accepted pipes:

```bash
python quickstart/demo.py cleanup
```

Use the same `--database` and connection options as setup. Do not run these statements in a shared production schema.

## Checking private-preview access

A separate access check used the two-join shape shown in Figure 5 of [Snowflake's Streaming Transformations announcement](https://www.snowflake.com/en/blog/real-time-analytics-snowflake/), alongside scalar, filter and aggregate controls. The scalar pipe definition was accepted. The join and filter definitions were rejected with `002098`, cross-row AVG and STDDEV_POP with `091136`, and an explicit 60-second RANGE window with `002364`, citing disabled feature gates. The disposable database was removed after the check; no warehouse or ingestion was needed.

This establishes that the probed preview capabilities were unavailable through that trial session. It does not establish the preview's general function limits or the account's contractual enrollment status.

There is no documented self-service SQL route to enable this private preview. [`SYSTEM$ENABLE_PREVIEW_ACCESS`](https://docs.snowflake.com/en/sql-reference/functions/system_enable_preview_access) applies to open previews; its usage notes direct private-preview requests to Snowflake Support. The [preview access policy](https://docs.snowflake.com/en/release-notes/preview-features) also directs requests through the Snowflake account team. Eligibility for this particular trial and region needs confirmation from Snowflake.
