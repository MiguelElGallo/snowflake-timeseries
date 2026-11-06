# Try a moving window or a precision check

Use these recipes after verifying the minute summary in the [tutorial](../quickstart/README.md) and before running cleanup. Keep the same virtual environment, connection and database options. If you already ran cleanup, create a fresh example using a new database name and pass that name to every command.

## Create a current 60-second summary

```bash
python quickstart/demo.py moving-window
```

The command creates the definition in [moving_window.sql](../quickstart/moving_window.sql) and prints the creation result. To inspect its rows, run this SQL in your Snowflake worksheet using the same role (substitute your database and warehouse names if customized):

```sql
USE WAREHOUSE SENSOR_TIMESERIES_DEMO_WH;
SELECT *
FROM SENSOR_TIMESERIES_DEMO.SENSORS.SENSOR_LAST_MINUTE
ORDER BY sensor_id;
```

Its time range is `(now − 60 seconds, now]`: the lower boundary is excluded and the upper boundary is included. The tutorial sends readings from a completed minute, so this table normally has no row for them. An empty query result here is expected; it does not demonstrate expiry or fresh-data latency. The tutorial's `read` command reads the clock-minute table, not this moving window.

To exercise fresh readings, late arrivals and expiry during silence, follow the separate [Dynamic Table experiment](../experiments/dynamic-tables/README.md). Its own setup and cleanup are independent of the tutorial. A 60-second target lag is a freshness objective, not a promise to change the table every second.

## Check a difficult standard deviation

```bash
python quickstart/demo.py precision
```

This runs [precision.sql](../quickstart/precision.sql), comparing five values near one trillion. The expected population standard deviation is `sqrt(2)`, about `1.4142`. Compare the FLOAT result with the fixed-point and centered results. The experiment observed zero for the FLOAT aggregate; do not generalize that to every FLOAT input or every future implementation.

## Remove the tutorial resources

```bash
python quickstart/demo.py cleanup
```

Keep `.state` until cleanup succeeds. The command verifies ownership before removing the database and warehouse it created.
