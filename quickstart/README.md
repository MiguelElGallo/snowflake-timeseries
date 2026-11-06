# A working minute-summary example

This example creates an isolated transient database, schema, X-Small standard warehouse, streaming pipe and two Dynamic Tables. It refuses to replace existing resources. It uses Snowpipe Streaming's high-performance Python SDK with an existing key-pair connection.

## Prerequisites

- Python 3.12 and a Snowflake account with Snowpipe Streaming high-performance access and Elastic Channels (the producer uses `get_elastic_channel()`).
- An existing named connection in `~/.snowflake/connections.toml`, containing `account`, `user`, `private_key_file`, and optionally `role` and `authenticator`. This minimal producer supports an unencrypted private key protected by filesystem permissions. It does not change credentials or grants. See Snowflake's [key-pair authentication guide](https://docs.snowflake.com/en/user-guide/key-pair-auth).
- A role authorized to create the isolated database and warehouse, and objects inside its own schema. Ask your account administrator for appropriate privileges; the example does not require naming an administrator role in code.
- Sufficient credit budget. Dynamic Tables refresh on a standard warehouse. Stop and clean up the example when finished.

## Run

From the repository root:

```bash
python3.12 -m venv .venv
source .venv/bin/activate
python -m pip install -r quickstart/requirements.txt
export SNOWFLAKE_CONNECTION_NAME=your_existing_keypair_connection
python quickstart/demo.py setup
python quickstart/demo.py seed
python quickstart/demo.py read
```

The default database is `SENSOR_TIMESERIES_DEMO` and its warehouse is `SENSOR_TIMESERIES_DEMO_WH`. To use another name, pass the same `--database YOUR_NEW_NAME` to every command. `--connections-file` selects another TOML file; `--connection` overrides the environment variable.

`seed` sends six messages: four valid readings (10, 20, 30, 40), a replay of 10 with the same measurement ID, and one malformed value. All event times belong to one completed UTC minute. A unique sensor ID distinguishes each invocation. It waits up to four minutes for the **automatically maintained** summary and verifies:

| Distinct readings | Valid values | Average | Population SD | Sample SD |
|---:|---:|---:|---:|---:|
| 5 | 4 | 25 | 11.180339887… | 12.909944487… |

Acknowledgement of streaming delivery does not by itself prove that the summary is visible. The script waits for and checks the numeric result separately.

Read [tables.sql](tables.sql) for the complete table, pipe and Dynamic Table definitions. The runner substitutes its validated warehouse name for `__WAREHOUSE__`.

The schema assumes `(sensor_id, event_id)` identifies one measurement whose value and event time never change. Resending the same measurement is safe for the summary; a correction policy needs an explicit version/order rule. Equal measurements with different IDs remain separate readings. All `TIMESTAMP_NTZ` values in this example represent UTC. A malformed **value** becomes NULL and its original text is kept; malformed IDs/timestamps are not a general quarantine mechanism and can fail ingestion.

`NUMBER(38,6)` keeps six decimal places. Choose a precision and scale that fit your sensor's range and resolution. Values outside the chosen range become NULL through `TRY_TO_DECIMAL`; excess fractional precision is rounded.

## Optional examples

```bash
python quickstart/demo.py moving-window
python quickstart/demo.py precision
```

[moving_window.sql](moving_window.sql) maintains the current `(now − 60 seconds, now]` window. The seed uses a **completed minute**, so it is normally absent from this current window. The [larger experiment](../experiments/dynamic-tables/README.md) supplies fresh readings and tests expiry during silence. A 60-second freshness target is not a promise to change the table every second.

[precision.sql](precision.sql) compares a large-offset FLOAT standard deviation with fixed-point and centered calculations. It is a deliberately difficult numeric example, not a claim that ordinary FLOAT data always fails.

## Clean up

```bash
python quickstart/demo.py cleanup
```

The command reads the local `.state` ownership manifest, verifies the account and object comments, and removes only the database and warehouse created by this run. Keep that state directory until cleanup succeeds. The database is disposable; dropping it removes the example tables, pipe and Dynamic Tables. A fresh run should use a new database name or archive the cleaned ownership file before reusing the name.

The setup and reading commands also use the warehouse. Their usage is part of this example's cost. This demo does not measure a complete monthly bill.
