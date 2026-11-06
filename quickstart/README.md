# A working minute-summary example

This example creates an isolated transient database, schema, X-Small standard warehouse, streaming pipe and two Dynamic Tables. It refuses to replace existing resources. It uses Snowpipe Streaming's high-performance Python SDK with an existing key-pair connection.

## Prerequisites

- Python 3.12 and a Snowflake account with Snowpipe Streaming high-performance access and Elastic Channels (the producer uses `get_elastic_channel()`).
- An existing named connection in `~/.snowflake/connections.toml`, containing `account`, `user`, `private_key_file`, and optionally `role` and `authenticator`. This minimal producer supports an unencrypted private key protected by filesystem permissions. It does not change credentials or grants. See Snowflake's [key-pair authentication guide](https://docs.snowflake.com/en/user-guide/key-pair-auth).
- A role authorized to create the isolated database and warehouse, and objects inside its own schema. Ask your account administrator for appropriate privileges; the example does not require naming an administrator role in code.
- Sufficient credit budget. Dynamic Tables refresh on a standard warehouse. Stop and clean up the example when finished.

## 1. Prepare your environment

From the repository root:

```bash
python3.12 -m venv .venv
source .venv/bin/activate
python -m pip install -r quickstart/requirements.txt
export SNOWFLAKE_CONNECTION_NAME=your_existing_keypair_connection
```

Replace `your_existing_keypair_connection` with the name of your compatible connection in `~/.snowflake/connections.toml`.

## 2. Create the example

```bash
python quickstart/demo.py setup
```

The default database is `SENSOR_TIMESERIES_DEMO` and its warehouse is `SENSOR_TIMESERIES_DEMO_WH`. Both names must be unused. Success prints:

```text
Ready. Run seed, then read. Run cleanup when finished.
```

## 3. Send readings and check the answer

```bash
python quickstart/demo.py seed
python quickstart/demo.py read
```

`seed` sends six messages: four valid readings (10, 20, 30, 40), a replay of 10 with the same measurement ID, and one malformed value. All event times belong to one completed UTC minute. A unique sensor ID distinguishes each invocation. It waits up to four minutes for the **automatically maintained** summary and verifies:

| Distinct readings | Valid values | Average | Population SD | Sample SD |
|---:|---:|---:|---:|---:|
| 5 | 4 | 25 | 11.180339887… | 12.909944487… |

Acknowledgement of streaming delivery does not by itself prove that the summary is visible. The script waits for and checks the numeric result separately.

Success from `seed` starts with `PASS: duplicate excluded, invalid value retained as NULL`, followed by the result. `read` prints column names and the stored row, including the generated sensor ID and minute. If the summary does not pass within four minutes, `seed` raises an error; that is not a successful run.

## 4. Clean up

```bash
python quickstart/demo.py cleanup
```

Success prints:

```text
Removed the demo resources. Keep or archive the local ownership file.
```

The command reads the local `.state` ownership manifest, verifies the account and object comments, and removes only the database and warehouse created by this run. Keep that state directory until cleanup succeeds. Dropping the disposable database removes its tables, pipe and Dynamic Tables. A fresh run should use a new database name or archive the cleaned ownership file before reusing the name.

You have sent real messages through a streaming pipe and verified a maintained minute summary, including a replay and an invalid value. The setup and reading commands also use the warehouse; their usage is part of the example's cost.

## Understand or adapt the example

Read [tables.sql](tables.sql) for the complete table, pipe and Dynamic Table definitions. The runner substitutes its validated warehouse name for `__WAREHOUSE__`.

The schema assumes `(sensor_id, event_id)` identifies one measurement whose value and event time never change. Resending the same measurement is safe for the summary; a correction policy needs an explicit version/order rule. Equal measurements with different IDs remain separate readings. All `TIMESTAMP_NTZ` values in this example represent UTC. A malformed **value** becomes NULL and its original text is kept; malformed IDs/timestamps are not a general quarantine mechanism and can fail ingestion.

`NUMBER(38,6)` keeps six decimal places. Choose a precision and scale that fit your sensor's range and resolution. Values outside the chosen range become NULL through `TRY_TO_DECIMAL`; excess fractional precision is rounded.

To use another database name, pass the same `--database YOUR_NEW_NAME` to every command. `--connections-file` selects another TOML file; `--connection` overrides the environment variable.

For optional branches, use the separate [moving-window and precision recipes](../docs/how-to-explore.md). They require the example resources to exist: run them before cleanup, or create a fresh example first. The [article](../ARTICLE.md) explains the design choices, and the [cost worksheet](../docs/costs.md) lists assumptions and billing units. This tutorial does not measure a complete monthly bill.
