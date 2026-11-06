# Dynamic-table sensor experiment

This standalone harness ingests sensor readings through high-performance Snowpipe Streaming, deduplicates events, and maintains statistics in dynamic tables. It checks fixed-minute buckets, two different rolling-window definitions, late arrivals, replay, numerical precision, and expiration during silence. It also measures when automatically refreshed summaries first contain the correct readings.

The tested pipe extracts and converts individual readings. This experiment does **not** test the separate Streaming Transformations private preview announced by Snowflake, which includes aggregation in a pipe. See [Snowflake's real-time analytics announcement](https://www.snowflake.com/en/blog/real-time-analytics-snowflake/) for that distinct path. No result here establishes the limits of every pipe feature or account entitlement.

## Requirements and configuration

Use Python 3.12 (the validated runtime), a Snowflake account with high-performance Streaming and Elastic Channels available, and a key-pair connection profile. The role must be allowed to create a fresh database and warehouse and to create/manage its tables, pipes and dynamic tables. Resource availability can differ by account, region and release. The script creates a transient database with zero-day retention for disposable synthetic data; this is not a production retention policy.

From this directory:

```sh
python3.12 -m venv .venv
source .venv/bin/activate
python -m pip install -r requirements.txt
export SNOWFLAKE_CONNECTION_NAME=my_sensor_lab
```

The named entry in your local `~/.snowflake/connections.toml` needs these fields. Keep the real file and key outside the repository:

```toml
[my_sensor_lab]
account = "organization-account"
user = "YOUR_USER"
role = "YOUR_LAB_ROLE"
authenticator = "SNOWFLAKE_JWT"
private_key_file = "/absolute/path/to/your/key.p8"
```

For an encrypted key, use `SNOWFLAKE_PRIVATE_KEY_PASSPHRASE` from your secret manager or the supported local profile password field. The helper maps it to each client's parameter and never logs it. See [key-pair setup](https://docs.snowflake.com/en/user-guide/key-pair-auth) and [Streaming client configuration](https://docs.snowflake.com/en/user-guide/snowpipe-streaming/snowpipe-streaming-high-performance-configurations).

| Environment variable | Meaning |
|---|---|
| `SNOWFLAKE_CONNECTION_NAME` | Required local profile name; there is no account-specific default. |
| `SNOWFLAKE_CONNECTIONS_FILE` | Optional alternative TOML path. |
| `SNOWFLAKE_LAB_ARTIFACT_DIR` | Output directory; defaults to ignored `artifacts/` beside these scripts. Use a fresh directory for a new experiment. |
| `SNOWFLAKE_LAB_DATABASE` | Optional fresh SQL identifier. Otherwise a random `TIMESERIES_…` name is generated and saved locally. |
| `SNOWFLAKE_LAB_WAREHOUSE` | Optional fresh warehouse name; defaults to the generated database name plus `_WH`. |
| `SNOWFLAKE_LAB_WAREHOUSE_GENERATION` | Optional `1` or `2`; otherwise account default. Actual configuration is recorded. |
| `SNOWFLAKE_STREAMING_URL` | Optional explicit HTTPS account URL, useful when the standard account hostname is unsuitable. |
| `SNOWFLAKE_PRIVATE_KEY_PASSPHRASE` | Optional encrypted-key passphrase; never put its value in a command committed to Git. |

The dedicated warehouse is X-Small with auto-suspend at 60 seconds. Query acceleration is explicitly disabled in this public harness to simplify cost attribution. Actual experiments should record warehouse generation: X-Small does not have one universal hourly price.

## Ordered experiment

First check the independent Python reference calculations, without network access or credentials:

```sh
python dt_oracle.py
python dt_run.py local
python local_checks.py
```

The following commands create resources and consume Snowflake credits. Setup refuses existing resource names rather than replacing them. Fixture, follow-up and paced stages are intentionally single-use in one artifact directory; this prevents silent duplicate ingestion on rerun.

```sh
python admin.py setup
python dt_run.py setup
python dt_run.py correctness
python dt_precision.py
python dt_followup.py
python dt_run.py paced
python admin.py capture
python admin.py suspend
```

`correctness` exercises ordinary statistical semantics and records large-offset floating-point differences as separate diagnostic findings. `dt_precision.py` is read-only and compares direct, centered and fixed-point SQL statistics. `dt_followup.py` materializes precision alternatives and runs the explicit-UTC idle-expiration test with an implicit-timezone control. The control's result is observed, not assumed to fail in every environment. Follow-up tables are suspended after their evidence is saved. Main minute summaries remain active for the paced stage.

The modest paced default is **10 sensors, 120 seconds, one run**, followed by a burst containing 30 seconds of events. A 60-second target lag and bounded final drain mean the command can take longer than two minutes. To opt into the sustained protocol explicitly:

```sh
python dt_run.py paced --sensors 1000 --seconds 1800 --repeats 3 --idle-seconds 300 --burst-seconds 120
```

Choose one paced configuration per fresh experiment. The sustained version sends 5.4 million paced events and another 120,000 burst events, plus the small fixtures. It includes five-minute gaps between the three 30-minute runs. The burst is deliberately sent faster than its event timestamps advance; its events can be ahead of the current wall clock. It measures fixed-minute aggregation and recovery, not a current-time filtering guarantee.

If a stage fails, inspect its artifacts. Do not delete its stage marker and repeat ingestion against the same data. Suspend or clean up, then use a new artifact directory and fresh resources. The optional `dt_followup.py --finalize-only` can finish suspension/result assembly only when all required follow-up evidence already exists; it does not rerun fixtures.

## What is checked

| Case | Acceptance rule |
|---|---|
| Fixed-minute buckets | Event time in `[minute start, next minute start)`; exact group coverage and seven statistics. |
| Explicit as-of window | Event time in `(as of − 60 seconds, as of]`; exact boundary values included/excluded deliberately. |
| Event-anchored SQL `RANGE` | Both ends inclusive: `[event time − 60 seconds, event time]`; equal timestamps share the frame. |
| Cross-bucket late event | A reading at second 15 arrives after second 180 has already been checked; affected historical statistics change correctly. |
| Replay after client restart | Replayed event ID advances the deduplication marker but leaves semantic counts and statistics unchanged. |
| Equal readings, distinct IDs | Both readings count even when timestamp and value are equal. |
| NULL, invalid and singleton | Event count differs from valid-value count; sample standard deviation is NULL with one valid observation. |
| Numerical stress | Values near one trillion with spread of only a few units are compared with a centered, 50-digit Decimal calculation. |
| Silence | One fresh server-timed event first appears, then expires without additional input or manual refresh during observation. |

SQL definitions explicitly request `REFRESH_MODE=INCREMENTAL`. Any rejected rolling definition is saved as a capability result, not silently replaced by a different computation. Inspect `dt_setup.json` and the resolved modes in `dt_show.json`.

See [dynamic-table supported queries](https://docs.snowflake.com/en/user-guide/dynamic-tables-supported-queries), [refresh behavior](https://docs.snowflake.com/en/user-guide/dynamic-tables-refresh), [window syntax](https://docs.snowflake.com/en/sql-reference/functions-window-syntax), and [standard-deviation semantics](https://docs.snowflake.com/en/sql-reference/functions/stddev).

## How latency is measured

Correctness fixtures use explicit refreshes so semantic failures are easy to isolate. **Paced latency uses automatic refreshes only.** A small panel of sensors is polled about every five seconds. An event is counted as visible only when a returned minute-summary revision has the exact count, mean, standard deviations, minimum and maximum expected for the complete sequence prefix containing it. Partial or out-of-order prefixes are logged as nonqualifying observations and revisited.

This measures producer send to **first observed correct containing summary**, including polling/query delay. It does not measure only arrival in the raw or deduplicated table. The default panel is sensors 0, 1, 17 and 999 when those IDs exist in the configured workload. Final validation independently checks every sensor, every expected bucket, exact key coverage, NULL behavior and the generated raw/canonical domain.

Acknowledgement latency is separately labeled an **observed upper bound**, because the producer may collect an already completed future later. Warm-up (the first 120 event seconds) and later observations are separated. Partial results and unobserved events are retained on failure; do not report successful observations alone as a complete latency distribution. The short default has no post-warm-up interval.

## Artifacts and cleanup

Artifacts include definitions, resolved modes, query IDs, refresh history, correctness diagnostics, event/summary observation times, progress and final result JSON. They are written locally, flushed throughout ingestion, and ignored by Git. They can contain your account, user, resource names and query text. Review and redact them before sharing; this repository includes no raw trial logs.

`admin.py capture` records delayed warehouse metering, including observer queries and idle time. It is not a pure refresh-cost measurement or a complete invoice. Storage and Streaming ingestion are separate charges. Recheck usage after reporting delays before making a final cost claim.

To remove **only the database and warehouse created by this harness**, run:

```sh
python admin.py cleanup
```

The ownership manifest records the live organization, account and region as well as resource names. Every modifying stage verifies this identity; switching profiles to a different account fails before a resource action. Successful drops immediately clear ownership so a later cleanup does not drop a reused name. Setup refuses adopting a preexisting resource. Keep the manifest until cleanup is verified. `admin.py suspend` stops scheduled refreshes and suspends the warehouse while retaining data; storage charges can continue. `dt_run.py cleanup` suspends the known dynamic tables only; `admin.py cleanup` performs full resource deletion.

This exact public harness completed a fresh end-to-end trial run with its modest defaults, including all documented stages and verified resource deletion. No implementation fixes or reruns were needed. [Validation results](VALIDATION.md) separate successful correctness checks from the deliberately exposed FLOAT precision finding. A fresh run remains necessary to verify behavior and availability in another account or environment.
