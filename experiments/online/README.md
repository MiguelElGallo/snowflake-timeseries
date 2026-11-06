# Online Feature Store experiment

**Prepared and locally tested; UNVERIFIED LIVE.** The original November 2026 trial in Azure East US 2 rejected an XS Postgres-backed Online Feature Store service with error `604056`: its managed `HIGHMEM_XL` compute family was unavailable to that trial. The failed service was removed and its status returned `NOT_FOUND`. No Online feature registration, REST ingestion, online statistics, expiry, latency or sustained throughput completed in that experiment.

This directory is standalone. It contains original experiment code and an original minimal connection helper; it does not redistribute Cortex Code skill files. No connection profiles, keys or raw account logs are included.

## What is prepared

The pipeline uses a `StreamSource`, a Python passthrough transformation, and a 60-second `FeatureAggregationMethod.CONTINUOUS` feature view on the Postgres Online store. It defines event count, non-NULL value count, mean, population SD and population variance. It is a separate ingestion/serving path from a Snowpipe `PIPE`.

| Phase | Work |
| --- | --- |
| Capability | Record SDK/API availability and target service state; create explicit XS only if absent; preserve an existing service. |
| Registration | Permanent seeded backfill table, stream source, sensor entity and a continuous 60-second feature view. |
| Semantics | Ordinary values, NULLs, singleton, equal values, same-time distinct events, replay, lateness, future timestamps and a large-offset precision diagnostic. |
| Expiry/replay | Observe the current window without new input, then recreate HTTP client sessions/JWT and replay with a new marker. This is not an operating-system process restart. |
| Controlled latency | Warm up for at least 10 seconds; measure ingest response, first correct count/mean/SD/variance visibility, and lookup latency separately. |
| Load | Three 30-minute segments of 1,000 sensors at 1 Hz, five-minute idle checks, then a 30-second burst at 10,000 events/s and recovery. |

The load phase checks all 1,000 entity payloads after each segment. It does **not** continuously benchmark lookups during sustained ingestion. It records target, offered and accepted rates, bounded queue saturation, shed events, request failures and drain censoring. Completed requests are reaped before capacity decisions. A late acknowledgement remains censored if it missed the drain deadline; censored counts overlap eventual accepted/failed counts and should not be added to them.

The rolling hypothesis is `(query_time - 60 seconds, query_time]`, using measured client/server clock bounds. Ambiguous boundary observations are retried only for the affected entities. Wrong results remain failures. Empty-window `MISSING_DATA` is recorded distinctly from numeric zero. A strict semantic/expiry/replay gate tied to the service creation manifest must pass before latency or load starts.

The local suite includes 18 REST response-contract tests, 27 fake-transport scheduler/replay tests, 12 connection-helper tests using generated keys and fake profiles, and 10 ownership-guard tests. These are client tests, not evidence that Snowflake's Online backend executed the workload.

## Install and run local checks

Use Python 3.12 and a virtual environment. From this directory:

```sh
python3.12 -m venv .venv
. .venv/bin/activate
python -m pip install -r requirements.txt

python online_contract_checks.py
python online_control_checks.py
python online_session_checks.py
python online_ownership_checks.py
python online_run.py
```

The four check commands use local fixtures/fake transports/generated keys and need no Snowflake credentials. The last command prints the execution gate and performs no cloud operation. By default, local results go to ignored `evidence/`; set `TS_LAB_EVIDENCE_DIR` to choose another directory.

## Configure an eligible lab account

Use an explicitly selected existing Snowflake CLI key-pair profile. The helper reads `connections.toml` from `$SNOWFLAKE_HOME` (default `~/.snowflake`) or the exact file specified by `SNOWFLAKE_CONNECTIONS_FILE`. It never silently selects a different profile.

The profile must contain `account`, `user`, `private_key_file`, and use `authenticator = "SNOWFLAKE_JWT"`. A profile `role` is optional. The key must be RSA. For an encrypted key, supply its passphrase through `SNOWFLAKE_PRIVATE_KEY_PASSPHRASE`; the helper does not read passwords from other profile fields. Secrets are never printed or placed in HTTP evidence.

```sh
export SNOWFLAKE_CONNECTION_NAME='your_lab_profile'
export SNOWFLAKE_DATABASE='TIMESERIES_ONLINE_LAB'
export SNOWFLAKE_SCHEMA='ONLINE_LAB'
export SNOWFLAKE_WAREHOUSE='TIMESERIES_ONLINE_WH'
export SNOWFLAKE_PRODUCER_ROLE='YOUR_EXISTING_FS_PRODUCER_ROLE'
export SNOWFLAKE_CONSUMER_ROLE='YOUR_EXISTING_FS_CONSUMER_ROLE'
export SNOWFLAKE_CLIENT_LOCATION='describe your client and region'
```

Database/schema/warehouse have the generic defaults shown above. The profile and producer/consumer roles have no default. These identifiers must be unquoted letters/digits/underscores, beginning with a letter or underscore; the schema is limited to 42 characters. Choose a new isolated database/schema and a fresh evidence directory for each independent run.

Provision the lab database, schema and warehouse yourself before running the capability script. For example, with an authorized role:

```sql
CREATE DATABASE TIMESERIES_ONLINE_LAB;
CREATE SCHEMA TIMESERIES_ONLINE_LAB.ONLINE_LAB;
CREATE WAREHOUSE TIMESERIES_ONLINE_WH
  WAREHOUSE_SIZE = 'XSMALL'
  AUTO_SUSPEND = 60
  AUTO_RESUME = TRUE
  INITIALLY_SUSPENDED = TRUE;
```

Configure existing producer/consumer account roles according to the [Feature Store setup documentation](https://docs.snowflake.com/en/developer-guide/snowflake-ml/feature-store/online-feature-store). The harness does not create roles, change network policies, upgrade a trial or modify payment details. Region support does not establish account entitlement. The documented service API exposes size selection, not a backend compute-family override.

## Execute deliberately

The Online service bills while it exists. Warehouse refresh/writeback costs are separate. Read the configuration and code before enabling cloud mutations.

```sh
# Read-only target status and inventory; requires the configured lab objects.
python online_capability.py

# Optional read-only SQL baseline plus client-only SDK definition probes.
python online_offline_probe.py

# Creates XS only after checking absence and persisting ownership intent.
python online_capability.py --setup
```

Proceed only if capability returns `READY`. A blocked create exits unsuccessfully and retains its precise error. An error may leave a failed service record: run `python online_capability.py --cleanup` to reconcile it. The scoped cleanup requires the same ownership manifest, including a matching live organization/account locator/region. A changed profile cannot silently redirect ownership to another account. A manifest that already confirmed cleanup cannot delete a later service at the reused locator. `--setup --retry` permits a prior unsuccessful, uncreated attempt only after another `NOT_FOUND` check and archives the earlier manifest.

Once eligible and ready, either run all reviewed phases with cleanup at the end:

```sh
python online_run.py --run --phase all
```

Or inspect each phase before continuing:

```sh
python online_run.py --run --phase register --keep-service
python online_run.py --run --phase semantics --keep-service
python online_run.py --run --phase latency --keep-service
python online_run.py --run --phase load
python online_capability.py --cleanup
```

`--keep-service` deliberately retains infrastructure and billing between phases. If a retained phase fails, perform explicit cleanup. Without that flag, `online_run.py` cleans the owned service in `finally`. The final load result can be failed/censored; do not infer success from the process finishing. Full local evidence files retain per-test outcomes, response metadata, request/query IDs and exact expected/actual payloads. Keep those files private: account inventory and object names can appear even though authentication headers and keys are excluded.

Service cleanup does not remove the database, schema, warehouse or feature metadata. After verifying `NOT_FOUND`, remove only the isolated lab resources you created. Do not delete the ownership manifest until service cleanup is confirmed. No code in this directory deletes account-wide infrastructure.

## Statistical and interpretation limits

The SDK's `Feature.stddev` docstring specifies population SD. A sample SD can be derived as `population_sd * sqrt(n/(n-1))` for `n > 1`, with NULL for `n <= 1`; this is a client conversion, not a native DDOF option. The server implementation remained untested in the original account.

The ordinary SQL fallback passed basic, singleton and NULL cases. Its diagnostic inputs `1e12 + [0.25, 0.50, 0.75]` returned zero FLOAT variance/SD in the original environment, although expected population SD was about `0.2041241452`. Centering on `1e12` recovered the result. That is ordinary SQL evidence, not Online Feature Store evidence or a universal failure threshold.

Client constructors also accepted intentionally questionable definitions. Constructor acceptance alone is not proof that registration or execution supports them. Source definitions, transport acknowledgements, correct feature visibility and cost are separate observations.

See the official [Online Feature Store guide](https://docs.snowflake.com/en/developer-guide/snowflake-ml/feature-store/online-feature-store), [aggregation definitions](https://docs.snowflake.com/en/developer-guide/snowflake-ml/feature-store/advanced-feature-engineering), [Ingest API](https://docs.snowflake.com/en/developer-guide/snowflake-ml/feature-store/online-feature-store-ingest-api-reference), [Query API](https://docs.snowflake.com/en/developer-guide/snowflake-ml/feature-store/online-feature-store-query-api-reference), and [floating-point guidance](https://docs.snowflake.com/en/sql-reference/data-types-numeric#rounding-errors).
