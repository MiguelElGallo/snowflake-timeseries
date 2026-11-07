# Read existing dynamic-table results with Interactive Analytics

This is a **capability smoke test, not a performance benchmark**. It creates one temporary X-Small interactive warehouse, reads ten existing fixture-summary rows in three queries, validates counts/averages/standard deviations including NULLs, and drops the warehouse in a `finally` block. It never modifies the source table or creates a copy of its data.

Snowflake documents that interactive warehouses can query dynamic tables directly. Attaching tables is optional; this test uses no attachment or fallback warehouse. See [Interactive Analytics](https://docs.snowflake.com/en/user-guide/interactive#all-table-format-support) and [warehouse creation syntax](https://docs.snowflake.com/en/sql-reference/sql/create-interactive-warehouse).

## Prerequisites

- Python 3.12 (the validated runtime) and the dependency below.
- An existing Snowflake connection profile, permission to create a warehouse, and permission to read the existing fixture summaries.
- A completed [dynamic-table fixture experiment](../dynamic-tables/README.md), including its late-arrival and replay checks. This script expects the published `DT.MINUTE` schema and its `fixture` rows. It is not a generic reader for arbitrary table layouts.
- Interactive Warehouse availability in your account and region. A rejected creation is saved as evidence; the script does not attempt to work around availability restrictions.

From this directory:

```sh
python -m pip install -r requirements.txt
python smoke.py local
export SNOWFLAKE_CONNECTION_NAME=your_existing_connection
python smoke.py run --source-table YOUR_LAB_DATABASE.DT.MINUTE
```

The local check has no cloud dependencies or credential requirement. `run` creates a fresh random warehouse name. Optional flags include `--warehouse YOUR_FRESH_NAME`, `--connection`, `--connections-file`, and `--output-dir`. The default fixture time matches the public dynamic-table experiment; `--fixture-base` is for an intentionally different synthetic fixture minute.

The account/role comes from your local connection file. No credentials are embedded in this repository. A key-pair profile can use `private_key_file` and the connector's password field for encrypted keys. The script expands a home-relative key path.

## What the test establishes

Three queries read these existing summary groups:

1. Equal-valued distinct events, all-NULL values, and a singleton valid value.
2. Historical minute buckets corrected by a genuinely late event.
3. Events exactly around minute boundaries.

The independent expected values cover ten rows. Key coverage and counts are exact, NULLs must match, and numeric values use an absolute tolerance of `1e-8`. Each returned row includes `CURRENT_WAREHOUSE()`, checked against the new warehouse; `SHOW WAREHOUSES` must report type `INTERACTIVE`. Persisted query-result reuse is disabled, and an empty fallback configuration is verified before querying.

This proves only that the tested account could read these already-materialized results correctly through that warehouse. It does not measure statistical refresh freshness, high concurrency, general query compatibility, or a speed advantage. Client elapsed times include request/response and possibly startup or cache work; three different smoke queries do not support latency percentiles.

## Cost and cleanup

The completed Interactive check recorded **0.600075600 warehouse compute credits**, including its minimum billable period. See the [measured credit report](../../docs/costs.md) for scope and separate cloud-services usage. Interactive warehouses have a **one-hour minimum billable period** each time they resume. Automatic suspension has a 24-hour minimum, so the script drops the warehouse explicitly after the check. [Billing rules](https://docs.snowflake.com/en/user-guide/interactive#cost-and-billing-considerations).

The script uses one cluster, refuses existing warehouse names, binds ownership to organization/account/region, and verifies a unique warehouse comment before dropping it. Ownership is cleared after deletion. If creation is rejected, it verifies that no owned warehouse remains. If a network failure leaves creation uncertain, cleanup can recover only the warehouse bearing this run's unique marker.

If the process is interrupted before its `finally` block completes, keep the local output directory and run:

```sh
python smoke.py cleanup
```

Use the same `--output-dir` and connection if you changed them. Cleanup will not adopt or drop a warehouse with a different account identity or marker. It does not touch the existing source database, dynamic tables or their refresh warehouse.

The ignored `artifacts/` directory contains ownership, query IDs, exact results, elapsed times and cleanup status. These generated files can expose account/resource names and should be reviewed before sharing. The source code includes no private trial evidence.
