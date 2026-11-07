# Measured experiment credit usage

These figures come from Snowflake Account Usage records for the completed experiments. They are credits consumed by the tracked resources, with no currency conversion, forecast or extrapolation from rates. The collection ran after the documented reporting delays; the experiment resources had already been removed.

## Recorded warehouse and ingestion credits

| Experiment component | Warehouse compute credits | Streaming ingestion credits |
|---|---:|---:|
| Main Dynamic Table lab | 2.406439800 | 0.001496030 |
| Interactive serving check | 0.600075600 | — |
| Quickstart validation A | 0.072259200 | 0.000000002 |
| Quickstart validation B | 0.064269000 | 0.000000002 |
| Portable Dynamic Table validation | 0.131664600 | 0.000000387 |
| Original pipe and SQL probes | 0.196628400 | 0.000911488 |
| Offline checks and earlier billing queries | 0.102135600 | — |
| **Tracked subtotal** | **3.573472200** | **0.002407909** |

**Warehouse compute plus streaming subtotal: 3.575880109 credits.** The main Dynamic Table lab alone accounts for **2.407935830 credits** across those two components.

The table preserves nine decimal places from the meter. A dash means no separate streaming pipe, not missing usage converted to zero. Very small positive ingestion values remain positive. The full measured records are available as [JSON](data/measured-credits.json) and [CSV](data/measured-credits.csv).

## What each row includes

- **Main Dynamic Table lab:** setup, fixtures, numeric and retention checks, automatically scheduled refreshes, polling, idle time, three 30-minute paced runs and an accelerated burst. The protocol ingested 5.52 million readings, but the meter includes the surrounding lab work too.
- **Interactive serving check:** its dedicated warehouse, including its minimum billable period. It read existing main-lab summaries; ingestion is not counted again.
- **Quickstart A and B:** two independent setup, seed, read and cleanup validations.
- **Portable Dynamic Table validation:** the separate reproducibility check, including its setup, observations and idle-expiry test.
- **Original pipe and SQL probes:** the earlier scalar/aggregate pipe probes, SQL checks and bulk ingestion. These are separate from the later 5.52-million-reading protocol.
- **Offline checks and earlier billing queries:** shared support warehouse work, including failed Online setup investigation, offline SQL checks, article examples and the earlier cost snapshots. It does not measure a running Online Feature Store service.

Each warehouse name had one recorded warehouse ID in the measured interval. Streaming records matched 16 distinct pipe IDs, including dropped-pipe metadata, across five experiment databases. Object IDs and account identifiers remain in private evidence; the public export contains only the labels and metrics above. The workloads differ, so these are not equal-capacity or equal-freshness offers.

## Main warehouse breakdown

| Main-lab warehouse compute | Credits |
|---|---:|
| Compute attributed to queries | 1.938393409 |
| Remaining warehouse compute (includes idle time) | 0.468046391 |
| Total main-lab warehouse compute | 2.406439800 |

These values come from `CREDITS_USED_COMPUTE` and `CREDITS_ATTRIBUTED_COMPUTE_QUERIES` in `WAREHOUSE_METERING_HISTORY`. The remaining amount is their difference. Query-attributed compute is contained within warehouse compute; adding both would double-count it. Idle time is not included in query-attributed compute. [Official column definitions](https://docs.snowflake.com/en/sql-reference/account-usage/warehouse_metering_history).

A separate query-attribution check assigned approximately **1.850265 compute credits** to 411 refresh records across the main lab's lifecycle. This is Snowflake's retrospective allocation, not an additional charge or an isolated sustained-run bill. The attribution view omits some very short queries and uses resource allocation within time intervals; it is not an exact per-statement invoice. [Query attribution definitions and limitations](https://docs.snowflake.com/en/sql-reference/account-usage/query_attribution_history).

## Amounts outside the subtotal

The seven tracked warehouses reported **0.146715571 unadjusted cloud-services credits**. The account-level daily record fully offset warehouse cloud-services usage for that day. That is account context, not a per-experiment allocation; unadjusted cloud-services usage is not added to the compute-and-streaming subtotal. [Daily metering and adjustments](https://docs.snowflake.com/en/sql-reference/account-usage/metering_daily_history).

The Query Acceleration history query succeeded and returned no rows for the tracked warehouses and interval. No usage rows is the reported result; it is not replaced with an invented zero charge.

Storage is outside this credit subtotal. No storage bytes or currency charges were converted into credits. The separate reporting warehouse used for this later reconciliation was also excluded: it was outside both the experiment resource list and measurement interval, and it was removed after collection.

Online Feature Store provisioning was blocked and private-preview Streaming Transformations was unavailable to the session. No live service cost is claimed for either. Failed setup and ordinary SQL work are included only where the tracked warehouse meter records them.

## Repeat the measurement

Use the [read-only metering SQL](metering.sql), supplying the exact resource names and complete UTC hour/day boundaries covering your run. Use a separate observer warehouse and account for it separately. Keep the warehouse ID and pipe ID when matching records so reused names cannot merge different resource lifetimes.

1. Sum warehouse compute from `WAREHOUSE_METERING_HISTORY`; preserve cloud-services usage as a separate field.
2. Join `METERING_HISTORY` to `PIPES` by pipe ID and name for high-performance streaming credits. Include dropped objects and preserve tiny reported amounts.
3. Check the dedicated Query Acceleration meter once. Do not add the same service again from a second metering view.
4. Read daily cloud-services adjustments as account-level context.
5. Sum raw decimal credit values before display rounding. Do not prorate hourly rows to individual test stages or infer production unit costs from this lab.

Warehouse usage can lag by three hours, cloud-services usage by six, and high-performance streaming credit reporting by twelve. Empty, failed or delayed queries do not establish zero cost. This collection waited beyond those delays; billing records can still be revised and the subtotal is not a complete storage-inclusive invoice. [Warehouse reporting](https://docs.snowflake.com/en/sql-reference/account-usage/warehouse_metering_history), [service metering](https://docs.snowflake.com/en/sql-reference/account-usage/metering_history), [streaming billing](https://docs.snowflake.com/en/user-guide/snowpipe-streaming/snowpipe-streaming-high-performance-cost).
