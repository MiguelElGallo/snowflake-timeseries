# Cost worksheet

Published rates are inputs to a scenario, not measured performance or a quote. Your contract, region, warehouse generation, workload and retained bytes determine the real bill. Trial credits do not change the rate model.

## One common scenario

- 1,000 sensors, one reading per second.
- 30 days = 2,592,000 seconds = 720 hours.
- 100 **assumed billable bytes** per raw reading.
- Decimal GB: 1,000,000,000 bytes.
- Illustrative dollars per credit: **$3**, not an assertion about any user's contract.

```text
readings = 1,000 × 2,592,000 = 2,592,000,000
raw GB = readings × 100 / 1,000,000,000 = 259.2
high-performance streaming credits = 259.2 × 0.0037 = 0.95904
illustrative ingestion dollars = 0.95904 × 3 = $2.87712
```

If every sensor instead sends one assumed 200-byte summary per minute:

```text
summary messages = 1,000 × 43,200 = 43,200,000
summary GB = 43,200,000 × 200 / 1,000,000,000 = 8.64
streaming credits = 8.64 × 0.0037 = 0.031968
illustrative ingestion dollars = 0.031968 × 3 = $0.095904
```

This saves about $2.78/month in ingestion in the assumed scenario. It excludes upstream aggregation, delivery, state recovery, raw storage elsewhere and all Snowflake query/refresh/storage charges. It does not imply that external aggregation is cheaper overall.

The billable-byte definition matters. Do not equate a compressed network payload, JSON file length or typed values with billing bytes without checking the selected mapping and current metering. In particular, VARIANT messages and mapped typed columns need not have identical accounting. See [high-performance streaming cost](https://docs.snowflake.com/en/user-guide/snowpipe-streaming/snowpipe-streaming-high-performance-cost) and [best practices](https://docs.snowflake.com/en/user-guide/snowpipe-streaming/snowpipe-streaming-high-performance-best-practices).

## Published compute rates used in the article

| Component | Credits/hour | 720-hour credits | Dollars at assumed $3/credit |
|---|---:|---:|---:|
| Azure Gen2 standard X-Small | 1.25 | 900 | 2,700 |
| Interactive X-Small, one cluster | 0.6 | 432 | 1,296 |
| Online Feature Store XS baseline | 0.76 | 547.2 | 1,641.60 |
| Online Feature Store S baseline | 2.66 | 1,915.2 | 5,745.60 |

Source: [Snowflake Service Consumption Table](https://www.snowflake.com/legal-files/CreditConsumptionTable.pdf). The table also lists Online autoscaling at 0.396 credits per node-hour and Online storage at $465/TB-month. Those are additional dimensions, not evidence that XS or S can carry this experiment's load. Standard database storage is separately priced by provider/region/contract; do not apply the Online storage rate to ordinary Snowflake tables.

These services have different purposes and capacity. A lower credit rate is not proof of lower cost per correct answer. An Interactive warehouse reading a Dynamic Table adds serving compute to the refresh compute on a standard warehouse.

## Serverless task and materialized-view examples

The same consumption table defines a normalized compute-hour as resources comparable to one X-Small warehouse running for an hour. A larger allocation running for less time can consume the same amount. Standard serverless task compute is **0.9 credits per normalized compute-hour**; materialized-view maintenance compute is **2 credits per normalized compute-hour**. Applicable cloud-services charges are additional. These are not prices per scheduled invocation or per elapsed wall-clock hour.

| Explicit assumption | Computation | Illustrative compute cost |
|---|---|---:|
| One serverless task/minute, each consuming 10 XS-equivalent compute-seconds | 43,200×10/3,600×0.9 | 108 credits/month; $324 at $3/credit |
| Materialized-view maintenance consuming 2 normalized compute-hours/day | 2×30×2 | 120 credits/month; $360 at $3/credit |
| Occasional SQL using 12 billable standard-warehouse minutes/day on Azure Gen2 XS | 12/60×30×1.25 | 7.5 credits/month; $22.50 at $3/credit |

The assumed work amounts are not measurements or equivalent freshness targets. They illustrate the formulas only. Stored results, ingestion, reader queries and other charges remain separate. Table 5 also lists Serverless Tasks Flex with a different rate; do not apply that rate to an ordinary serverless task without meeting that product's conditions.

## Billing behavior

**Standard warehouses:** one-minute minimum per resume, then per-second billing, including idle time before suspension. Rough compute model for an isolated standard warehouse:

```text
credits ≈ hourly_rate × sum(max(60, active_interval_seconds)) / 3600
```

This excludes query acceleration, separately billed services, adjustments and storage. High-frequency polling may prevent the warehouse from suspending. Refresh target lag is not billable runtime.

**Interactive warehouses:** one-hour minimum every start/resume; per-second thereafter. Minimum automatic-suspend interval is 24 hours. Extra clusters have their own minimum periods, and a configured standard fallback has its own compute bill. Frequent manual suspend/resume cycles can incur new minimum charges. See [Interactive billing](https://docs.snowflake.com/en/user-guide/interactive#cost-and-billing-considerations).

**Dynamic Tables:** warehouse refresh compute, cloud-services work where billable, storage and reading queries. See [Dynamic Table costs](https://docs.snowflake.com/en/user-guide/dynamic-tables/cost).

**Tasks:** either user-managed warehouse runtime or serverless task compute. Measure actual work, retries and minimum billing effects; do not multiply the schedule by a guessed query duration. See [task compute choices](https://docs.snowflake.com/en/user-guide/tasks-intro#recommendations-for-choosing-a-compute-model).

**Materialized views:** maintenance, storage and query compute; change rate and query shape matter. See [cost estimation](https://docs.snowflake.com/en/user-guide/views-materialized#estimating-and-controlling-costs).

**Online Feature Store:** continuously running service baseline, Online storage, optional extra capacity and offline writeback/processing. The trial could not provision it, so no measured live Online cost or latency is reported. See [Online billing](https://docs.snowflake.com/en/developer-guide/snowflake-ml/feature-store/online-feature-store#cost-and-billing-considerations).

**Private-preview Streaming Transformations:** no complete preview price was established from the public announcement. Do not assume the 0.0037-credit streaming-ingestion rate includes its cross-message processing.

## How to measure a real comparison

Use [the read-only metering templates](metering.sql) for warehouse usage, high-performance Streaming, Query Acceleration, daily database bytes and optional Online service meters. Substitute the explicit UTC interval and your exact resource names. They keep demo, serving and refresh components separate; the optional Online account totals cannot be attributed to a particular experiment automatically. The organization billing view can be unavailable and can lag up to 72 hours.

All six query sections compiled and executed in the trial with owned resource substitutions. Warehouse and Streaming views returned partial metering records. The remaining sections did not establish charges for the interval; this billing-query check does not establish live Online Feature Store behavior or a final application bill.

Use separate tagged resources and record ingestion bytes, successful output counts, warehouse activity, refresh/query history, service hours, extra capacity and retained storage. Include observers, setup and warm-up, or explicitly exclude and account for them. Query acceleration was enabled on the original trial warehouses and must be checked separately; the public Dynamic Table harness disables it for simpler attribution.

Account usage views can lag: streaming metering can take up to 12 hours and Online service usage up to three. An empty recent result means **not yet established**, not zero cost. Early warehouse snapshots are partial usage, not a complete application bill. See [results](results.md) for what was actually measured.
