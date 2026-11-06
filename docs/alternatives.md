# Where calculations and stored results belong

## Pipe capability scope

The tested high-performance Snowpipe Streaming path accepted per-message casts, timestamp derivation, numeric expressions and conditional logic. Aggregate/group/window probes were rejected by that account. The experiment is evidence about those definitions in that environment, not proof that every Snowflake pipe forever excludes cross-message calculations. The [AVG, standard-deviation and window probes](pipe-probes.md) include the SQL for rechecking that boundary.

Snowflake's [real-time analytics overview](https://www.snowflake.com/en/blog/real-time-analytics-snowflake/) describes **Streaming Transformations in private preview**, including aggregation, filtering, reference-data enrichment and nested-array expansion before rows land. Its performance numbers are Snowflake's internal measurements and targets, not results from this repository. The announcement does not establish exact STDDEV_POP/STDDEV_SAMP support, window inclusivity, late-data correction limits, retention rules or a complete preview price.

A preview evaluation should request documented syntax and enablement, then run the same independent tests used here: known values, NULLs, singleton groups, exact boundary timestamps, out-of-order arrivals, deliberate measurement replay, idle expiry, large-offset numeric data, observed time until the correct result, and total ingestion/compute/storage charges. Do not invent preview SQL from the marketing description.

The public [COPY transformation guide](https://docs.snowflake.com/en/user-guide/data-load-transform) and [streaming-pipe guide](https://docs.snowflake.com/en/user-guide/snowpipe-streaming/snowpipe-streaming-pipe-object) describe the standard supported path. A CREATE PIPE succeeding for file-based Snowpipe does not, by itself, validate that a later file load with that query will succeed.

## Alternatives and what to validate

| Path | Stored data | Validation that matters |
|---|---|---|
| Query or ordinary view | Raw input; no separately maintained aggregate | Query latency under actual request rate and scanned history |
| Dynamic Tables | Raw, deduplicated result and aggregates as defined | Resolved refresh mode, exact result correctness, observed freshness and refresh compute |
| Streams/tasks | Whatever ordinary result tables the task writes | Stream consumption, idempotent writes, affected-window repairs, failed-run retries and retained-history policy |
| Materialized view | Raw physical table plus maintained eligible aggregate | Enterprise availability, allowed single-table query shape, maintenance cost and numerical precision |
| Preview streaming transformations | Depends on preview's output/retention model | Account enablement, function/window semantics and full pricing |
| Producer summaries | Counts/means/mergeable state, optionally raw elsewhere | Replay identity for summary messages, late-event corrections and loss of original detail |
| Interactive warehouse | Can query existing standard/Dynamic/Iceberg tables | Warm/cold query latency, concurrency, fallback rate and billed running hours |
| Online Feature Store | Online feature state plus configured offline data | Per-feature response statuses, update/expiry semantics, API limits, full serving cost and account eligibility |

## Raw history is a separate decision

An ordinary Dynamic Table tracks source changes. Deleting all readings in an old minute can delete that minute's derived row. Querying a summary successfully before deleting raw data is not an archive test.

For a retained history, decide:

1. When is a minute considered closed?
2. How late can a reading arrive before it requires a correction job?
3. Where is the raw data needed to rebuild that minute kept?
4. Does a correction replace a complete summary, or merge a verified nonoverlapping contribution?
5. How do readers distinguish complete windows from provisional ones?

A normal retained table populated by a task can preserve closed results after source expiry, provided the task explicitly handles these rules. Dynamic Table frozen-history patterns are another option, with their own supported conditions and repair implications. See [Snowflake's design patterns](https://docs.snowflake.com/en/user-guide/dynamic-tables/design-patterns). The original retention experiment verified that ordinary summaries disappeared after source deletion while its explicitly frozen result remained; that is not a general guarantee for every frozen-history design.

The [merge example](../examples/merge_summaries.py) combines nonoverlapping groups from count, mean and centered second moment. It cannot detect overlapping groups or repair a replayed summary on its own. An hourly mean must weight minute means by valid counts. An hourly standard deviation cannot be obtained by averaging minute standard deviations.

## Interactive warehouses can serve existing results

[Zero-copy Interactive Analytics](https://docs.snowflake.com/en/user-guide/interactive) lets an Interactive warehouse query existing standard, Dynamic and Iceberg tables. Using it to read the minute Dynamic Table does not remove that table's refresh compute. A configured standard fallback may execute queries that exceed the Interactive timeout; count that usage separately. The [live capability check](../experiments/interactive/README.md) verified direct serving of ten expected summary rows. This repository does not report a concurrency benchmark for Interactive Analytics.

This is the zero-copy serving path used in the article's comparison. The wider Interactive Analytics product also has dedicated Interactive Table forms, including automatically refreshed definitions and Interactive materialized views, with their own maintenance compute and restrictions. They are documented alternatives, not live-tested paths in this repository. The existing Dynamic Table plus an Interactive warehouse avoids requiring such a conversion for this example.

## Online Feature Store is a separate ingestion and lookup path

The [Online Feature Store](https://docs.snowflake.com/en/developer-guide/snowflake-ml/feature-store/online-feature-store) has direct-ingestion and per-entity lookup APIs. The prepared experiment uses a sensor as the entity key. It is not a PIPE option or an alias for Interactive Analytics. See the [Online experiment](../experiments/online/README.md) for its API-aware assertions and the actual trial provisioning blocker.
