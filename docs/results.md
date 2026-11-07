# Results and limits

These are synthetic-data experiments, not production service-level guarantees. The article keeps the explanation short; this appendix distinguishes live observations, local-only tests, documented features and blocked paths.

## Live Dynamic Table correctness

The independent reference calculation uses Decimal and a centered two-pass standard deviation. Assertions compare the complete set of grouping keys, counts, NULL patterns and finite numeric values. Ordinary-value absolute tolerance is 1e-8; the large-offset numeric diagnostic uses 1e-4. The diagnostic's failure is preserved rather than hidden by a looser tolerance.

| Case | Observed result |
|---|---|
| Fixed minute count/mean/population SD/sample SD/min/max | Passed for ordinary-value fixtures |
| Different event IDs with equal values | Both measurements retained |
| Same measurement replayed after client restart | Summary values unchanged |
| Event arriving 165 seconds behind the latest event | Earlier event-time bucket corrected |
| Invalid value, explicit NULL, all-NULL group | Counts and NULL statistics matched reference |
| Single valid value | Population SD zero; sample SD NULL |
| Exact/microsecond time boundaries | Fixed, explicit-as-of and historical RANGE results matched their declared boundaries |
| Current-window idle expiry with explicit UTC | Passed; observed empty about 103.05s after event time, without new data or manual refresh during observation |
| Implicit session-clock-to-NTZ control | Fresh UTC reading absent; retained as unsuccessful control |
| Large-offset native FLOAT aggregate | Failed: population/sample SD zero instead of expected nonzero values |
| Fixed-point and centered aggregate variants | Passed in both ordinary SQL and an incremental Dynamic Table |

The current-window test uses `(now-60s, now]`. The historical RANGE test includes both boundaries `[event_time-60s, event_time]`. Equal timestamps are peers; ROWS frames are not used as a substitute for elapsed time.

The paired timezone control used the same event and ready input, changing only the clock conversion. This supports the explicit-UTC correction in that account; it is not a universal statement about every scheduled session's timezone.

The five large-offset values were `1e12 + [-2,-1,0,1,2]`, all distinct and exactly representable as the tested FLOAT inputs. Expected population SD was `sqrt(2)` and sample SD `sqrt(2.5)`. The failed aggregate result was zero for both. Casting the preserved values to `NUMBER(38,6)` or subtracting the fixed 1e12 reference recovered the expected deviations. Choose precision/scale deliberately; a later cast cannot restore source information already lost.

## Sustained workload

The complete protocol passed: three paced 30-minute runs of 1,000 sensors at one reading per second, with five-minute idle intervals and a separate accelerated 120-second-event burst. Each paced run submitted and validated 1,800,000 events and all 31,000 resulting sensor/minute groups. There are 31 clock-minute buckets because the runs did not begin exactly on a minute boundary.

| Run | Events | Final groups | Post-warm-up observations | Median delay | 95th percentile | 99th percentile | Maximum delay |
|---|---:|---:|---:|---:|---:|---:|---:|
| 1 | 1,800,000 | 31,000 | 6,720 | 37.182 s | 59.100 s | 62.508 s | 67.398 s |
| 2 | 1,800,000 | 31,000 | 6,720 | 37.288 s | 59.236 s | 62.689 s | 66.272 s |
| 3 | 1,800,000 | 31,000 | 6,720 | 36.124 s | 58.103 s | 61.309 s | 65.170 s |

All 7,200 expected panel observations per paced run were eventually observed, including warm-up; no event was censored. Final group keys and counts matched exactly. The largest absolute error in the checked load statistics was approximately 3.55e-15. Raw and deduplicated event counts, distinct IDs and generated-value domains matched for all 1,000 sensors. The separately reported large-offset FLOAT diagnostic still failed; load correctness does not erase that numerical finding.

The burst submitted 120,000 events representing 120 event-time seconds, checked all 3,000 final groups and observed all 480 panel events. Its total run time including final draining/validation was about 43.38 seconds. It deliberately advances event time faster than wall time, so it is a fixed-minute aggregation/recovery check, not a 1 Hz workload or a current-window performance result.

The [curated JSON and all 21,600 paced observation records](data/README.md) preserve both warm-up and post-warm-up measurements without account metadata. Quantiles use linear interpolation at `(n-1) × p`. The [chart generator](../examples/draw_freshness.py) recomputes the plotted median and 95th percentile from the CSV before drawing the chart.

Freshness means time from producer send to an observed summary containing the correct payload. Four fixed panel sensors are polled during load; every sensor and bucket is checked at the end. The five-second polling interval and query time are included in observed delay. Acknowledgement timing is the observed FIFO future-reap upper bound, not an exact network acknowledgment completion timestamp. The first 120 seconds are reported separately as warm-up. Sustained testing covers fixed-minute summaries, not a rolling-window scale benchmark.

The producer and observer ran on the same Mac outside Snowflake. Refreshes and observation queries shared an Azure Gen2 X-Small standard warehouse. The measurements therefore include client/network delay and observation overhead; they do not isolate server-side refresh execution. Query acceleration was enabled on that original warehouse and is a separate billing dimension. The public portable harness disables it for simpler attribution.

## Online Feature Store

XS provisioning failed with Snowflake error 604056: the `HIGHMEM_XL` compute family was not allowed for the trial account. The region was otherwise documented as supported. A prior statement timeout was corrected at session scope before this definitive provisioning result. No running service or usable live endpoint was obtained.

The failed service was removed and its absence verified. **Live Online ingest/query correctness, replay, late-data behavior, expiry, sustained throughput and latency were not run.** No online benchmark or live online cost comparison is claimed.

The prepared public harness passed 67 local checks: 18 REST response-contract cases, 27 scheduler/replay controls, 12 connection-helper cases and 10 ownership cases. These test client behavior with controlled responses; they cannot establish the server's actual semantics. See [the Online README](../experiments/online/README.md).

The load design checks complete 1,000-key payloads at segment boundaries and separately performs controlled latency probes. It is not a continuous query-under-load benchmark.

## Public quickstart

The public recipe's table/pipe/dedup/minute definitions ran live. `TRY_TO_DECIMAL` in the pipe was accepted and processed malformed numeric input into NULL. The streaming seed passed the exact expected count/average/deviation check. The optional explicit-UTC moving-window definition was accepted. Independent local review also checked ownership safeguards and 200 large-offset split/merge cases for the standalone summary-merging example.

## Other capability scopes

- Private-preview Streaming Transformations: documented by Snowflake; not enabled or benchmarked here, and no exact SD/window syntax invented.
- Interactive warehouse: a separate live capability check returned all ten expected summary rows in three queries, with the executing warehouse identity verified and result reuse disabled. The warehouse was X-Small, one cluster, with no fallback or attached table copies. The source Dynamic Table was unchanged. Its temporary warehouse was dropped and absence verified. This is not a latency or concurrency benchmark; the brief run still has a one-hour minimum billing expectation. See the [exact public reproducer](../experiments/interactive/README.md).
- Original file-based aggregate PIPE definitions: CREATE acceptance alone was not followed by a file load, so it does not establish working aggregate ingestion.
- Original streams/tasks check: a small semantic comparison passed; it was not included in the sustained Dynamic Table workload.
- Original accelerated ingestion run: 3.6 million events was a bulk-load observation, not 1 Hz sensor simulation. The sustained experiment above is the relevant paced workload.

## Reproduction and provenance

The [public Dynamic Table experiment](../experiments/dynamic-tables/README.md) completed a fresh end-to-end live run using its modest defaults: ten sensors for 120 seconds and a 30-second-event burst. All documented stages and verified cleanup completed without implementation fixes or reruns; the FLOAT precision finding remained explicitly reported. See its [validation record](../experiments/dynamic-tables/VALIDATION.md). This portable-code validation is separate from the full sustained protocol above. The [public Interactive smoke reproducer](../experiments/interactive/README.md) was also the code executed for the live serving-path check.

The [Online experiment](../experiments/online/README.md) includes the prepared protocol, dependencies and ownership-checked cleanup, and passed its documented local checks. Its live ingest/query stages and full end-to-end portable run remain unexecuted because service provisioning was blocked. Successful Dynamic Table and Interactive runs do not establish Online service behavior.

Raw local evidence contains account and object metadata and is not published. Public evidence is curated to retain the workload, numerical results, failure conditions and limitations without credentials or unrelated account information. Genuine screenshots show synthetic SQL results in Cortex Code Desktop.

All eight Dynamic Tables in the main experiment were suspended before its owned database and both warehouses were removed. Absence was verified and ownership records were cleared. The quickstarts, portable validation, Interactive warehouse and failed Online service were also cleaned up; the irregular-reading session removed its temporary tables.

The [credit usage report](costs.md) uses a later reconciliation after the documented reporting delays. Seven tracked experiment warehouses consumed **3.573472200 compute credits**; their streaming pipes recorded **0.002407909 credits**, for a scoped subtotal of **3.575880109 credits**. The main Dynamic Table lab accounts for **2.406439800 compute credits** and **0.001496030 streaming credits**. These include setup, checks, polling and idle time, not only the sustained workload. Cloud-services adjustments, storage and the later billing observer are separate. The report links the curated measured records and [read-only metering queries](metering.sql); it makes no monthly or currency projections.
