# Public harness validation

The published Python harness was executed end to end against fresh, randomly named trial resources. It used the documented modest defaults: **10 sensors for 120 seconds**, followed by a burst containing 30 seconds of events. It did not reuse the separate sustained experiment's database or warehouse.

All documented stages completed without an implementation change or rerun: resource setup, SQL definitions, ordinary correctness, read-only precision, materialized precision and idle expiration, paced input, burst/recovery, metering capture, suspension and cleanup. Compilation, the independent local oracle and four offline ownership checks had also passed before the live run.

## Results

| Check | Observed result |
|---|---|
| Requested SQL definitions | All accepted; no unsupported arm in this account. Dynamic tables explicitly requested incremental refresh. |
| Ordinary correctness | Passed initial data, genuine cross-bucket lateness, replay after restarting the client, equal-valued distinct events, exact boundaries, NULLs, singleton values and the empty as-of window. |
| Large-offset FLOAT diagnostic | Reproduced the precision finding: direct population/sample SD returned 0 for five distinct values near one trillion. The source values remained distinct. This is recorded as a numerical finding, not hidden behind an overall pass. |
| Precision alternatives | Ordinary SQL and the separate incremental dynamic table passed fixed-point and centered calculations. Expected population SD was approximately 1.4142135624; sample SD approximately 1.5811388301. |
| Current-window expiration | The explicit UTC variant first contained the event, then expired automatically with no new input or manual refresh during observation. Expiration was observed about 87.56 seconds after the event, including the 60-second window plus refresh/observation delay. |
| Paired timezone control | The implicit timestamp-conversion control initially returned no row, while the explicit UTC variant returned the expected row. This is an observed account result, not a claim that the implicit form fails everywhere. |
| Paced input | All 1,200 expected events arrived; every expected sensor/minute group matched. All 240 sampled panel events were observed in correct summary revisions. |
| Burst/recovery | All 300 expected events arrived; every final group matched. All 60 sampled panel events were observed in correct summary revisions. |
| Final metrics | Counts and key coverage were exact; the checked means, standard deviations, minima and maxima had zero measured error against the local oracle in the two load runs. No sampled events remained unobserved. |
| Cleanup | Database and warehouse absence verified; both ownership entries cleared. |

The overall execution completed successfully **with the explicitly reported FLOAT numerical finding**. The small workload validates the public example; it is not a capacity benchmark or a production latency guarantee. The two-minute default has no post-warm-up interval under the harness's first-120-seconds definition. Its latency sample includes only sensors 0 and 1; final correctness checks cover all ten sensors.

## Evidence handling and scope

The private, ignored artifact directory retains exact SQL, query IDs, stage logs, timing records, expected/actual diagnostics and cleanup evidence. No account identifier, raw log or private resource name is embedded in this public summary. The run used the pinned connector and Streaming SDK versions from `requirements.txt`.

This validation confirms the portable code ran successfully in one trial environment. It does not establish availability in every account/region, a general FLOAT failure for all inputs, a promise that target lag equals a fixed refresh interval, or equivalent cost/performance across serving architectures.
