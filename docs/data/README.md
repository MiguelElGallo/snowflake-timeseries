# Curated sustained-workload measurements

`sustained-results.json` contains the three completed paced runs and the separate burst result. `summary-visibility.csv` contains all 21,600 observed panel-event delays from the paced runs: four synthetic sensors, 1,800 event sequences per sensor, three runs. No successful observations were selected from an incomplete run.

The CSV columns are run number, synthetic sensor number, sequence number and measured seconds from producer send to the first **observed correct containing minute summary**. Sequence numbers 1–120 are warm-up; exclude those to reproduce the post-warm-up comparisons. Each run then has 6,720 observations. Quantiles use linear interpolation at `(n-1) × p` on sorted observations, equivalent to Python's `statistics.quantiles(..., method='inclusive')`.

The five-second polling interval and query execution time add observation delay. The four-sensor panel measures freshness; the separate final checks covered every sensor and minute bucket. The CSV alone does not re-prove the returned SQL payloads. The [experiment harness](../../experiments/dynamic-tables/README.md) supplies the complete generator, independent statistics reference, payload assertions and observation method for a new run.

This export deliberately contains no account identifiers, credentials, warehouse names, query IDs or absolute client clock values. It retains every panel delay, including warm-up, and the outcome summaries. See [results and limits](../results.md) before comparing these measurements to another system.

To regenerate the chart from the published measurements:

```bash
python -m pip install matplotlib==3.11.2
python examples/draw_freshness.py
```

Run the commands from the repository root in a Python 3.12 virtual environment. The script recalculates the plotted median and 95th percentile from the CSV and checks them against the JSON before writing the PNG and SVG. Matplotlib is needed only for the figures, not for ingestion or the independent statistical reference.
