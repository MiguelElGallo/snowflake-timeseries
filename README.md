# Sensor time series in Snowflake

A complete sensor example and reproducible experiments for Snowpipe Streaming, Dynamic Tables and the Online Feature Store.

Start with the [article](ARTICLE.md), then run the [quickstart](quickstart/README.md). It sends real streaming messages, removes duplicate measurements and checks a minute's average and standard deviation.

```text
Sensor messages → streaming pipe → raw readings → one copy per measurement → minute summaries
```

## Choose a starting point

| Directory | Purpose |
|---|---|
| [quickstart](quickstart/README.md) | Small, complete streaming-to-summary example, including setup and scoped cleanup |
| [experiments/dynamic-tables](experiments/dynamic-tables/README.md) | Correctness, late readings, replays, numeric precision, idle expiry and sustained-load experiments |
| [experiments/online](experiments/online/README.md) | Prepared Online Feature Store experiment, with local contract tests; live service testing was blocked by the trial account |
| [experiments/interactive](experiments/interactive/README.md) | Live-verified capability check: query existing Dynamic Table summaries through an Interactive warehouse |
| [docs](docs/README.md) | Cost assumptions, alternatives, methodology and results |
| [assets](assets/README.md) | Genuine Cortex Code Desktop screenshots and explanatory charts |

The current standard streaming-pipe path and the **private-preview Streaming Transformations** described by Snowflake are different capability scopes. Rejection of an aggregate pipe definition in one account does not prove that Snowflake can never aggregate during ingestion. See the article and [Snowflake's overview](https://www.snowflake.com/en/blog/real-time-analytics-snowflake/).

This repository contains synthetic sensor data. Credentials stay in your existing local Snowflake connection profile. Do not commit generated `.state`, evidence, tokens or private keys.

The examples create billable resources. The quickstart uses an X-Small standard warehouse; the larger experiments have separate cost and cleanup instructions. This is educational code, not a production ingestion service.
