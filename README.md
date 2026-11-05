# Genie ZeroOps playground: fund NAV pipeline

A small, realistic data pipeline for showing **Genie ZeroOps**, the Databricks background agent
that monitors jobs, pipelines and tables, investigates issues and proposes fixes.

The scenario is a fund administrator. Every business day an external market data vendor delivers
trades, end-of-day prices and FX fixings, and a valuation job publishes the NAV and AUM of five funds.
All data is synthetic and generated deterministically.

```
 market_data_feed (job)                    fund_nav_pipeline (job)
 ┌────────────────────────┐   raw_trades   ┌──────────────────┐   ┌─────────────────┐   ┌──────────────┐
 │ deliver_market_data    │──raw_prices───▶│ ingest_to_silver │──▶│ build_positions │──▶│ compute_nav  │
 │ (simulated vendor)     │  raw_fx_rates  └──────────────────┘   └─────────────────┘   └──────────────┘
 └────────────────────────┘   ref_*          silver_trades          silver_positions      gold_positions_valued
                                             silver_prices                                gold_fund_nav
                                             silver_fx_rates
```

| Layer | Tables |
|---|---|
| Vendor delivery (latest snapshot) | `raw_trades`, `raw_prices`, `raw_fx_rates` |
| Reference data | `ref_funds`, `ref_instruments`, `ref_opening_positions` |
| Silver (merged history) | `silver_trades`, `silver_prices`, `silver_fx_rates` (3 intraday fixings per day), `silver_positions` |
| Gold | `gold_positions_valued`, `gold_fund_nav` |

Both jobs run on serverless compute and are scheduled for business days (06:00 and 06:30
Europe/Berlin; paused by default). Data contracts in `src/nav_pipeline/checks.py` fail the run
instead of publishing wrong numbers.

## Layout

```
databricks.yml                  bundle definition, variables and targets
resources/                      UC schema and the two jobs
src/nav_pipeline/               business logic (transforms, checks, synthetic vendor feed)
src/*.py                        notebook entry points, one per job task
tests/                          unit tests (local PySpark)
```

## Deploy and run

Prerequisites: Databricks CLI, a Unity Catalog enabled workspace with serverless jobs.
Set `workspace.host` and the `catalog` variable in `databricks.yml` for your workspace.

```bash
databricks bundle validate
databricks bundle deploy

databricks bundle run market_data_feed
databricks bundle run fund_nav_pipeline

# A specific business date
databricks bundle run market_data_feed  --params business_date=2026-11-02
databricks bundle run fund_nav_pipeline --params business_date=2026-11-02
```

Run the feed before the NAV job for the same business date. Re-runs are idempotent.
To turn on the daily schedules, deploy with `--var schedule_pause_status=UNPAUSED`.

## Tests

Requires Java 17+ and [uv](https://docs.astral.sh/uv/).

```bash
uv run --group dev pytest
```
