# Capstone starter kit

Teams of 2–3 build a working data platform and present it on Day 5 (10 min talk + live demo, 5 min technical Q&A).

## Choose an option

| Option | Builds on | Data ideas |
|---|---|---|
| **1 · Qafila Lakehouse**: batch + streaming platform for Qafila, from raw sources to governed marts and a near-real-time operations view | Everything; `pipeline.py` is a working start | The course data (`labs/data/raw`), plus the problems listed in `DATA_ISSUES.txt` |
| **2 · Open Data Platform**: ingest Saudi open datasets into a lakehouse with tests, a semantic layer and a small dashboard | Labs 1.3, 2.3, 2.4, 3.1, 4.2, 4.4 | The national open data portal, GASTAT releases, or other open data you're allowed to use; at least one source must refresh (API or periodic file) |
| **3 · Real-time Operations Monitor**: streaming pipeline with event-time KPIs, SLA alerts, an Iceberg history table and replay | Labs 3.3, 3.4, 4.1, 4.2 | Qafila courier events (`labs/data/raw/events`), replayed as a stream; add a second source (orders or payments) |

Only use data you're allowed to use. Personal data stays out of git, out of slides and out of external LLM APIs (Lab 4.3).

## Minimum requirements

1. An end-to-end pipeline from **at least two source types** (database, API, files, events) to analytics-ready tables, runnable with **one command**.
2. A layered model (raw/bronze → staging/silver → marts/gold) with the **grain** of every fact table written down.
3. **Incremental** loading that's safe to re-run (idempotent), demonstrated live.
4. At least **8 data tests or contract checks**, one of which catches a **real** problem in the data.
5. **Orchestration** (Dagster, Airflow or Prefect) or a justified alternative, plus a short **governance note**: the classification of every column that holds personal data and how it's protected.

## Rubric

| Criterion | Weight |
|---|---|
| Functional end-to-end pipeline | 30% |
| Architecture & data modeling decisions | 25% |
| Data quality, testing & observability | 20% |
| Orchestration, governance & operability | 15% |
| Presentation & reproducibility | 10% |

## What's in this folder

| File | Purpose |
|---|---|
| `pipeline.py` | Working baseline for option 1, as one Dagster code location: 9 ingestion assets (SQLite app DB, payments API extract, courier events, supplier CSVs, reviews) → the `dbt_qafila` project (every dbt test becomes an asset check) → a gold table published to Iceberg, plus a contract check, a volume monitor and a nightly schedule. `uv run python capstone/pipeline.py` runs it once (≈1–2 min); `uv run dagster dev -f capstone/pipeline.py` opens the UI. |
| `contracts/payments.odcs.yaml` | The ODCS contract from Lab 4.2; `pipeline.py` runs its quality rules. |
| `TEMPLATE_README.md` | README skeleton for your team repository |

## Known weaknesses of the baseline (good places to start)

- **Everything is a full refresh.** Ingestion replaces whole tables; only `fct_order_items` is incremental. Make the event and payment loads incremental (Labs 1.3, 1.4, 3.4) and show a re-run changes nothing.
- **No streaming path.** Courier events are loaded as one file. Replay them as micro-batches into Iceberg with exactly-once commits (Lab 3.4) and compute the SLA from the table.
- **Contract breaches only warn.** The payments contract fails (16 non-positive amounts) but nothing is quarantined or routed to an owner. Decide what should block, and where bad rows go.
- **Personal data flows into raw tables unprotected.** `raw.customers` holds national IDs, IBANs and mobiles. Classify, pseudonymize and mask (Lab 4.3) before anything reaches marts.
- **No partitions.** Add a daily-partitioned asset and demonstrate a backfill (Lab 4.1).

## Suggested repository layout

```
your-project/
├── README.md              # from TEMPLATE_README.md
├── pyproject.toml         # pinned dependencies (uv lock)
├── pipeline.py            # or definitions.py: the Dagster code location
├── ingest/                # dlt sources, loaders
├── dbt_project/           # models, tests, contracts
├── contracts/             # ODCS YAML
├── data/                  # or a script that downloads it; never commit personal data
└── docs/                  # architecture diagram, governance note
```

## Timeline

| When | Do |
|---|---|
| End of Day 4 (10') | Confirm option, sources and scope with the instructor |
| Block 1 (1–2 h) | Sources landing in raw, one command end to end (start from `pipeline.py`) |
| Block 2 (1–2 h) | Models with documented grain, incremental loads, tests and contract checks |
| Block 3 (1 h) | Orchestration, governance note, README, 6–8 slides, one timed rehearsal, a recorded backup of the demo |

Submit the repository link before Day 5, Session 1.
