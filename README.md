# Qafila Lakehouse — Capstone Project

**Student:** [اسمك الكامل] (solo project)
**Program:** Modern Data Engineering — SDAIA Academy
**Session dates:** [تاريخ بداية الدورة] – [تاريخ نهايتها]
**Trainer:** [اسم المدرب]
**Course repository:** https://github.com/SDAIAAcademy/data-engineering

> My capstone work is in `labs/capstone/pipeline.py`. The rest of the repository is the
> course material it builds on (data generator, dbt project, utilities, contract).

## Problem
Qafila's ops and finance teams need trusted daily revenue per city and store
and daily delivery KPIs, refreshed nightly, without querying the production app database.

## Sources (4 types)
| Source | Type | Load strategy |
|---|---|---|
| App DB: orders, order_items | database | incremental on `updated_at`, 3-day look-back, delete+insert on key; watermark committed in the same transaction |
| App DB: customers, stores, products | database | full replace (small tables) |
| Payments | API (JSON lines) | incremental on `updated_at`, merge on `payment_id` |
| Delivery events | events | raw landed as-is, deduplicated by `event_id` downstream |
| Supplier prices, reviews | files | full replace |

## Architecture
raw (DuckDB) → dbt staging → dbt marts → gold on Apache Iceberg.
Orchestrated by Dagster: one code location, nightly schedule at 01:30 Riyadh time.

## How to run

### On Google Colab (how I ran it)
    !git clone https://github.com/[اسم حسابك]/data-engineering
    %cd data-engineering/labs
    !pip install -q uv
    !uv sync
    !uv run python prepare_data.py
    !uv run python capstone/pipeline.py            # end to end, one command
    !uv run python capstone/pipeline.py backfill   # daily ops partitions

### Locally (from `labs/`)
    uv sync
    uv run python prepare_data.py
    export QAFILA_PII_KEY=<secret>
    uv run python capstone/pipeline.py
    uv run python capstone/pipeline.py backfill
    uv run dagster dev -f capstone/pipeline.py     # Dagster UI

## Data model (grain)
- `fct_order_items`: one row per order line.
- `fct_orders`: one row per order (the delivery fee lives here, so there is no fan-out).
- `mart_daily_city_sales`: one row per day per city, published to `gold.daily_city_sales` (Iceberg).
- `ops.delivery_kpis_daily`: one row per Riyadh day per store (daily partitions).

## Idempotency
Running the pipeline twice gives identical row counts; the second run reloads only
the 3-day look-back window. Backfilling a day twice deletes, then re-inserts, that day.

## Quality & observability
- dbt tests on every model (each one becomes a Dagster asset check).
- Payments ODCS contract (`labs/capstone/contracts/payments.odcs.yaml`):
  - structure rules are **blocking**: dbt does not run on a broken feed;
  - value rules **warn** and route bad rows to `quarantine.payments`.
- **47 checks** run on every build (43 dbt data tests, 3 dbt unit tests, payments contract, volume monitor, PII scan).
- **Real issues caught** in the source data:
  - 16 payments with amount ≤ 0 (contract value rule; rows routed to `quarantine.payments`);
  - 142 order lines with non-positive quantity (quarantined in `quarantine.qrn_order_items`, excluded from facts);
  - 4 supplier costs ≤ 0;
  - 258 orders whose totals don't match their payments (warn: routed to the payments owner).
- **Incremental proof:** first run loaded 45,766 orders and 41,785 payments; the re-run reloaded only
  the 3-day look-back (2,351 orders, 2,151 payments) with identical row counts and check results.
- **Reconciliation:** raw.order_items (172,545) − fct_order_items (172,403) = 142,
  exactly the quarantined lines: every row is either in the fact table or quarantined with a reason.
- Volume monitor: each day compared with the same weekday's 4-week median, so weekends don't raise false alarms.
- **Backfill proof:** 7 daily partitions (Sep 24–30) → 84 rows (7 days × 12 stores);
  re-running the backfill gives the same 84 rows (delete-then-insert per day).


## Governance (PDPL / NDMO)
- Column classification by NDMO level: `governed.pii_classification`.
- National ID stays in raw; downstream uses an HMAC-SHA256 `person_token` (key from an environment variable, not code).
- Analysts read `governed.customers`: masked mobile, email domain only, no names.
- Check `no_national_ids_in_marts` scans every marts column with a Luhn validator.
- Right to erasure: delete in the source, re-run (customers are fully replaced), expire Iceberg
  snapshots and remove orphan files, then verify with the scan check.
- DuckDB has no role grants; in production this becomes column masks and row policies in the platform or catalog.

## Limitations & next steps
- No streaming path yet: the next step is the micro-batch Iceberg sink from Lab 3.4 for a live delivery-SLA view.
- The partition range is fixed to the sample week; in production, drop `end_date`.
- At 100× volume: Iceberg for all raw tables, partition by `order_date`, and Spark only if a single job reads terabytes.
