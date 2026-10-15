# %% [markdown]
# # Lab 4.1 · Orchestrating Qafila with Dagster
#
# Day 4 · Session 1 (Orchestration) · 25 min. Until now you ran every step by hand, in the right order. An orchestrator
# does that for you: on a schedule, in dependency order, with retries, history and alerts. You'll use **Dagster**, which
# models pipelines as **assets** (the tables you want to exist) rather than tasks. Everything runs inside this notebook:
# no server needed.
#
# **Tasks**
# - [ ] 1 · Ingestion as software-defined assets (5')
# - [ ] 2 · The dbt project as assets: one lineage graph (6')
# - [ ] 3 · A daily-partitioned asset and a backfill (6')
# - [ ] 4 · A blocking asset check catches bad data (5')
# - [ ] 5 · Schedules and sensors (3')
# - [ ] ✱ Stretch · Retries against a flaky API

# %%
import os, sys, subprocess
from pathlib import Path

IN_COLAB = "google.colab" in sys.modules
if IN_COLAB:
    REPO_URL = "https://github.com/SDAIAAcademy/data-engineering.git"
    if not Path("/content/data-engineering").exists():
        subprocess.run(["git", "clone", "-q", REPO_URL, "/content/data-engineering"], check=True)
    os.chdir("/content/data-engineering/labs")
    subprocess.run([sys.executable, "-m", "pip", "install", "-q", "-r", "requirements-colab.txt"], check=True)
else:
    while not Path("de_utils.py").exists() and Path.cwd() != Path.cwd().parent:
        os.chdir("..")  # solutions/ notebooks run from labs/
sys.path.insert(0, os.getcwd())

import de_utils as U
U.ensure_data()
print("data:", U.RAW, "| quick mode:", U.QUICK)

# %%
import shutil
import warnings
from datetime import datetime

warnings.filterwarnings("ignore")             # Dagster flags some APIs as beta/preview
import dagster as dg
from dagster_dbt import DbtCliResource, dbt_assets

LAB = U.lab_dir("D4_1", reset=True)
DB = LAB / "qafila.duckdb"                    # a private copy of the warehouse for this lab
shutil.copy(U.build_warehouse(), DB)
U.run_dbt(["parse"], db_path=DB, quiet=True)  # writes target/manifest.json, which Dagster reads
EVENTS = (U.RAW / "events" / "delivery_events.jsonl").as_posix()
QUIET = {"loggers": {"console": {"config": {"log_level": "CRITICAL"}}}}   # keep notebook output readable

def sql(query, params=None):
    """Run SQL against the lab warehouse with a short-lived connection (dbt needs the file too)."""
    con = U.connect(DB)
    con.execute("SET python_enable_replacements = false")   # a missing table is an error, not a Python variable
    try:
        return con.execute(query, params or []).fetchall()
    finally:
        con.close()

# %% [markdown]
# ## 1 · Ingestion as software-defined assets (5')
#
# An **asset** is a function that produces a persistent object (a table, a file, a model) plus its dependencies.
# Dagster builds the graph from the function signatures and `deps`, and keeps a history of every materialization.
# The key `["raw", "delivery_events"]` matches dbt's source `raw.delivery_events`, so dbt models will hang off it.

# %%
@dg.asset(key=["raw", "delivery_events"], group_name="ingest", kinds={"duckdb"})
def raw_delivery_events():
    """Courier app events, loaded as is (JSON lines, three timestamp formats)."""
    sql(f"""CREATE OR REPLACE TABLE raw.delivery_events AS
            SELECT * FROM read_json('{EVENTS}', format = 'newline_delimited', columns = {{
              event_id: 'VARCHAR', order_id: 'BIGINT', store_id: 'VARCHAR', city: 'VARCHAR', courier_id: 'VARCHAR',
              event_type: 'VARCHAR', event_time: 'VARCHAR', lat: 'DOUBLE', lon: 'DOUBLE', ingest_time: 'VARCHAR'}})""")
    rows = sql("SELECT count(*) FROM raw.delivery_events")[0][0]
    return dg.MaterializeResult(metadata={"rows": rows})

# %% [markdown]
# ✏️ **Exercise** · Write `raw_reviews`, key `["raw", "reviews"]`, same group: load `U.RAW / "reviews.csv"` into
# `raw.reviews` with `read_csv(...)`, and return the row count as metadata.

# %%
# BEGIN SOLUTION
@dg.asset(key=["raw", "reviews"], group_name="ingest", kinds={"duckdb"})
def raw_reviews():
    """Customer reviews export (Arabic and English)."""
    src = (U.RAW / "reviews.csv").as_posix()
    sql(f"CREATE OR REPLACE TABLE raw.reviews AS SELECT * FROM read_csv('{src}')")
    return dg.MaterializeResult(metadata={"rows": sql("SELECT count(*) FROM raw.reviews")[0][0]})
# END SOLUTION

result = dg.materialize([raw_delivery_events, raw_reviews], run_config=QUIET)
for ev in result.get_asset_materialization_events():
    m = ev.event_specific_data.materialization
    print(m.asset_key.to_user_string(), {k: v.value for k, v in m.metadata.items()})

# %%
U.check(result.success, "both ingestion assets materialized")
U.check(sql("SELECT count(*) FROM raw.reviews")[0][0] == 3_000, "raw.reviews loaded")

# %% [markdown]
# ## 2 · The dbt project as assets: one lineage graph (6')
#
# `dagster-dbt` reads dbt's `manifest.json` and turns **every model, seed and snapshot into an asset** and every dbt test
# into an **asset check**. dbt sources become dependencies on assets with the same key, like the ones you just wrote.

# %%
dbt = DbtCliResource(project_dir=str(U.DBT_PROJECT), profiles_dir=str(U.DBT_PROJECT))

@dbt_assets(manifest=U.DBT_PROJECT / "target" / "manifest.json")
def qafila_dbt(context: dg.AssetExecutionContext, dbt: DbtCliResource):
    yield from dbt.cli(["build"], context=context).stream()

defs = dg.Definitions(assets=[raw_delivery_events, raw_reviews, qafila_dbt], resources={"dbt": dbt})
graph = defs.resolve_asset_graph()
print(len(qafila_dbt.keys), "dbt assets, e.g.", sorted(k.to_user_string() for k in qafila_dbt.keys)[:4])
print("upstream of fct_orders:", sorted(k.to_user_string() for k in graph.get(dg.AssetKey(["marts", "fct_orders"])).parent_keys))

# %% [markdown]
# ✏️ **Exercise** · New courier events just landed. Rebuild **only** `raw/delivery_events` and everything downstream of
# it: build `selection = dg.AssetSelection.keys(["raw", "delivery_events"]).downstream()` and pass it to
# `dg.materialize([...all three asset definitions...], selection=selection, resources={"dbt": dbt}, run_config=QUIET)`.
# Store the result in `run` and the set of materialized asset keys (as strings) in `rebuilt`.

# %%
# BEGIN SOLUTION
selection = dg.AssetSelection.keys(["raw", "delivery_events"]).downstream()
run = dg.materialize([raw_delivery_events, raw_reviews, qafila_dbt], selection=selection,
                     resources={"dbt": dbt}, run_config=QUIET)
rebuilt = {ev.event_specific_data.materialization.asset_key.to_user_string() for ev in run.get_asset_materialization_events()}
# END SOLUTION
print(sorted(rebuilt))
checks = run.get_asset_check_evaluations()
print(f"{len(checks)} dbt tests ran as asset checks; failed: {[c.check_name for c in checks if not c.passed]}")

# %%
U.check(run.success and "raw/delivery_events" in rebuilt and "marts/fct_orders" in rebuilt, "events and their downstream rebuilt")
U.check("marts/dim_products" not in rebuilt and "raw/reviews" not in rebuilt, "unrelated assets were left alone")

# %% [markdown]
# 🔍 That's the asset-based mindset: you ask for **data** to be fresh ("events and everything that uses them"), and the
# orchestrator works out which steps to run. In a task-based tool you'd maintain that list of tasks by hand.
#
# ## 3 · A daily-partitioned asset and a backfill (6')
#
# Event data arrives every day. A **partitioned** asset processes one day per run, so a bad day can be re-run alone and
# history can be **backfilled** in parallel. Each run must be **idempotent**: running a day twice gives the same table.

# %%
DAYS = dg.DailyPartitionsDefinition(start_date="2026-09-24", end_date="2026-10-01")
print(DAYS.get_partition_keys())

sql("CREATE SCHEMA IF NOT EXISTS ops")
sql("""CREATE TABLE IF NOT EXISTS ops.delivery_events_daily (
         day DATE, event_id VARCHAR, order_id BIGINT, store_id VARCHAR, event_type VARCHAR, ingest_time VARCHAR)""")

LOAD_SQL = f"""
    SELECT ingest_time[:10]::DATE AS day, event_id, order_id, store_id, event_type, ingest_time
    FROM raw.delivery_events
    WHERE ingest_time[:10] = $day
"""

# %% [markdown]
# ✏️ **Exercise** · Complete the asset body: for the run's `context.partition_key`, **delete** that day's rows from
# `ops.delivery_events_daily`, then **insert** `LOAD_SQL` for that day (pass `{"day": day}` as parameters), and return the
# row count of the day as metadata. Delete-then-insert per partition is what makes it idempotent.

# %%
@dg.asset(key=["ops", "delivery_events_daily"], partitions_def=DAYS, group_name="ops", deps=[raw_delivery_events])
def delivery_events_daily(context: dg.AssetExecutionContext):
    day = context.partition_key
    # BEGIN SOLUTION
    sql("DELETE FROM ops.delivery_events_daily WHERE day = $day", {"day": day})
    sql(f"INSERT INTO ops.delivery_events_daily {LOAD_SQL}", {"day": day})
    rows = sql("SELECT count(*) FROM ops.delivery_events_daily WHERE day = $day", {"day": day})[0][0]
    return dg.MaterializeResult(metadata={"rows": rows})
    # END SOLUTION

@dg.asset(key=["ops", "daily_delivery_kpis"], partitions_def=DAYS, group_name="ops", deps=[delivery_events_daily])
def daily_delivery_kpis(context: dg.AssetExecutionContext):
    """Per store and day: orders delivered (counted from events)."""
    day = context.partition_key
    sql("CREATE TABLE IF NOT EXISTS ops.daily_delivery_kpis (day DATE, store_id VARCHAR, delivered BIGINT)")
    sql("DELETE FROM ops.daily_delivery_kpis WHERE day = $day", {"day": day})
    sql("""INSERT INTO ops.daily_delivery_kpis
           SELECT day, store_id, count(*) FROM ops.delivery_events_daily
           WHERE day = $day AND event_type = 'delivered' GROUP BY ALL""", {"day": day})

def backfill(assets, keys, checks=()):
    """Run one partition at a time, like a backfill with concurrency 1. Returns {partition: success}."""
    instance = dg.DagsterInstance.ephemeral()
    return {k: dg.materialize(list(assets) + list(checks), partition_key=k, instance=instance, run_config=QUIET,
                              raise_on_error=False).success for k in keys}

print(backfill([delivery_events_daily, daily_delivery_kpis], DAYS.get_partition_keys()))
before = sql("SELECT count(*) FROM ops.delivery_events_daily")[0][0]
backfill([delivery_events_daily, daily_delivery_kpis], ["2026-09-27"])          # re-run one day
after = sql("SELECT count(*) FROM ops.delivery_events_daily")[0][0]
print(f"rows after backfill: {before:,} | after re-running Sep 27: {after:,}")

# %%
U.check(before == after == 22_518, "every event loaded once; re-running a day changed nothing")

# %% [markdown]
# ## 4 · A blocking asset check catches bad data (5')
#
# An **asset check** runs right after its asset. If it's **blocking** and fails, downstream assets in the same run are
# skipped, so bad data doesn't spread into KPIs.
#
# ✏️ **Exercise** · (a) Write `no_duplicate_events`, a blocking check on `delivery_events_daily`: for the run's partition,
# count rows minus distinct `event_id`s; pass only if it's 0, with the count as metadata. Run a backfill **with the check**
# and look at what happens to `daily_delivery_kpis`. (b) Fix the load: redefine `LOAD_SQL` so it keeps one row per
# `event_id` (DuckDB: `QUALIFY row_number() OVER (PARTITION BY event_id ORDER BY ingest_time) = 1`) and backfill again.

# %%
# BEGIN SOLUTION
@dg.asset_check(asset=delivery_events_daily, blocking=True, description="Phones retry: every event_id must appear once")
def no_duplicate_events(context: dg.AssetCheckExecutionContext):
    day = context.run.tags["dagster/partition"]
    dupes = sql("SELECT count(*) - count(DISTINCT event_id) FROM ops.delivery_events_daily WHERE day = $day", {"day": day})[0][0]
    return dg.AssetCheckResult(passed=dupes == 0, metadata={"duplicates": dupes})
# END SOLUTION

first = backfill([delivery_events_daily, daily_delivery_kpis], DAYS.get_partition_keys(), checks=[no_duplicate_events])
print("runs succeeded:", first)
print("duplicates per day:", dict(sql("SELECT day::VARCHAR, count(*) - count(DISTINCT event_id) FROM ops.delivery_events_daily GROUP BY 1 ORDER BY 1")))

# BEGIN SOLUTION
LOAD_SQL = f"""
    SELECT ingest_time[:10]::DATE AS day, event_id, order_id, store_id, event_type, ingest_time
    FROM raw.delivery_events
    WHERE ingest_time[:10] = $day
    QUALIFY row_number() OVER (PARTITION BY event_id ORDER BY ingest_time) = 1
"""
# END SOLUTION
second = backfill([delivery_events_daily, daily_delivery_kpis], DAYS.get_partition_keys(), checks=[no_duplicate_events])
print("after the fix:", second)

# %%
U.check(not all(first.values()), "the blocking check failed runs that loaded duplicates")
U.check(all(second.values()), "after the fix, every partition passes")
U.check(sql("SELECT count(*) - count(DISTINCT event_id) FROM ops.delivery_events_daily")[0][0] == 0, "no duplicates left")

# %% [markdown]
# 🔍 Your dbt tests from Day 2 already run as asset checks (Task 2). Asset checks are the place for the things dbt can't
# see: files before they're loaded, API responses, row counts against yesterday. Freshness and volume monitors (Lab 4.2)
# plug in here too.
#
# ## 5 · Schedules and sensors (3')
#
# **Schedules** start runs on a clock; **sensors** start runs when something happens (a file lands, an upstream asset
# updates). Both can be tested without waiting for real time.

# %%
daily_job = dg.define_asset_job("daily_ops", selection=[delivery_events_daily, daily_delivery_kpis], partitions_def=DAYS)
nightly = dg.build_schedule_from_partitioned_job(daily_job, hour_of_day=1, minute_of_hour=30)   # 01:30 UTC
tick = nightly.evaluate_tick(dg.build_schedule_context(scheduled_execution_time=datetime(2026, 9, 28, 1, 30)))
print("schedule at 2026-09-28 01:30 requests partition:", [r.partition_key for r in tick.run_requests])

# %% [markdown]
# ✏️ **Exercise** · Write a sensor `supplier_files` for `supplier_job` (given) that yields one
# `dg.RunRequest(run_key=<file name>)` per CSV in `U.RAW / "suppliers"`. The `run_key` makes it idempotent: Dagster
# never launches two runs with the same key, so a file is processed once even though the sensor sees it on every tick.

# %%
@dg.asset(group_name="ingest")
def supplier_prices_landed():
    """Placeholder for the supplier-file loader (Day 1's quarantine logic would live here)."""

supplier_job = dg.define_asset_job("supplier_job", selection=[supplier_prices_landed])

# BEGIN SOLUTION
@dg.sensor(job=supplier_job, minimum_interval_seconds=60)
def supplier_files(context: dg.SensorEvaluationContext):
    for f in sorted((U.RAW / "suppliers").glob("*.csv")):
        yield dg.RunRequest(run_key=f.name, tags={"file": f.name})
# END SOLUTION

requests = list(supplier_files(dg.build_sensor_context()))
print([r.run_key for r in requests])
U.check(len(requests) == 4 and len({r.run_key for r in requests}) == 4, "one run per supplier file, keyed by file name")

full = dg.Definitions(assets=[raw_delivery_events, raw_reviews, qafila_dbt, delivery_events_daily, daily_delivery_kpis,
                              supplier_prices_landed],
                      asset_checks=[no_duplicate_events], jobs=[daily_job, supplier_job], schedules=[nightly],
                      sensors=[supplier_files], resources={"dbt": dbt})
print(len(full.resolve_asset_graph().get_all_asset_keys()), "assets in the Qafila code location")

# %% [markdown]
# 🔍 In production this `Definitions` object lives in a Python package; `dagster dev` serves the UI (lineage graph, runs,
# backfills) and a daemon runs schedules and sensors. Try it on your laptop for the capstone: `labs/capstone/` has one.
#
# ## ✱ Stretch · Retries against a flaky API
#
# The payments API rate-limits (HTTP 429). Give an asset a `dg.RetryPolicy(max_retries=3, delay=1)` and watch Dagster
# retry it. The API below answers every 2nd request with 429 and is primed so the **first** call fails; fetch one page
# with `urllib.request.urlopen(f"{api.base_url}/payments?updated_since=2026-09-01T00:00:00Z")`.

# %%
# BEGIN SOLUTION
import json
import urllib.request

api = U.PaymentsAPI(page_size=500, rate_limit_every=2)
api.start()
api.requests = 1          # primed: the next request is the 2nd, so it gets a 429
attempts = []

@dg.asset(retry_policy=dg.RetryPolicy(max_retries=3, delay=1))
def payments_page():
    attempts.append(datetime.now().strftime("%H:%M:%S"))
    with urllib.request.urlopen(f"{api.base_url}/payments?updated_since=2026-09-01T00:00:00Z") as r:   # 429 raises HTTPError
        page = json.load(r)
    return dg.MaterializeResult(metadata={"rows": len(page["data"])})

r = dg.materialize([payments_page], run_config=QUIET, raise_on_error=False)
api.stop()
print("success:", r.success, "| attempts:", attempts)
# END SOLUTION

# %% [markdown]
# ## Checkpoint
#
# 1. Task-based (Airflow 2 style) vs asset-based (Dagster, Airflow 3 assets): what does the orchestrator know in each case?
# 2. Why must a partitioned asset be idempotent? What would happen in a backfill otherwise?
# 3. When should a check be **blocking**, and when should it only warn?
# 4. Schedule or sensor: how would you trigger the supplier price load, and why?
