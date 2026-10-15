# %% [markdown]
# # Lab 1.3 · Ingest Qafila with dlt
#
# Day 1 · Session 3 (Batch ingestion) · 25 min. What you'll build:
# - a hand-written API client that pages through results and backs off on rate limits;
# - a **dlt** pipeline that loads payments incrementally from the API into DuckDB, safe to re-run;
# - full and incremental loads of app-database tables, and a look at schema evolution and the load log.
#
# The payments API is a local mock (`U.PaymentsAPI`) that behaves like a real provider: pages of 500 records, a cursor
# parameter `updated_since`, and an HTTP **429 Too Many Requests** every few calls. Its `clock` lets us pretend time passes.
#
# **Tasks**
# - [ ] 1 · Call the API by hand: pages, a 429, backoff (5')
# - [ ] 2 · dlt resource with an incremental cursor + merge (7')
# - [ ] 3 · Run twice: prove the load is idempotent (3')
# - [ ] 4 · Load app tables (full for stores, incremental for orders) (5')
# - [ ] 5 · Spot the new `wallet_provider` column; read the load log (5')
# - [ ] ✱ Stretch · Load the supplier CSVs; what happens in week 39?

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
import random
import time

os.environ["RUNTIME__LOG_LEVEL"] = "ERROR"          # keep dlt's log quiet in the notebook
import dlt
import pandas as pd
import requests
from dlt.sources.helpers.rest_client import RESTClient
from dlt.sources.helpers.rest_client.paginators import JSONLinkPaginator

OUT = U.lab_dir("D1_3", reset=True)
LAKE_DB = OUT / "qafila_lake.duckdb"                 # our destination: a DuckDB file with a `raw` schema

api = U.PaymentsAPI(page_size=500, rate_limit_every=5, clock="2026-09-15T00:00:00.000Z")   # "today" is 15 September
BASE = api.start()
print("payments API running at", BASE)

# %% [markdown]
# ## 1 · Call the API by hand: pages, a 429, backoff (5')
#
# One request first. Note the response shape: `data`, `page`, `next_page`, `next`.

# %%
r = requests.get(f"{BASE}/payments", params={"updated_since": "2026-09-14T00:00:00.000Z", "page": 1}, timeout=10)
body = r.json()
print(r.status_code, {k: v for k, v in body.items() if k != "data"})
pd.DataFrame(body["data"][:3])

# %% [markdown]
# ✏️ **Exercise** · Complete `get_with_retry`: on `429` or any `5xx`, wait (use the `Retry-After` header if present,
# otherwise `2 ** attempt` seconds plus random jitter, capped at 30) and try again; on any other error raise; on success
# return the JSON body. Then `fetch_all` pages through every result for a cursor value.

# %%
def get_with_retry(url, params, tries=6):
    for attempt in range(tries):
        # BEGIN SOLUTION
        r = requests.get(url, params=params, timeout=30)
        if r.status_code == 429 or r.status_code >= 500:
            wait = float(r.headers.get("Retry-After", 0)) or min(30, 2 ** attempt) + random.random()
            print(f"  {r.status_code} on page {params.get('page')}: waiting {wait:.1f}s")
            time.sleep(wait)
            continue
        r.raise_for_status()
        return r.json()
        # END SOLUTION
    raise RuntimeError(f"gave up after {tries} tries")


def fetch_all(updated_since):
    rows, page = [], 1
    while page:
        body = get_with_retry(f"{BASE}/payments", {"updated_since": updated_since, "page": page})
        rows += body["data"]
        page = body["next_page"]
    return rows

# %%
before = api.throttled
rows = fetch_all("2026-09-12T00:00:00.000Z")
print(f"{len(rows):,} payments updated since 12 Sep | requests so far: {api.requests} | 429s: {api.throttled - before}")
U.check(len(rows) == len({r['payment_id'] for r in rows}), "no duplicates across pages")
U.check(api.throttled > before, "we hit at least one rate limit and survived it")

# %% [markdown]
# That works, but notice everything we'd still have to build: remembering the cursor between runs, typing the
# columns, creating tables, merging updates, recording what was loaded when. That's what an ingestion framework is for.
#
# ## 2 · dlt resource with an incremental cursor + merge (7')
#
# A dlt **resource** is a Python generator that yields records (or pages of records). Declarations on the decorator tell
# dlt how to load them:
# - `primary_key="payment_id"` + `write_disposition="merge"`: upsert, so an updated payment (e.g. a refund) replaces its old version;
# - `dlt.sources.incremental("updated_at", initial_value=...)`: dlt keeps the highest `updated_at` seen in the pipeline
#   **state**, gives it back as `.last_value` on the next run, and drops records it has already loaded.
#
# dlt's `RESTClient` handles paging (here: follow the `next` link) and retries 429s for us.
#
# ✏️ **Exercise** · Complete the resource: page through `/payments` with `updated_since` set to the incremental's
# `last_value`, and yield every page.

# %%
api.rate_limit_every = 25   # be kinder from now on: dlt retries 429s with backoff, but waiting is boring

@dlt.resource(name="payments", primary_key="payment_id", write_disposition="merge")
def payments(updated_at=dlt.sources.incremental("updated_at", initial_value="2026-08-01T00:00:00.000Z")):
    client = RESTClient(base_url=BASE, paginator=JSONLinkPaginator(next_url_path="next"), data_selector="data")
    # BEGIN SOLUTION
    yield from client.paginate("/payments", params={"updated_since": updated_at.last_value})
    # END SOLUTION


pipe = dlt.pipeline(pipeline_name="qafila_ingest", destination=dlt.destinations.duckdb(str(LAKE_DB)),
                    dataset_name="raw", pipelines_dir=str(OUT / ".dlt"))
info = pipe.run(payments())
print(info)

# %%
def q(sql):
    """Run SQL against the pipeline's destination and return a DataFrame."""
    with pipe.sql_client() as c:
        with c.execute_query(sql) as cur:
            return cur.df()

loaded = q("SELECT count(*) AS rows, count(DISTINCT payment_id) AS ids, max(updated_at) AS max_updated FROM raw.payments")
loaded

# %%
U.check(loaded.rows[0] == loaded.ids[0], "one row per payment_id")
print("cursor stored in pipeline state:", pipe.state["sources"]["qafila_ingest"]["resources"]["payments"]["incremental"]["updated_at"]["last_value"])

# %% [markdown]
# ## 3 · Run twice: prove the load is idempotent (3')
#
# Run the same pipeline again without changing anything. Then move the API's clock forward a week (new payments, and
# refunds that update old ones) and run once more.

# %%
rows_before = q("SELECT count(*) AS n FROM raw.payments").n[0]
pipe.run(payments())
rows_rerun = q("SELECT count(*) AS n FROM raw.payments").n[0]
U.check(rows_rerun == rows_before, f"re-run changed nothing ({rows_before:,} rows)")

api.set_clock("2026-09-22T00:00:00.000Z")          # a week later
pipe.run(payments())
after = q("SELECT count(*) AS rows, count(DISTINCT payment_id) AS ids FROM raw.payments")
print(f"after a week: {after.rows[0]:,} rows, {after.ids[0]:,} distinct payment ids")

# %% [markdown]
# ✏️ **Exercise** · Prove the merge really **updated** rows instead of appending them. Payments created before
# 15 September were all loaded by the first run. Count those (`created_at` before 15 September) that now carry a
# **different** `_dlt_load_id` than the first load's: they were refunded during the week and replaced by the merge.
# Store the number in `updated_by_merge`. (`first_load` is given.)

# %%
first_load = q("SELECT min(load_id) AS id FROM raw._dlt_loads").id[0]
# BEGIN SOLUTION
updated_by_merge = q(f"""
    SELECT count(*) AS n FROM raw.payments
    WHERE created_at < TIMESTAMPTZ '2026-09-15 00:00:00+00' AND _dlt_load_id <> '{first_load}'
""").n[0]
# END SOLUTION
print("payments replaced by the merge:", updated_by_merge)
q(f"""SELECT status, count(*) AS n FROM raw.payments
      WHERE created_at < TIMESTAMPTZ '2026-09-15 00:00:00+00' AND _dlt_load_id <> '{first_load}' GROUP BY ALL""")

# %%
U.check(updated_by_merge > 0, "some old payments were updated in place")
U.check(after.rows[0] == after.ids[0], "still exactly one row per payment after the update run")

# %% [markdown]
# ## 4 · Load app tables (full for stores, incremental for orders) (5')
#
# dlt's `sql_table` reads a table through SQLAlchemy. Small reference tables are cheapest to **replace** every run;
# big, growing tables load **incrementally** on a cursor and **merge** on their key.
#
# ✏️ **Exercise** · Create `orders` as an incremental, merged `sql_table` on `updated_at` (primary key `order_id`). Start
# from `2026-09-25 00:00:00` so the first run is small.

# %%
from dlt.sources.sql_database import sql_table

APP_DB = f"sqlite:///{(U.RAW / 'qafila_oltp.sqlite').as_posix()}"
stores = sql_table(credentials=APP_DB, table="stores", write_disposition="replace", backend="pyarrow")
# BEGIN SOLUTION
orders = sql_table(credentials=APP_DB, table="orders", backend="pyarrow",
                   incremental=dlt.sources.incremental("updated_at", initial_value="2026-09-25 00:00:00"),
                   primary_key="order_id", write_disposition="merge")
# END SOLUTION
print(pipe.run([stores, orders]))
q("""SELECT 'stores' AS t, count(*) AS n FROM raw.stores
     UNION ALL SELECT 'orders', count(*) FROM raw.orders""")

# %%
n1 = q("SELECT count(*) AS n FROM raw.orders").n[0]
pipe.run([stores, orders])
n2 = q("SELECT count(*) AS n FROM raw.orders").n[0]
U.check(n1 == n2, "re-running the orders load adds nothing")
U.check(q("SELECT count(*) AS n FROM raw.stores").n[0] == 12, "stores replaced, not duplicated")

# %% [markdown]
# 🔍 The cursor here is the app's `updated_at`. That works only because the app sets it on every change. It also can't
# see **hard deletes**: an order row deleted in the source simply never shows up again. Lab 1.4 handles that with CDC.
#
# ## 5 · Spot the new `wallet_provider` column; read the load log (5')
#
# From 15 September the payment provider started sending `wallet_provider` for wallet payments. Nobody told us.
# dlt noticed and evolved the table: compare the columns, and see which rows have it.

# %%
q("""SELECT column_name, data_type FROM information_schema.columns
     WHERE table_schema = 'raw' AND table_name = 'payments' ORDER BY ordinal_position""")

# %%
q("""SELECT method, wallet_provider, count(*) AS n FROM raw.payments
     WHERE method = 'wallet' GROUP BY ALL ORDER BY n DESC""")

# %% [markdown]
# Every load is recorded. `_dlt_loads` has one row per load package; every data row carries the `_dlt_load_id` of the
# load that wrote it.
#
# ✏️ **Exercise** · Build `load_log`: one row per load in `raw._dlt_loads` (columns `load_id`, `inserted_at`) with the number
# of `raw.payments` rows that **currently** carry that `_dlt_load_id` (`payments_rows`, 0 if none), ordered by `inserted_at`.

# %%
# BEGIN SOLUTION
load_log = q("""
    SELECT l.load_id, l.inserted_at, count(p._dlt_id) AS payments_rows
    FROM raw._dlt_loads l
    LEFT JOIN raw.payments p ON p._dlt_load_id = l.load_id
    GROUP BY ALL ORDER BY l.inserted_at
""")
# END SOLUTION
load_log

# %%
U.check(load_log.payments_rows.sum() == after.rows[0], "every payment row is traceable to a load")

# %% [markdown]
# 🔍 The first load now "owns" fewer rows than it inserted: the refunds merged a week later replaced some of its rows.
# The re-run in Task 3 found nothing new, so it created no load at all, and the last two loads are the app tables.
# Lineage at row level is cheap and invaluable when you have to answer "where did this number come from?"
#
# ## ✱ Stretch · Load the supplier CSVs; what happens in week 39?
#
# Load the four weekly price files with a simple resource (append, one file per run, in order) and inspect the columns
# after each run. What did dlt do with week 38's `"9,49"` values, and with week 39's `cost_sar`?

# %%
import csv

# BEGIN SOLUTION
@dlt.resource(name="supplier_prices", write_disposition="append")
def supplier_file(path):
    with open(path, encoding="utf-8-sig") as f:
        for row in csv.DictReader(f):
            for k in ("unit_cost", "cost_sar"):
                if k in row:
                    try:
                        row[k] = float(row[k])
                    except ValueError:
                        pass   # leave unparseable text as it is
            row["_file"] = Path(path).name
            yield row

for path in sorted((U.RAW / "suppliers").glob("prices_*.csv")):
    pipe.run(supplier_file(path))
    cols = q("""SELECT column_name, data_type FROM information_schema.columns
                WHERE table_schema = 'raw' AND table_name = 'supplier_prices' ORDER BY ordinal_position""")
    print(path.name, "->", [c for c in cols.column_name if not c.startswith("_dlt")])
q("SELECT _file, count(unit_cost) AS unit_cost, count(unit_cost__v_text) AS unit_cost_text, count(cost_sar) AS cost_sar "
  "FROM raw.supplier_prices GROUP BY ALL ORDER BY _file")
# END SOLUTION

# %% [markdown]
# 🔍 dlt never fails on these files: week 38's text values land in a **variant column** `unit_cost__v_text`, and week
# 39's renamed column becomes a new column, leaving `unit_cost` empty. The raw layer is complete, but a naive report
# on `unit_cost` silently loses two weeks. Tolerant ingestion needs strict validation downstream (Lab 2.4) and a
# contract with the supplier (Lab 4.2).

# %%
api.stop()

# %% [markdown]
# ## Checkpoint
#
# 1. Where does dlt keep the incremental cursor, and why does it matter that it is committed with the data?
# 2. Which tables would you load with `replace`, `append` and `merge` at Qafila, and why?
# 3. The API returns the same payment on two pages because data changed during paging. What protects you?
# 4. Is it good or bad that dlt accepted week 39's renamed column without failing? Who should find out, and when?
