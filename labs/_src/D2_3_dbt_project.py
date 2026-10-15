# %% [markdown]
# # Lab 2.3 · A dbt project for Qafila
#
# Day 2 · Session 3 (dbt: transformation as software) · 25 min. What you'll do:
# - tour a real dbt project: sources, staging → intermediate → marts, macros, configs;
# - write a **staging model** and an **incremental fact model** yourself, and build the DAG;
# - watch an incremental run pick up a late change, and read the **lineage** dbt derives from `ref()`.
#
# The complete project is in `labs/dbt_qafila/`. This lab works on a **copy** with two models removed
# (`stg_orders` and `fct_order_items`), and on a copy of the warehouse, so nothing you break affects other labs.
# dbt runs **in-process** from Python (`U.run_dbt([...])` wraps `dbtRunner`); in a terminal you'd type `dbt build` etc.
#
# **Tasks**
# - [ ] 1 · Tour the project and list its models (4')
# - [ ] 2 · Write `stg_orders` and build the staging layer (6')
# - [ ] 3 · Write the incremental `fct_order_items` and build the marts (7')
# - [ ] 4 · Change a source row; watch the incremental run (5')
# - [ ] 5 · Read the lineage from the manifest (3')
# - [ ] ✱ Stretch · Add a weekly category mart and document it

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
import json
import shutil

import duckdb

U.build_warehouse()
OUT = U.lab_dir("D2_3", reset=True)
PROJ = OUT / "dbt_qafila"
DB = OUT / "qafila.duckdb"
shutil.copytree(U.DBT_PROJECT, PROJ, ignore=shutil.ignore_patterns("target", "logs", "dbt_packages"))
shutil.copy(U.WAREHOUSE_DB, DB)                                  # bronze layer only
for m in ["models/staging/stg_orders.sql", "models/marts/fct_order_items.sql"]:   # you'll write these two
    (PROJ / m).write_text("-- TODO (Lab 2.3): placeholder so the project still parses\nselect 1 as todo\n", encoding="utf-8")


def dbt(*args, quiet=False):
    """Run dbt on this lab's project and database."""
    return U.run_dbt(list(args), project_dir=PROJ, db_path=DB, quiet=quiet)


def sql(query):
    with duckdb.connect(str(DB)) as c:          # same settings as dbt's own connection in this process
        return c.sql(query).df()

# %% [markdown]
# ## 1 · Tour the project and list its models (4')
#
# A dbt project is a folder of SQL `SELECT`s plus YAML. dbt turns each `SELECT` into a view or table, in dependency order.

# %%
for p in sorted(PROJ.rglob("*")):
    if p.is_file():
        print(p.relative_to(PROJ))

# %%
print((PROJ / "dbt_project.yml").read_text(encoding="utf-8"))
print((PROJ / "models/staging/stg_order_items.sql").read_text(encoding="utf-8"))

# %% [markdown]
# Three things to notice:
# - `{{ source('raw', 'order_items') }}` points at a declared **source** (`models/staging/_sources.yml`), never at a table name.
# - Folder-level configs in `dbt_project.yml`: staging models are **views** in schema `staging`, marts are **tables** in `marts`.
# - Reusable SQL lives in **macros** (`macros/text.sql`: Arabic normalization, number parsing, E.164 mobiles, Riyadh time).
#
# `dbt ls` lists what dbt sees. `+fct_orders` means "fct_orders and everything upstream of it".

# %%
res = dbt("ls", "--resource-type", "model", "--output", "name", quiet=True)
print(sorted(res.result))

# %% [markdown]
# Every model is listed, including the two placeholders: dbt checks at parse time that every `ref()` points to an existing
# model, so we couldn't simply delete them. Let's write them.
#
# ## 2 · Write `stg_orders` and build the staging layer (6')
#
# ✏️ **Exercise** · Write `models/staging/stg_orders.sql`: one row per order from `source('raw', 'orders')` with the columns
# `order_id, customer_id, store_id, status, payment_method, promo_code, delivery_fee`, plus
# - `created_at_utc` (cast `created_at` to `timestamp`),
# - `created_at_riyadh` and `order_date` (Riyadh date) using the `to_riyadh()` macro,
# - `updated_at_utc`.
#
# Write the file from Python (the cell writes `STG_ORDERS` to disk).

# %%
# BEGIN SOLUTION
STG_ORDERS = """
-- One row per order. The app stores UTC as text; Qafila reports in Riyadh time.
select
    order_id,
    customer_id,
    store_id,
    status,
    payment_method,
    promo_code,
    delivery_fee,
    created_at::timestamp                                as created_at_utc,
    {{ to_riyadh('created_at::timestamp') }}             as created_at_riyadh,
    {{ to_riyadh('created_at::timestamp') }}::date       as order_date,
    updated_at::timestamp                                as updated_at_utc
from {{ source('raw', 'orders') }}
"""
# END SOLUTION
(PROJ / "models/staging/stg_orders.sql").write_text(STG_ORDERS.strip() + "\n", encoding="utf-8")
dbt("run", "--select", "staging")

# %%
sql("SELECT order_id, created_at_utc, created_at_riyadh, order_date FROM staging.stg_orders WHERE order_id = 5000002")

# %%
row = sql("SELECT created_at_utc, created_at_riyadh, order_date FROM staging.stg_orders WHERE order_id = 5000002").iloc[0]
U.check((row.created_at_riyadh - row.created_at_utc).total_seconds() == 3 * 3600, "Riyadh time = UTC + 3h")
U.check(sql("SELECT count(*) AS n FROM staging.stg_orders").n[0] == 45766, "one row per order")

# %% [markdown]
# Peek at what dbt actually sent to DuckDB: `dbt compile` renders the Jinja into plain SQL under `target/compiled/`.

# %%
dbt("compile", "--select", "stg_orders", quiet=True)
print(next((PROJ / "target" / "compiled").rglob("stg_orders.sql")).read_text(encoding="utf-8"))

# %% [markdown]
# ## 3 · Write the incremental `fct_order_items` and build the marts (7')
#
# The order-line fact is the biggest table, and it grows every day. An **incremental** model processes only new or
# changed data on later runs:
# - `is_incremental()` is true when the table already exists (and you didn't pass `--full-refresh`);
# - `{{ this }}` is the existing table, so `max(...)` over it is your high-water mark;
# - `unique_key` + `incremental_strategy='delete+insert'` replaces reprocessed rows instead of duplicating them.
#
# ✏️ **Exercise** · Write `models/marts/fct_order_items.sql`, the Lab 2.2 fact in dbt form. Grain: one row per order line
# with `quantity > 0`. Config: `materialized='incremental'`, `unique_key='order_line_id'`, `incremental_strategy='delete+insert'`,
# `on_schema_change='fail'`. On incremental runs, only take orders with `updated_at_utc` later than **3 days before** the max
# `order_updated_at_utc` already loaded (late status changes). Columns, in this order (the YAML contract enforces them):
# `order_line_id, order_id, line_no, order_date, customer_id (-1 via var('unknown_key') if not in dim_customers), store_id,
# product_id, order_status, payment_method, promo_code, quantity, unit_price, discount, gross_amount, net_amount, order_updated_at_utc`.

# %%
# BEGIN SOLUTION
FCT = """
{{
    config(
        materialized='incremental',
        unique_key='order_line_id',
        incremental_strategy='delete+insert',
        on_schema_change='fail'
    )
}}

with orders as (
    select * from {{ ref('stg_orders') }}
    {% if is_incremental() %}
    where updated_at_utc > (select max(order_updated_at_utc) - interval 3 day from {{ this }})
    {% endif %}
),

customers as (
    select customer_id from {{ ref('dim_customers') }}
)

select
    i.order_line_id,
    i.order_id,
    i.line_no,
    o.order_date,
    coalesce(c.customer_id, {{ var('unknown_key') }})   as customer_id,
    o.store_id,
    i.product_id,
    o.status                                            as order_status,
    o.payment_method,
    o.promo_code,
    i.quantity,
    i.unit_price,
    i.discount,
    i.gross_amount,
    i.net_amount,
    o.updated_at_utc                                    as order_updated_at_utc
from orders o
join {{ ref('stg_order_items') }} i on i.order_id = o.order_id
left join customers c on c.customer_id = o.customer_id
where i.quantity > 0
"""
# END SOLUTION
(PROJ / "models/marts/fct_order_items.sql").write_text(FCT.strip() + "\n", encoding="utf-8")
dbt("seed", quiet=True)
dbt("run", "--select", "+fct_orders")       # fct_orders and everything it depends on

# %%
facts = sql("SELECT count(*) AS lines, count(DISTINCT order_id) AS orders, round(sum(net_amount)) AS net_sar FROM marts.fct_order_items")
facts

# %%
U.check(facts.lines[0] == 172545 - 142, "every order line except the 142 with non-positive quantity")
U.check(sql("SELECT count(*) AS n FROM marts.fct_order_items WHERE customer_id = -1").n[0] > 0, "erased customers mapped to -1")

# %% [markdown]
# ## 4 · Change a source row; watch the incremental run (5')
#
# Simulate tomorrow's load: one delivered order is refunded on 1 October, so its `status` and `updated_at` change in the
# source. Then run the fact model again, incrementally.

# %%
with duckdb.connect(str(DB)) as c:
    c.sql("UPDATE raw.orders SET status = 'refunded', updated_at = '2026-10-01 09:15:00' WHERE order_id = 5000002")
before = sql("SELECT order_status, order_updated_at_utc FROM marts.fct_order_items WHERE order_id = 5000002 LIMIT 1")
dbt("run", "--select", "fct_order_items")
after = sql("SELECT order_status, order_updated_at_utc FROM marts.fct_order_items WHERE order_id = 5000002 LIMIT 1")
display(before, after)

# %% [markdown]
# ✏️ **Exercise** · Prove the run was incremental and idempotent:
# - `n_processed`: how many orders did the incremental filter select? (Count orders in `staging.stg_orders` with
#   `updated_at_utc` later than 3 days before the max `order_updated_at_utc` **before** this run: `2026-09-30 23:59:59`.)
# - `dupes`: count of `order_line_id` values that appear more than once in the fact (should be 0).

# %%
# BEGIN SOLUTION
n_processed = sql("""SELECT count(*) AS n FROM staging.stg_orders
                     WHERE updated_at_utc > TIMESTAMP '2026-09-30 23:59:59' - INTERVAL 3 DAY""").n[0]
dupes = sql("SELECT count(*) AS n FROM (SELECT order_line_id FROM marts.fct_order_items GROUP BY 1 HAVING count(*) > 1)").n[0]
# END SOLUTION
print(f"incremental run reprocessed {n_processed:,} of 45,766 orders | duplicate lines: {dupes}")
U.check(after.order_status[0] == "refunded", "the late change reached the fact table")
U.check(dupes == 0, "delete+insert replaced rows instead of duplicating them")

# %% [markdown]
# 🔍 The look-back window (3 days) is a business decision: it must cover how late changes can arrive. Too short and
# late refunds are missed forever; too long and every run reprocesses a lot. `dbt run --full-refresh` rebuilds from scratch
# when the logic changes.
#
# ## 5 · Read the lineage from the manifest (3')
#
# Every `ref()` and `source()` is an edge in a graph. dbt writes the graph to `target/manifest.json`; `dbt docs generate`
# adds column metadata and `dbt docs serve` renders it as a website.
#
# ✏️ **Exercise** · From the manifest, list the direct parents (`depends_on.nodes`) of `model.qafila.fct_orders`, and the
# **direct** children of `model.qafila.stg_orders` (the manifest's `child_map`). Store them in `parents` and `children` (lists of names).

# %%
dbt("parse", quiet=True)
manifest = json.loads((PROJ / "target" / "manifest.json").read_text(encoding="utf-8"))
# BEGIN SOLUTION
parents = [n.split(".")[-1] for n in manifest["nodes"]["model.qafila.fct_orders"]["depends_on"]["nodes"]]
children = [n.split(".")[-1] for n in manifest["child_map"]["model.qafila.stg_orders"] if n.startswith("model.")]
# END SOLUTION
print("fct_orders <-", sorted(parents))
print("stg_orders ->", sorted(children))
U.check({"fct_order_items", "stg_orders"} <= set(parents), "fct_orders reads the fact and the staging orders")

# %% [markdown]
# 🔍 This graph is how dbt knows the build order, what `+model` selects, and what breaks downstream when a column
# changes. Orchestrators (Lab 4.1) import exactly this graph to schedule each model as an asset.
#
# ## ✱ Stretch · Add a weekly category mart and document it
#
# Add `models/marts/mart_weekly_category.sql`: one row per ISO week and product category with delivered revenue and units,
# built from `fct_order_items` and `dim_products`. Add a YAML entry with a description and a uniqueness test on
# `iso_week || category`, then `dbt build --select mart_weekly_category`.

# %%
# BEGIN SOLUTION
(PROJ / "models/marts/mart_weekly_category.sql").write_text("""
select
    weekofyear(f.order_date)            as iso_week,
    p.category,
    round(sum(f.net_amount), 2)         as revenue_sar,
    sum(f.quantity)                     as units
from {{ ref('fct_order_items') }} f
join {{ ref('dim_products') }} p using (product_id)
where f.order_status = 'delivered'
group by all
""".strip() + "\n", encoding="utf-8")
(PROJ / "models/marts/_weekly.yml").write_text("""
version: 2
models:
  - name: mart_weekly_category
    description: Delivered revenue and units per ISO week (Riyadh dates) and product category.
    data_tests:
      - unique:
          arguments:
            column_name: "iso_week || '|' || category"
""".strip() + "\n", encoding="utf-8")
dbt("build", "--select", "mart_weekly_category")
display(sql("SELECT * FROM marts.mart_weekly_category ORDER BY iso_week DESC, revenue_sar DESC LIMIT 5"))
# END SOLUTION

# %% [markdown]
# ## Checkpoint
#
# 1. Why `{{ source() }}` and `{{ ref() }}` instead of table names? What would break without them?
# 2. Staging models are views and marts are tables here. When would you make a staging model a table?
# 3. A bug in `stg_orders` is fixed. Is the incremental `fct_order_items` now correct? What do you run?
# 4. dbt v2 (the Rust "Fusion" engine) parses projects much faster and understands SQL. What changes for this project? What doesn't?
