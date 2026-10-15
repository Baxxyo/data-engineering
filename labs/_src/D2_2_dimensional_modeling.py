# %% [markdown]
# # Lab 2.2 · Model Qafila as a star schema
#
# Day 2 · Session 2 (Dimensional modeling) · 25 min. What you'll build, in plain SQL:
# - a declared, tested **grain** for Qafila's sales fact;
# - four **dimensions** (date, product, store, customer), with surrogate keys and an "unknown" member;
# - `fct_order_items`, reconciled to the source to the halala;
# - answers to real business questions, and the classic **fan-out** mistake (and its fix).
#
# In Lab 2.3 the same model moves into dbt. Doing it by hand first shows what dbt automates and what it doesn't: the
# modeling decisions are yours either way.
#
# **Tasks**
# - [ ] 1 · Declare the grain and test it (4')
# - [ ] 2 · Build four dimensions, including an unknown member (7')
# - [ ] 3 · Build the fact table and reconcile it (6')
# - [ ] 4 · Answer business questions from the star (5')
# - [ ] 5 · Spot the fan-out trap (3')
# - [ ] ✱ Stretch · One big table vs. star: size and speed

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
import duckdb
import pandas as pd

U.build_warehouse()
OUT = U.lab_dir("D2_2", reset=True)
con = duckdb.connect(str(OUT / "qafila_model.duckdb"))         # our model lives in its own file...
con.sql("SET enable_progress_bar = false")
con.sql(f"ATTACH '{U.WAREHOUSE_DB.as_posix()}' AS wh (READ_ONLY)")   # ...and reads the bronze layer from the warehouse
con.sql("CREATE SCHEMA IF NOT EXISTS star")
q = lambda sql: con.sql(sql).df()

# %% [markdown]
# ## 1 · Declare the grain and test it (4')
#
# The **grain** is the one sentence that says what a fact row means. Everything else (keys, measures, which dimensions
# can attach) follows from it. For Qafila's sales:
#
# > **One row per order line**: one product in one order, with its quantity and price at the time of ordering.
#
# A grain is a claim about the data, so test it.
#
# ✏️ **Exercise** · Compute `n_lines` (rows in `wh.raw.order_items`) and `n_keys` (distinct `(order_id, line_no)` pairs).
# The grain holds if they're equal.

# %%
# BEGIN SOLUTION
n_lines, n_keys = con.sql("""
    SELECT count(*), count(DISTINCT (order_id, line_no)) FROM wh.raw.order_items
""").fetchone()
# END SOLUTION
U.check(n_lines == n_keys, f"(order_id, line_no) is unique across {n_lines:,} lines: the grain holds")

# %% [markdown]
# Which measures live at this grain? `quantity`, `unit_price`, `discount` and `net_amount` are **line-level**. The
# `delivery_fee` is **order-level**: it doesn't belong in this fact (Task 5 shows what happens if you put it there anyway).
#
# ## 2 · Build four dimensions, including an unknown member (7')
#
# Dimensions hold the descriptive context you filter and group by. Two conventions we follow:
# - **Surrogate keys** (`product_key`, ...): integers owned by the warehouse, independent of source IDs. They're essential
#   once a dimension keeps history (SCD2: several rows per product) or merges sources with clashing IDs.
# - An **unknown member** (key `-1`): facts whose reference is missing (an erased customer) still join, instead of
#   silently disappearing from inner joins.
#
# The date dimension is generated, not loaded:

# %%
con.sql("""
    CREATE OR REPLACE TABLE star.dim_date AS
    SELECT strftime(d, '%Y%m%d')::INTEGER AS date_key, d AS date_day, year(d) AS year, month(d) AS month,
           monthname(d) AS month_name, dayname(d) AS day_name, weekofyear(d) AS iso_week,
           dayofweek(d) IN (5, 6) AS is_weekend,                    -- Friday, Saturday
           d = DATE '2026-09-23' AS is_national_day
    FROM (SELECT unnest(range(DATE '2026-08-01', DATE '2026-10-01', INTERVAL 1 DAY))::DATE AS d)
""")
q("SELECT * FROM star.dim_date WHERE date_day BETWEEN '2026-09-22' AND '2026-09-26'")

# %% [markdown]
# ✏️ **Exercise** · Build the other three dimensions in schema `star`:
# - `dim_product`: `product_key` (1, 2, 3... by `product_id`), `product_id`, `sku` (trimmed), `name_en`, `category`, `brand`, `list_price`;
# - `dim_store`: `store_key` (1, 2, 3... by `store_id`), `store_id`, `store_name` (`name_en`), `city`;
# - `dim_customer`: `customer_key` (1, 2, 3... by `customer_id`), `customer_id`, `city`, `district`, `signup_date`; **no**
#   name, email, mobile, national ID or IBAN (analytics doesn't need them). Then add the unknown member:
#   `(-1, -1, 'Unknown', 'Unknown', NULL)`.

# %%
# BEGIN SOLUTION
con.sql("""
    CREATE OR REPLACE TABLE star.dim_product AS
    SELECT row_number() OVER (ORDER BY product_id) AS product_key, product_id, trim(sku) AS sku, name_en,
           category, brand, unit_price AS list_price
    FROM wh.raw.products
""")
con.sql("""
    CREATE OR REPLACE TABLE star.dim_store AS
    SELECT row_number() OVER (ORDER BY store_id) AS store_key, store_id, name_en AS store_name, city
    FROM wh.raw.stores
""")
con.sql("""
    CREATE OR REPLACE TABLE star.dim_customer AS
    SELECT row_number() OVER (ORDER BY customer_id) AS customer_key, customer_id, city, district,
           created_at::DATE AS signup_date
    FROM wh.raw.customers
""")
con.sql("INSERT INTO star.dim_customer VALUES (-1, -1, 'Unknown', 'Unknown', NULL)")
# END SOLUTION
q("""SELECT 'dim_product' AS dim, count(*) AS rows FROM star.dim_product
     UNION ALL SELECT 'dim_store', count(*) FROM star.dim_store
     UNION ALL SELECT 'dim_customer', count(*) FROM star.dim_customer""")

# %%
U.check(q("SELECT count(*) AS n FROM star.dim_customer WHERE customer_key = -1").n[0] == 1, "unknown member present")
U.check("national_id" not in con.table("star.dim_customer").columns, "no direct identifiers in the customer dimension")

# %% [markdown]
# ## 3 · Build the fact table and reconcile it (6')
#
# The fact table holds **keys** (to dimensions) and **measures** (numbers you add up), at the declared grain.
# Degenerate dimensions such as `order_id` and `order_status` stay in the fact: they have no other attributes worth a table.
#
# ✏️ **Exercise** · Create `star.fct_order_items` with: `order_id`, `line_no`, `date_key` (Riyadh date of the order as
# `YYYYMMDD` integer: `created_at` is UTC text, add 3 hours), `customer_key` (`-1` when the customer no longer exists),
# `product_key`, `store_key`, `order_status`, `quantity`, `unit_price`, `discount`, `net_amount` (= quantity × unit_price − discount).
# Keep every line for now, including the negative quantities (we deal with them in Lab 2.4).

# %%
# BEGIN SOLUTION
con.sql("""
    CREATE OR REPLACE TABLE star.fct_order_items AS
    SELECT i.order_id, i.line_no,
           strftime((o.created_at::TIMESTAMP + INTERVAL 3 HOUR)::DATE, '%Y%m%d')::INTEGER AS date_key,
           coalesce(c.customer_key, -1)       AS customer_key,
           p.product_key,
           s.store_key,
           o.status                           AS order_status,
           i.quantity, i.unit_price, i.discount,
           round(i.quantity * i.unit_price - i.discount, 2) AS net_amount
    FROM wh.raw.order_items i
    JOIN wh.raw.orders o USING (order_id)
    LEFT JOIN star.dim_customer c ON c.customer_id = o.customer_id
    JOIN star.dim_product p ON p.product_id = i.product_id
    JOIN star.dim_store s ON s.store_id = o.store_id
""")
# END SOLUTION
q("SELECT * FROM star.fct_order_items LIMIT 3")

# %% [markdown]
# Reconciliation: the fact must have exactly the source's rows and the source's money. If a join silently dropped or
# duplicated rows, these numbers tell you.

# %%
recon = q("""
    SELECT (SELECT count(*) FROM wh.raw.order_items)                                             AS source_rows,
           (SELECT count(*) FROM star.fct_order_items)                                           AS fact_rows,
           (SELECT round(sum(quantity * unit_price - discount), 2) FROM wh.raw.order_items)       AS source_sar,
           (SELECT round(sum(net_amount), 2) FROM star.fct_order_items)                          AS fact_sar,
           (SELECT count(*) FROM star.fct_order_items WHERE customer_key = -1)                   AS unknown_customer_rows
""")
recon

# %%
U.check(recon.source_rows[0] == recon.fact_rows[0], "no rows lost or duplicated")
U.check(abs(recon.source_sar[0] - recon.fact_sar[0]) < 1, "revenue reconciles")
U.check(recon.unknown_customer_rows[0] > 0, "lines of erased customers kept via the unknown member")

# %% [markdown]
# ## 4 · Answer business questions from the star (5')
#
# Star queries all look alike: filter and group by dimension attributes, sum fact measures. Here is one:

# %%
q("""
    SELECT p.category, round(sum(f.net_amount)) AS revenue_sar, sum(f.quantity) AS units
    FROM star.fct_order_items f JOIN star.dim_product p USING (product_key)
    WHERE f.order_status = 'delivered'
    GROUP BY ALL ORDER BY revenue_sar DESC LIMIT 5
""")

# %% [markdown]
# ✏️ **Exercise** · Answer two questions with star queries (delivered orders only):
# - `weekend`: average **daily** revenue on weekend days vs. weekdays, by city (columns `city`, `is_weekend`, `avg_daily_revenue`);
# - `national_day`: for each city, revenue on National Day vs. the average of the other September days
#   (columns `city`, `national_day_revenue`, `other_days_avg`, `uplift` = ratio of the two).

# %%
# BEGIN SOLUTION
weekend = q("""
    WITH daily AS (
        SELECT d.date_day, d.is_weekend, s.city, sum(f.net_amount) AS revenue
        FROM star.fct_order_items f
        JOIN star.dim_date d USING (date_key) JOIN star.dim_store s USING (store_key)
        WHERE f.order_status = 'delivered'
        GROUP BY ALL
    )
    SELECT city, is_weekend, round(avg(revenue)) AS avg_daily_revenue FROM daily GROUP BY ALL ORDER BY city, is_weekend
""")
national_day = q("""
    WITH daily AS (
        SELECT d.date_day, d.is_national_day, s.city, sum(f.net_amount) AS revenue
        FROM star.fct_order_items f
        JOIN star.dim_date d USING (date_key) JOIN star.dim_store s USING (store_key)
        WHERE f.order_status = 'delivered' AND d.month = 9
        GROUP BY ALL
    )
    SELECT city,
           round(max(revenue) FILTER (WHERE is_national_day)) AS national_day_revenue,
           round(avg(revenue) FILTER (WHERE NOT is_national_day)) AS other_days_avg,
           round(national_day_revenue / other_days_avg, 2) AS uplift
    FROM daily GROUP BY city ORDER BY city
""")
# END SOLUTION
display(weekend, national_day)

# %%
U.check(len(national_day) == 3 and (national_day.uplift > 1.5).all(), "National Day lifted revenue in every city")

# %% [markdown]
# ## 5 · Spot the fan-out trap (3')
#
# Finance wants "revenue including delivery fees". A colleague joins the **order-level** fee onto the **line-level** fact:

# %%
fanout = q("""
    SELECT round(sum(f.net_amount + o.delivery_fee)) AS revenue_incl_fees
    FROM star.fct_order_items f JOIN wh.raw.orders o USING (order_id)
    WHERE f.order_status = 'delivered'
""")
fanout

# %% [markdown]
# ✏️ **Exercise** · Compute the correct number, `correct`: line revenue summed at line grain **plus** delivery fees summed
# at **order** grain (once per order). How much did the naive query overstate fees?

# %%
# BEGIN SOLUTION
correct = q("""
    SELECT round((SELECT sum(net_amount) FROM star.fct_order_items WHERE order_status = 'delivered')
               + (SELECT sum(delivery_fee) FROM wh.raw.orders WHERE status = 'delivered')) AS revenue_incl_fees
""")
# END SOLUTION
over = fanout.revenue_incl_fees[0] - correct.revenue_incl_fees[0]
print(f"naive: {fanout.revenue_incl_fees[0]:,.0f} SAR | correct: {correct.revenue_incl_fees[0]:,.0f} SAR | overstated by {over:,.0f} SAR")
U.check(over > 0, "the naive join counts each order's fee once per line")

# %% [markdown]
# 🔍 This is **fan-out**: joining a coarser grain to a finer one repeats the coarse measure. It's the most common way
# dashboards end up wrong. The fix is in the model: order-level measures belong in an order-grain fact (`fct_orders`),
# and you combine facts at a shared grain (here: day or order), never by joining one fact's rows to the other's.
#
# ## ✱ Stretch · One big table vs. star: size and speed
#
# Many teams now serve a denormalized **one big table** (OBT) on top of the star: all attributes pre-joined. Build
# `star.obt_order_items` and compare storage and the speed of the category query above.

# %%
# BEGIN SOLUTION
con.sql("""
    CREATE OR REPLACE TABLE star.obt_order_items AS
    SELECT f.*, d.date_day, d.day_name, d.is_weekend, p.name_en AS product, p.category, p.brand,
           s.store_name, s.city AS store_city, c.city AS customer_city, c.district
    FROM star.fct_order_items f
    JOIN star.dim_date d USING (date_key) JOIN star.dim_product p USING (product_key)
    JOIN star.dim_store s USING (store_key) JOIN star.dim_customer c USING (customer_key)
""")
star_q = """SELECT p.category, sum(f.net_amount) FROM star.fct_order_items f JOIN star.dim_product p USING (product_key)
            WHERE f.order_status = 'delivered' GROUP BY ALL"""
obt_q = "SELECT category, sum(net_amount) FROM star.obt_order_items WHERE order_status = 'delivered' GROUP BY ALL"
print(f"star: {U.best_of(lambda: con.sql(star_q).fetchall()) * 1000:.1f} ms | OBT: {U.best_of(lambda: con.sql(obt_q).fetchall()) * 1000:.1f} ms")
print("columns: fact", len(con.table("star.fct_order_items").columns), "| OBT", len(con.table("star.obt_order_items").columns))
# END SOLUTION

# %% [markdown]
# 🔍 On a columnar engine the OBT is often a bit faster and much easier for analysts, at the cost of storage and of
# rebuilding it whenever a dimension changes. A common pattern: model as a star (the source of truth), serve OBTs or
# semantic-layer views on top (Lab 4.4).
#
# ## Checkpoint
#
# 1. State the grain of `fct_orders` and of a daily inventory snapshot fact. Which measures are additive across days?
# 2. Why use surrogate keys when the source already has IDs?
# 3. A customer moves from Jeddah to Riyadh. With `dim_customer` as built here (Type 1), which reports change retroactively?
# 4. Where else at Qafila could fan-out bite? (Hint: payments per order, events per order.)
