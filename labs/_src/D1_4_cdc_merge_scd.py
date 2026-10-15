# %% [markdown]
# # Lab 1.4 · From a CDC feed to an SCD2 dimension
#
# Day 1 · Session 4 (Change data capture & load strategies) · 25 min. What you'll build:
# - a current-state copy of Qafila's `customers` table, kept in sync from a Debezium-style change feed with `MERGE`;
# - a reconciliation that proves the copy matches the source exactly;
# - a **Type 2 slowly changing dimension** (full history) and an as-of join that uses it.
#
# The inputs: a snapshot of `customers` taken on **1 September 2026**, and every change made during September
# (`data/raw/cdc/customers_changes.jsonl`), in the order the connector delivered them.
#
# **Tasks**
# - [ ] 1 · Read the feed; find duplicates and out-of-order events (4')
# - [ ] 2 · Latest change per key; MERGE into a current-state table (7')
# - [ ] 3 · Reconcile with the source database (0 differences) (4')
# - [ ] 4 · Build the SCD2 history from the same events (7')
# - [ ] 5 · As-of join: revenue by customer city at order time (3')
# - [ ] ✱ Stretch · Make the MERGE incremental: process the feed in 4 batches

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

pd.set_option("display.max_colwidth", 60)
con = duckdb.connect()
con.sql("SET enable_progress_bar = false")
U.attach_app(con)
RAW = U.RAW.as_posix()

# %% [markdown]
# ## 1 · Read the feed; find duplicates and out-of-order events (4')
#
# We keep the delivery order as `arrival` (DuckDB preserves the file order when it numbers rows here) and lift the
# log position `source.lsn` into its own column.

# %%
con.sql(f"""
    CREATE TABLE changes AS
    SELECT row_number() OVER () AS arrival, source.lsn AS lsn, op, ts_ms, before, after
    FROM read_json_auto('{RAW}/cdc/customers_changes.jsonl')
""")
con.sql("SELECT op, count(*) AS events FROM changes GROUP BY op ORDER BY op").df()

# %%
con.sql("SELECT arrival, lsn, op, after.customer_id, after.city, after.mobile FROM changes WHERE op = 'u' LIMIT 3").df()

# %% [markdown]
# ✏️ **Exercise** · Compute:
# - `n_duplicates`: events delivered more than once (rows minus distinct `lsn`);
# - `n_out_of_order`: events whose `lsn` is **smaller** than the largest `lsn` that arrived before them.
#   Hint: `max(lsn) OVER (ORDER BY arrival ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING)`.

# %%
# BEGIN SOLUTION
n_duplicates = con.sql("SELECT count(*) - count(DISTINCT lsn) FROM changes").fetchone()[0]
n_out_of_order = con.sql("""
    SELECT count(*) FROM (
        SELECT lsn < max(lsn) OVER (ORDER BY arrival ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS late
        FROM changes
    ) WHERE late
""").fetchone()[0]
# END SOLUTION
total = con.sql("SELECT count(*) FROM changes").fetchone()[0]
print(f"{total:,} events | {n_duplicates} duplicates | {n_out_of_order:,} out of order")
U.check(n_duplicates > 100, "the connector redelivered some events")
U.check(n_out_of_order > 1000, "arrival order is not commit order")

# %% [markdown]
# 🔍 Both are normal for CDC: delivery is **at-least-once** (duplicates after a connector restart) and partitions or
# retries reorder events. The fix is the same for both: **trust the log position, not the arrival order**.
#
# ## 2 · Latest change per key; MERGE into a current-state table (7')
#
# Start from the snapshot. Careful with types: a CSV sniffer would read `0596390578` as the number 596390578 and silently
# drop the leading zero, so identifiers are read as text.

# %%
con.sql(f"""
    CREATE TABLE customers AS
    SELECT * FROM read_csv('{RAW}/cdc/customers_snapshot_2026-09-01.csv',
        types = {{'mobile': 'VARCHAR', 'national_id': 'VARCHAR', 'iban': 'VARCHAR',
                  'created_at': 'VARCHAR', 'updated_at': 'VARCHAR'}})
""")
print("snapshot rows:", con.sql("SELECT count(*) FROM customers").fetchone()[0])
con.sql("DESCRIBE customers").df()[["column_name", "column_type"]].T

# %% [markdown]
# Applying every event in `lsn` order would work, but it's wasteful: only the **last** change per customer matters for the
# current state. A delete has no `after` image, so the key comes from `before`.
#
# ✏️ **Exercise** · Create the view `latest`: one row per `customer_id` with its **last** event by `lsn` (duplicates
# dropped first), with columns `customer_id`, `op`, `lsn` and all the `after` fields (`after.*`). Then write the `MERGE` that
# deletes on `op = 'd'`, updates all columns on other matches, and inserts new customers (but never a deleted one).
#
# Hint: `MERGE INTO customers USING latest_rows s USING (customer_id) WHEN MATCHED AND s.op = 'd' THEN DELETE WHEN MATCHED
# THEN UPDATE SET ... WHEN NOT MATCHED AND ... THEN INSERT (...) VALUES (...)`. `latest_rows` (given) has `op` plus exactly
# the target's columns, in the same order.

# %%
# BEGIN SOLUTION
con.sql("""
    CREATE OR REPLACE VIEW latest AS
    WITH dedup AS (
        SELECT DISTINCT ON (lsn) * FROM changes
    )
    SELECT coalesce(after.customer_id, before.customer_id) AS customer_id, op, lsn, after.* EXCLUDE (customer_id)
    FROM dedup
    QUALIFY row_number() OVER (PARTITION BY coalesce(after.customer_id, before.customer_id) ORDER BY lsn DESC) = 1
""")
cols = ", ".join(con.table("customers").columns)
con.sql(f"CREATE OR REPLACE VIEW latest_rows AS SELECT op, {cols} FROM latest")
con.sql("""
    MERGE INTO customers
    USING latest_rows s USING (customer_id)
    WHEN MATCHED AND s.op = 'd' THEN DELETE
    WHEN MATCHED THEN UPDATE SET full_name = s.full_name, email = s.email, mobile = s.mobile, national_id = s.national_id,
                                 iban = s.iban, city = s.city, district = s.district, created_at = s.created_at,
                                 updated_at = s.updated_at, is_deleted = s.is_deleted
    WHEN NOT MATCHED AND s.op <> 'd' THEN INSERT (customer_id, full_name, email, mobile, national_id, iban, city, district,
                                                  created_at, updated_at, is_deleted)
        VALUES (s.customer_id, s.full_name, s.email, s.mobile, s.national_id, s.iban, s.city, s.district,
                s.created_at, s.updated_at, s.is_deleted)
""")
# END SOLUTION
con.sql("SELECT op, count(*) AS customers FROM latest GROUP BY op ORDER BY op").df()

# %% [markdown]
# ## 3 · Reconcile with the source database (0 differences) (4')
#
# The app database `app.customers` holds the true state at the end of September. If our MERGE is right, the two tables
# are identical, row for row, column for column.
#
# ✏️ **Exercise** · Count `missing` (rows in the source but not in our copy) and `extra` (rows in our copy but not in the
# source), comparing **all** columns. `EXCEPT` does exactly that.

# %%
# BEGIN SOLUTION
missing = con.sql("SELECT count(*) FROM (SELECT * FROM app.customers EXCEPT SELECT * FROM customers)").fetchone()[0]
extra = con.sql("SELECT count(*) FROM (SELECT * FROM customers EXCEPT SELECT * FROM app.customers)").fetchone()[0]
# END SOLUTION
n_src, n_copy = con.sql("SELECT (SELECT count(*) FROM app.customers), (SELECT count(*) FROM customers)").fetchone()
print(f"source {n_src:,} rows | copy {n_copy:,} rows | missing {missing} | extra {extra}")
U.check(missing == 0 and extra == 0, "the replica matches the source exactly")

# %% [markdown]
# 🔍 What would have gone wrong with the arrival order instead of `lsn`? Try it: rebuild `latest` ordering by `arrival`
# and re-run the reconciliation. A reconciliation query like this one is the best test a replication pipeline can have:
# run it on a schedule, and alert when it isn't zero.
#
# ## 4 · Build the SCD2 history from the same events (7')
#
# The current state answers "where does the customer live **now**?". Analysts also ask "where did they live **when they
# ordered**?". A Type 2 dimension keeps one row per **version** with a validity interval.
#
# Every version starts at its change time (`ts_ms`, the commit time) and ends where the next version of the same customer starts.
# Snapshot rows are the first versions. A delete (`op = 'd'`) ends the last version and starts nothing.

# %%
con.sql("""
    CREATE OR REPLACE VIEW versions AS
    SELECT customer_id, 0 AS lsn, false AS is_delete, city, district, mobile, is_deleted,
           updated_at::TIMESTAMP AS valid_from
    FROM read_csv('""" + RAW + """/cdc/customers_snapshot_2026-09-01.csv', types = {'mobile': 'VARCHAR'})
    UNION ALL
    SELECT coalesce(after.customer_id, before.customer_id), lsn, op = 'd', after.city, after.district, after.mobile,
           after.is_deleted, make_timestamp(ts_ms * 1000)
    FROM (SELECT DISTINCT ON (lsn) * FROM changes)
""")
con.sql("SELECT * FROM versions WHERE customer_id = (SELECT after.customer_id FROM changes WHERE op = 'u' LIMIT 1) ORDER BY lsn").df()

# %% [markdown]
# ✏️ **Exercise** · Create the table `dim_customer_scd2` from `versions`: add `valid_to` (the next version's `valid_from`
# for the same customer, ordered by `lsn`; `NULL` for the last one) and `is_current` (`valid_to IS NULL`), then drop the
# delete markers (`is_delete`). Ordering by `lsn` handles out-of-order events for free.

# %%
# BEGIN SOLUTION
con.sql("""
    CREATE OR REPLACE TABLE dim_customer_scd2 AS
    SELECT customer_id, city, district, mobile, is_deleted, valid_from,
           lead(valid_from) OVER w AS valid_to,
           lead(valid_from) OVER w IS NULL AS is_current
    FROM versions
    WINDOW w AS (PARTITION BY customer_id ORDER BY lsn)
    QUALIFY NOT is_delete      -- filters after the window is computed, so deletes still end the previous version
""")
# END SOLUTION
stats = con.sql("""
    SELECT count(*) AS versions, count(DISTINCT customer_id) AS customers,
           count(*) FILTER (WHERE is_current) AS current_rows FROM dim_customer_scd2""").df()
stats

# %%
erased = con.sql("SELECT count(*) FROM changes WHERE op = 'd'").fetchone()[0]
U.check(stats.current_rows[0] == n_src, "one current version per live customer")
U.check(stats.versions[0] > stats.customers[0], "changed customers have several versions")
print(f"{erased} customers were erased: their versions are closed, but still here. Is that allowed? (see below)")

# %% [markdown]
# 🔍 The erased customers asked Qafila to destroy their personal data, a right the PDPL gives every data subject. Closing their last
# version is **not enough**: their name, mobile and national ID still sit in older versions. For erasure, delete **every**
# version of the customer, and in bronze, and in every snapshot (Lab 4.3 does this on a lakehouse table).

# %%
con.sql("""
    DELETE FROM dim_customer_scd2
    WHERE customer_id IN (SELECT before.customer_id FROM changes WHERE op = 'd')
""")
U.check(con.sql("SELECT count(*) FROM dim_customer_scd2 WHERE NOT is_current").fetchone()[0] > 0, "history kept for everyone else")

# %% [markdown]
# ## 5 · As-of join: revenue by customer city at order time (3')
#
# Some customers moved city during September. Attribute September revenue to the city the customer lived in **at order
# time** (as-of join), and compare with the naive join on the current city.
#
# ✏️ **Exercise** · Complete `by_city`: columns `city`, `revenue_at_order_time` (as-of join on the validity interval)
# and `revenue_current_city` (join on `is_current`). Orders are UTC strings: cast `created_at` to `TIMESTAMP`.

# %%
con.sql("""
    CREATE OR REPLACE VIEW sept_orders AS
    SELECT o.order_id, o.customer_id, o.created_at::TIMESTAMP AS created_at,
           sum(i.quantity * i.unit_price - i.discount) AS revenue
    FROM app.orders o JOIN app.order_items i USING (order_id)
    WHERE o.status = 'delivered' AND o.created_at >= '2026-09-01'
    GROUP BY ALL
""")
# BEGIN SOLUTION
by_city = con.sql("""
    WITH at_order AS (
        SELECT d.city, sum(o.revenue) AS revenue_at_order_time
        FROM sept_orders o JOIN dim_customer_scd2 d
          ON o.customer_id = d.customer_id
         AND o.created_at >= d.valid_from AND (o.created_at < d.valid_to OR d.valid_to IS NULL)
        GROUP BY ALL
    ), cur AS (
        SELECT d.city, sum(o.revenue) AS revenue_current_city
        FROM sept_orders o JOIN dim_customer_scd2 d ON o.customer_id = d.customer_id AND d.is_current
        GROUP BY ALL
    )
    SELECT city, round(revenue_at_order_time) AS revenue_at_order_time, round(revenue_current_city) AS revenue_current_city
    FROM at_order JOIN cur USING (city) ORDER BY city
""").df()
# END SOLUTION
by_city["difference"] = by_city.revenue_current_city - by_city.revenue_at_order_time
by_city

# %%
U.check(by_city.difference.abs().sum() > 0, "moving customers shift revenue between cities")

# %% [markdown]
# 🔍 The totals differ by a few percent at most, but for a city manager whose bonus depends on it, "a few percent" is the
# whole conversation. Which one is right depends on the question: **as-of** for "where did we sell", **current** for
# "where are our customers now". Also note: orders of erased customers fall out of both joins. Day 2's star schema keeps
# them with an "unknown customer" row.
#
# ## ✱ Stretch · Make the MERGE incremental: process the feed in 4 batches
#
# In production the feed arrives continuously: you merge **batches**, and an old event can arrive in a later batch than a
# newer event for the same key. Keep the last applied `lsn` per row (`_lsn`) and only apply an event if it's newer.
# Process `changes` in 4 batches by `arrival` and verify the result still reconciles with the source.

# %%
# BEGIN SOLUTION
con.sql(f"""
    CREATE OR REPLACE TABLE customers_inc AS
    SELECT *, 0::BIGINT AS _lsn FROM read_csv('{RAW}/cdc/customers_snapshot_2026-09-01.csv',
        types = {{'mobile': 'VARCHAR', 'national_id': 'VARCHAR', 'iban': 'VARCHAR', 'created_at': 'VARCHAR', 'updated_at': 'VARCHAR'}})
""")
edges = [round(total * k / 4) for k in range(5)]
for lo, hi in zip(edges, edges[1:]):
    con.sql(f"""
        MERGE INTO customers_inc t
        USING (
            SELECT coalesce(after.customer_id, before.customer_id) AS customer_id, op, lsn AS _lsn, after.* EXCLUDE (customer_id)
            FROM (SELECT DISTINCT ON (lsn) * FROM changes WHERE arrival > {lo} AND arrival <= {hi})
            QUALIFY row_number() OVER (PARTITION BY coalesce(after.customer_id, before.customer_id) ORDER BY lsn DESC) = 1
        ) s USING (customer_id)
        WHEN MATCHED AND s._lsn <= t._lsn THEN DO NOTHING
        WHEN MATCHED AND s.op = 'd' THEN DELETE
        WHEN MATCHED THEN UPDATE SET full_name = s.full_name, email = s.email, mobile = s.mobile, national_id = s.national_id,
            iban = s.iban, city = s.city, district = s.district, created_at = s.created_at, updated_at = s.updated_at,
            is_deleted = s.is_deleted, _lsn = s._lsn
        WHEN NOT MATCHED AND s.op <> 'd' THEN INSERT (customer_id, full_name, email, mobile, national_id, iban, city, district,
            created_at, updated_at, is_deleted, _lsn)
            VALUES (s.customer_id, s.full_name, s.email, s.mobile, s.national_id, s.iban, s.city, s.district,
                    s.created_at, s.updated_at, s.is_deleted, s._lsn)
    """)
diff = con.sql("""SELECT count(*) FROM (
    (SELECT * FROM app.customers EXCEPT SELECT * EXCLUDE (_lsn) FROM customers_inc)
    UNION ALL (SELECT * EXCLUDE (_lsn) FROM customers_inc EXCEPT SELECT * FROM app.customers))""").fetchone()[0]
print("differences after 4 batches:", diff)
# END SOLUTION

# %% [markdown]
# 🔍 If your batched result has a few differences, look for customers **deleted** in an early batch whose older update
# arrives in a later batch: with the row gone there is no `_lsn` to compare against, so the old update re-inserts it.
# Real CDC sinks keep **tombstones** (a deleted marker with its `lsn`) for a while for exactly this reason.
#
# ## Checkpoint
#
# 1. Why order by `lsn` and not by `ts_ms`? When could two events share a `ts_ms`?
# 2. Your MERGE runs twice on the same batch. What happens, and why is that fine?
# 3. Which Qafila attributes deserve SCD2 history, and which are fine as Type 1 (overwrite)?
# 4. An erasure request arrives. List every place in today's pipeline where that customer's data now lives.
