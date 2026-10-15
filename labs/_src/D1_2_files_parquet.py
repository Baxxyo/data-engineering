# %% [markdown]
# # Lab 1.2 · Parquet in practice
#
# Day 1 · Session 2 (Files & columnar storage) · 25 min. What you'll do:
# - store the same few million Qafila order lines as CSV and as Parquet (Snappy, ZSTD) and measure size and speed;
# - open a Parquet footer and read its row groups, statistics and encodings;
# - see why **sorting** and **partitioning** let an engine skip most of a file, and how over-partitioning backfires.
#
# **Tasks**
# - [ ] 1 · CSV vs. Parquet (Snappy, ZSTD): size and scan time (6')
# - [ ] 2 · Read the footer: row groups, statistics, encodings (5')
# - [ ] 3 · Sorted vs. unsorted: count row groups skipped (5')
# - [ ] 4 · Partition by date; query one day (5')
# - [ ] 5 · Over-partition on purpose; measure the cost (4')
# - [ ] ✱ Stretch · Same queries in Polars lazy mode; compare plans

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

con = duckdb.connect()
con.sql("SET enable_progress_bar = false")
U.attach_app(con)
OUT = U.lab_dir("D1_2", reset=True)      # artifacts/D1_2: everything this lab writes
O = OUT.as_posix()

# %% [markdown]
# Qafila has ~173k order lines. Files behave differently at scale, so we simulate a bigger Qafila by replicating the lines
# `SCALE` times (with new order IDs) and **shuffling** them, as data often arrives in no useful order.
# One wide row per order line, the way analysts like it:

# %%
SCALE = U.pick(20, 3)
con.sql(f"""
    CREATE TABLE lines AS
    WITH base AS (
        SELECT o.order_id, i.line_no, o.created_at::TIMESTAMP AS created_at, o.store_id, s.city, o.status,
               o.payment_method, p.category, p.name_en AS product, i.quantity, i.unit_price, i.discount,
               round(i.quantity * i.unit_price - i.discount, 2) AS amount
        FROM app.orders o
        JOIN app.order_items i USING (order_id)
        JOIN app.products p USING (product_id)
        JOIN app.stores s USING (store_id)
    )
    SELECT * EXCLUDE (k) REPLACE (order_id + k * 10000000 AS order_id)
    FROM base, range({SCALE}) r(k)
    ORDER BY hash(order_id + k, line_no)        -- deterministic shuffle
""")
n_rows = con.sql("SELECT count(*) FROM lines").fetchone()[0]
print(f"{n_rows:,} rows x {len(con.table('lines').columns)} columns")

# %% [markdown]
# ## 1 · CSV vs. Parquet (Snappy, ZSTD): size and scan time (6')
#
# DuckDB's `COPY ... TO` writes any format. CSV first, then Parquet with the Snappy codec:

# %%
with U.timer("write CSV"):
    con.sql(f"COPY lines TO '{O}/lines.csv' (HEADER)")
with U.timer("write Parquet (snappy)"):
    con.sql(f"COPY lines TO '{O}/lines_snappy.parquet' (FORMAT parquet, COMPRESSION snappy)")

# %% [markdown]
# ✏️ **Exercise** · Write the same table to `lines_zstd.parquet` with **ZSTD** compression.

# %%
with U.timer("write Parquet (zstd)"):
    # BEGIN SOLUTION
    con.sql(f"COPY lines TO '{O}/lines_zstd.parquet' (FORMAT parquet, COMPRESSION zstd)")
    # END SOLUTION

# %% [markdown]
# Now the same analytical query on each file: revenue by city for delivered orders. It needs 3 of 13 columns.

# %%
QUERY = "SELECT city, round(sum(amount)) AS revenue FROM {src} WHERE status = 'delivered' GROUP BY city ORDER BY city"
files = {"CSV": f"read_csv('{O}/lines.csv')",
         "Parquet snappy": f"'{O}/lines_snappy.parquet'",
         "Parquet zstd": f"'{O}/lines_zstd.parquet'"}
rows = []
for name, src in files.items():
    path = OUT / {"CSV": "lines.csv", "Parquet snappy": "lines_snappy.parquet", "Parquet zstd": "lines_zstd.parquet"}[name]
    secs = U.best_of(lambda: con.sql(QUERY.format(src=src)).fetchall())
    rows.append({"format": name, "size_mb": round(U.size_mb(path), 1), "query_ms": round(secs * 1000)})
results = pd.DataFrame(rows)
results

# %%
assert con.sql(QUERY.format(src=files["CSV"])).fetchall() == con.sql(QUERY.format(src=files["Parquet zstd"])).fetchall()
csv_mb, zstd_mb = results.size_mb.iloc[0], results.size_mb.iloc[2]
U.check(zstd_mb < csv_mb / 3, f"ZSTD Parquet is {csv_mb / zstd_mb:.1f}x smaller than CSV")

# %% [markdown]
# 🔍 **Look at** both columns. Parquet is several times smaller *and* faster: the engine reads only the three columns it
# needs, already typed, while the CSV must be parsed in full, every time. ZSTD is usually a little smaller than Snappy and,
# on a laptop SSD, about as fast. On object storage (S3), where you pay per byte transferred, smaller files often win outright.
#
# ## 2 · Read the footer: row groups, statistics, encodings (5')
#
# Every Parquet file ends with a footer that describes the file. `parquet_metadata()` returns one row per
# **column chunk** (= column × row group).

# %%
meta = con.sql(f"SELECT * FROM parquet_metadata('{O}/lines_zstd.parquet')").df()
print(meta.columns.tolist())
meta[["row_group_id", "row_group_num_rows", "path_in_schema", "type", "stats_min", "stats_max",
      "compression", "encodings", "total_compressed_size", "total_uncompressed_size"]].head(13)

# %% [markdown]
# ✏️ **Exercise** · From `meta`, compute:
# - `n_row_groups`: how many row groups the file has;
# - `col_bytes`: a pandas Series with the total **compressed** bytes per column (`path_in_schema`), largest first.

# %%
# BEGIN SOLUTION
n_row_groups = meta.row_group_id.nunique()
col_bytes = meta.groupby("path_in_schema").total_compressed_size.sum().sort_values(ascending=False)
# END SOLUTION
print(f"{n_row_groups} row groups of up to {meta.row_group_num_rows.max():,} rows")
print((col_bytes / n_rows).round(2).rename("bytes per row").to_string())
U.check(n_row_groups >= 2, "the file has several row groups")
U.check(col_bytes["order_id"] > 5 * col_bytes["store_id"], "store_id costs a fraction of order_id")

# %% [markdown]
# 🔍 `store_id` has 12 distinct values in millions of rows. **Dictionary encoding** stores each value once and the column
# becomes a run of small integers that compresses to a fraction of a byte per row. Unique, shuffled IDs (`order_id`) and
# high-precision numbers (`amount`) cost several bytes per row. Narrow, repetitive columns are nearly free in Parquet.
#
# Now look at the statistics of `created_at` per row group:

# %%
meta[meta.path_in_schema == "created_at"][["row_group_id", "row_group_num_rows", "stats_min", "stats_max"]].head(8)

# %% [markdown]
# Every row group spans (almost) the whole two months: the rows are shuffled. A filter on one day can't rule out any row group.
#
# ## 3 · Sorted vs. unsorted: count row groups skipped (5')
#
# Write two files with small row groups (100k rows) so the effect is visible: one in the shuffled order, one sorted by `created_at`.

# %%
con.sql(f"COPY lines TO '{O}/unsorted.parquet' (FORMAT parquet, COMPRESSION zstd, ROW_GROUP_SIZE 100000)")
con.sql(f"COPY (FROM lines ORDER BY created_at) TO '{O}/sorted.parquet' (FORMAT parquet, COMPRESSION zstd, ROW_GROUP_SIZE 100000)")
DAY_FROM, DAY_TO = "2026-09-23 00:00:00", "2026-09-24 00:00:00"

# %% [markdown]
# ✏️ **Exercise** · Write `prunable(path)`: using `parquet_metadata`, return `(total_row_groups, row_groups_to_read)` for the filter
# `created_at >= DAY_FROM AND created_at < DAY_TO`. A row group must be read if its `[stats_min, stats_max]` range overlaps
# the filter. (`stats_min`/`stats_max` are strings; ISO timestamps compare correctly as strings.)

# %%
def prunable(path):
    # BEGIN SOLUTION
    m = con.sql(f"""
        SELECT row_group_id, stats_min, stats_max FROM parquet_metadata('{path}')
        WHERE path_in_schema = 'created_at'""").df()
    must_read = ((m.stats_max >= DAY_FROM) & (m.stats_min < DAY_TO)).sum()
    return len(m), int(must_read)
    # END SOLUTION

for name in ["unsorted", "sorted"]:
    total, read = prunable(f"{O}/{name}.parquet")
    q = f"SELECT count(*), sum(amount) FROM '{O}/{name}.parquet' WHERE created_at >= '{DAY_FROM}' AND created_at < '{DAY_TO}'"
    ms = U.best_of(lambda: con.sql(q).fetchall()) * 1000
    print(f"{name:9s} row groups to read: {read:3d} / {total:3d}   query: {ms:6.1f} ms")

# %%
U.check(prunable(f"{O}/sorted.parquet")[1] < prunable(f"{O}/unsorted.parquet")[1] / 5, "sorting lets the engine skip most row groups")

# %% [markdown]
# 🔍 Same data, same query, same answer. Sorted by the filter column, the statistics are tight and nearly every row group
# is skipped without being read. This is why table formats (Day 3) offer **sort orders** and **clustering**.
#
# You can also see it in the plan: `EXPLAIN ANALYZE` shows how many rows the scan actually produced.

# %%
print(con.sql(f"EXPLAIN ANALYZE SELECT sum(amount) FROM '{O}/sorted.parquet' WHERE created_at >= '{DAY_FROM}' AND created_at < '{DAY_TO}'").fetchall()[0][1][-1800:])

# %% [markdown]
# ## 4 · Partition by date; query one day (5')
#
# Partitioning splits the table into folders by a column value (`order_date=2026-09-23/`). A filter on that column
# opens one folder and ignores the rest, even before reading any footer.
#
# ✏️ **Exercise** · Write `lines` to the folder `OUT/by_date`, partitioned by `order_date` (= `created_at::DATE`).

# %%
# BEGIN SOLUTION
con.sql(f"""
    COPY (SELECT *, created_at::DATE AS order_date FROM lines)
    TO '{O}/by_date' (FORMAT parquet, COMPRESSION zstd, PARTITION_BY (order_date))
""")
# END SOLUTION
print(U.count_files(OUT / "by_date"), "files, e.g.", sorted(p.name for p in (OUT / "by_date").iterdir())[:3])

# %%
by_date = f"read_parquet('{O}/by_date/*/*.parquet', hive_partitioning = true)"
one_day = f"SELECT count(*), sum(amount) FROM {by_date} WHERE order_date = '2026-09-23'"
all_days = f"SELECT count(*), sum(amount) FROM {by_date}"
print("one day :", con.sql(one_day).fetchone(), f"{U.best_of(lambda: con.sql(one_day).fetchall()) * 1000:.1f} ms")
print("all days:", con.sql(all_days).fetchone(), f"{U.best_of(lambda: con.sql(all_days).fetchall()) * 1000:.1f} ms")
U.check(U.count_files(OUT / "by_date") == 61, "one folder per day")

# %% [markdown]
# 🔍 The partition column isn't stored inside the files at all: it's in the path (`order_date=2026-09-23`). That's
# **Hive-style partitioning**. Two caveats: the folder date is UTC here (we didn't convert to Riyadh time), and engines
# only prune when you filter on the partition column itself, not on `created_at`.
#
# ## 5 · Over-partition on purpose; measure the cost (4')
#
# Someone suggests partitioning by date **and** store "for faster store reports". Let's measure.
#
# ✏️ **Exercise** · Write `OUT/by_date_store`, partitioned by `order_date` and `store_id`. Then fill `cost` with, for each
# layout (`sorted.parquet`, `by_date`, `by_date_store`): the number of files, total MB, and the best time of a full-table
# aggregate `SELECT store_id, sum(amount) ... GROUP BY store_id`.

# %%
# BEGIN SOLUTION
con.sql(f"""
    COPY (SELECT *, created_at::DATE AS order_date FROM lines)
    TO '{O}/by_date_store' (FORMAT parquet, COMPRESSION zstd, PARTITION_BY (order_date, store_id))
""")
layouts = {"single file": (OUT / "sorted.parquet", f"'{O}/sorted.parquet'"),
           "by_date": (OUT / "by_date", by_date),
           "by_date_store": (OUT / "by_date_store", f"read_parquet('{O}/by_date_store/*/*/*.parquet', hive_partitioning = true)")}
cost = []
for name, (path, src) in layouts.items():
    q = f"SELECT store_id, sum(amount) FROM {src} GROUP BY store_id"
    cost.append({"layout": name, "files": 1 if path.is_file() else U.count_files(path),
                 "size_mb": round(U.size_mb(path), 1), "full_scan_ms": round(U.best_of(lambda: con.sql(q).fetchall()) * 1000)})
cost = pd.DataFrame(cost)
# END SOLUTION
cost

# %%
U.check(cost.files.iloc[2] > 500, f"{cost.files.iloc[2]} files for {cost.size_mb.iloc[2]} MB")
U.check(cost.full_scan_ms.iloc[2] > cost.full_scan_ms.iloc[0], "the over-partitioned layout is the slowest to scan")

# %% [markdown]
# 🔍 Hundreds of small files: each one costs an open, a footer read and scheduling work, and compresses worse. On object
# storage every file is also an HTTP request. Store filters were already fast in one file thanks to dictionary encoding
# and statistics. Partition by what you **load** and **filter** by most (usually a date), keep files large, and use
# sorting for the rest.
#
# ## ✱ Stretch · Same queries in Polars lazy mode; compare plans
#
# Polars builds a **lazy** query plan and optimizes it before reading anything. `explain()` shows the plan:
# look for the projection (columns read) and the filter pushed into the Parquet scan.

# %%
import polars as pl

# BEGIN SOLUTION
plan = (pl.scan_parquet(f"{O}/sorted.parquet")
          .filter(pl.col("status") == "delivered")
          .group_by("city")
          .agg(pl.col("amount").sum().round(0).alias("revenue"))
          .sort("city"))
print(plan.explain())
print(plan.collect())
# END SOLUTION

# %% [markdown]
# ## Checkpoint
#
# 1. Why is Parquet faster than CSV even when both files are on the same SSD?
# 2. Your daily load appends one small Parquet file per run, every 5 minutes. What happens after a year, and what's the fix?
# 3. Which column would you sort Qafila's order lines by, and which would you partition by? Why not the same?
# 4. When would you still deliver a CSV?
