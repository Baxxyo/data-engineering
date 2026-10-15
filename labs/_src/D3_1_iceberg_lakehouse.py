# %% [markdown]
# # Lab 3.1 · An Iceberg lakehouse with PyIceberg
#
# Day 3 · Session 1 (Open table formats & the lakehouse) · 25 min. What you'll build:
# - a local **Apache Iceberg** catalog (SQLite for the catalog, a folder standing in for object storage);
# - a partitioned `qafila.orders` table, loaded in batches, with its metadata tree on disk;
# - updates, deletes and **time travel**; schema and **partition evolution** without rewriting data;
# - the same table read by three engines: PyIceberg, DuckDB and Polars.
#
# **Tasks**
# - [ ] 1 · Create a catalog, a namespace and a partitioned table (5')
# - [ ] 2 · Load two batches; walk the metadata tree on disk (5')
# - [ ] 3 · Upsert, delete, time travel (6')
# - [ ] 4 · Evolve the schema and the partitioning without rewriting data (5')
# - [ ] 5 · One table, many engines: DuckDB and Polars (4')
# - [ ] ✱ Stretch · Expire old snapshots and see which files go away

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

import duckdb
import pandas as pd
import pyarrow as pa
from pyiceberg.transforms import DayTransform, MonthTransform
from pyiceberg.types import StringType

pd.set_option("display.max_colwidth", 70)
LAKE = U.lab_dir("D3_1", reset=True) / "lakehouse"      # our "object store"
con = duckdb.connect()
con.sql("SET enable_progress_bar = false")
U.attach_app(con)


def orders_between(start, end):
    """Orders created in [start, end) from the app database, as an Arrow table."""
    return con.sql(f"""
        SELECT order_id, customer_id, store_id, status, payment_method, delivery_fee,
               created_at::TIMESTAMP AS created_at, updated_at::TIMESTAMP AS updated_at
        FROM app.orders WHERE created_at >= '{start}' AND created_at < '{end}' ORDER BY order_id
    """).to_arrow_table()

batch1 = orders_between("2026-08-01", "2026-08-04")
batch2 = orders_between("2026-08-04", "2026-08-08")
print(batch1.num_rows, "+", batch2.num_rows, "orders |", batch1.schema)

# %% [markdown]
# ## 1 · Create a catalog, a namespace and a partitioned table (5')
#
# The **catalog** maps table names to their current metadata file. Here it's a SQLite file (`SqlCatalog`); in production
# it's a REST catalog (Apache Polaris, Unity Catalog, AWS Glue, Nessie, Lakekeeper...). The **warehouse** is where data
# and metadata files go: a local folder here, S3/ADLS/GCS in production.

# %%
catalog = U.get_catalog(LAKE)
catalog.create_namespace("qafila")
print(catalog.list_namespaces())

# %% [markdown]
# ✏️ **Exercise** · Create the table `qafila.orders` with `batch1.schema`, then add a partition field: **day** of `created_at`,
# named `created_day` (use `table.update_spec()` as a context manager and `add_field(source_column, DayTransform(), name)`).
#
# Iceberg partitioning is **hidden**: the partition value is derived from `created_at`. Queries filter on `created_at`
# and never need to know a `created_day` column exists.

# %%
# BEGIN SOLUTION
orders = catalog.create_table("qafila.orders", schema=batch1.schema)
with orders.update_spec() as spec:
    spec.add_field("created_at", DayTransform(), "created_day")
# END SOLUTION
print(orders.spec())
print(orders.schema())

# %%
U.check(len(orders.spec().fields) == 1 and orders.spec().fields[0].name == "created_day", "partitioned by day(created_at)")

# %% [markdown]
# ## 2 · Load two batches; walk the metadata tree on disk (5')
#
# Every write creates a new **snapshot**: an immutable, complete view of the table at that moment. Writers never modify
# existing files; they add files and atomically swap the catalog's pointer to a new metadata file.

# %%
orders.append(batch1)
# ✏️ Exercise: append the second batch, then build `snaps` = orders.inspect.snapshots() as pandas (columns snapshot_id, operation, summary)
# BEGIN SOLUTION
orders.append(batch2)
snaps = orders.inspect.snapshots().to_pandas()[["committed_at", "snapshot_id", "operation", "summary"]]
# END SOLUTION
snaps

# %%
U.check(len(snaps) == 2, "two appends -> two snapshots")
U.check(orders.scan().to_arrow().num_rows == batch1.num_rows + batch2.num_rows, "every order is in the table")

# %% [markdown]
# Now the files. Under `qafila/orders/` there are two folders: `data/` (Parquet, one folder per partition value) and
# `metadata/`: `*.metadata.json` (one per commit), `snap-*.avro` (**manifest lists**, one per snapshot) and `*-m0.avro`
# (**manifests**, listing data files with per-column statistics).

# %%
table_dir = LAKE / "qafila" / "orders"
for p in sorted(table_dir.rglob("*")):
    if p.is_file():
        print(f"{str(p.relative_to(table_dir)):95s} {p.stat().st_size / 1e3:7.1f} kB")

# %%
meta = json.loads(Path(orders.metadata_location).read_text())
print("format-version:", meta["format-version"])
print("current-snapshot-id:", meta["current-snapshot-id"])
print("snapshots:", [(s["snapshot-id"], s["summary"]["operation"], s["manifest-list"].split("/")[-1]) for s in meta["snapshots"]])

# %% [markdown]
# 🔍 The tree, top-down: **catalog** → current `metadata.json` (schemas, partition specs, snapshots) → **manifest list** of
# the current snapshot → **manifests** → **data files**. A reader never lists directories (slow on object storage): it
# follows pointers, and uses the statistics in manifests to skip whole files, like Parquet footers but one level up.
# `inspect.files()` shows what the manifests know:

# %%
orders.inspect.files().to_pandas()[["file_path", "partition", "record_count", "file_size_in_bytes"]].assign(
    file_path=lambda d: d.file_path.str.split("/").str[-2:].str.join("/"))

# %% [markdown]
# ## 3 · Upsert, delete, time travel (6')
#
# Plain Parquet files can't be updated in place. Table formats can: they write new files and record which old files
# (or which rows in them) no longer count. Two real changes for Qafila:
# - five delivered orders from 1 August were **refunded** later: upsert their new status;
# - one customer exercised the **right to erasure**: delete their orders.

# %%
refunds = batch1.filter(pa.compute.is_in(batch1["order_id"], pa.array(batch1["order_id"].to_pylist()[:5])))
refunds = refunds.set_column(refunds.schema.get_field_index("status"), "status", pa.array(["refunded"] * 5))
result = orders.upsert(refunds, join_cols=["order_id"])
print(result)
erased_customer = batch1["customer_id"][7].as_py()
print("erasing orders of customer", erased_customer)

# %% [markdown]
# ✏️ **Exercise** · Delete every order of `erased_customer` (`orders.delete(delete_filter=...)` takes a string
# expression like `"customer_id = 123"`). Then, for **each snapshot** in `orders.snapshots()`, count the rows and the
# refunded orders by scanning **as of that snapshot** (`orders.scan(snapshot_id=...)`). Build `history` as a DataFrame
# with `snapshot_id`, `operation`, `rows`, `refunded`.

# %%
# BEGIN SOLUTION
orders.delete(delete_filter=f"customer_id = {erased_customer}")
rows = []
for s in orders.snapshots():
    t = orders.scan(snapshot_id=s.snapshot_id).to_arrow()
    rows.append({"snapshot_id": s.snapshot_id, "operation": s.summary.operation.value, "rows": t.num_rows,
                 "refunded": pa.compute.sum(pa.compute.equal(t["status"], "refunded")).as_py() or 0})
history = pd.DataFrame(rows)
# END SOLUTION
history

# %%
U.check(history.rows.iloc[-1] < history.rows.iloc[1], "the delete removed rows from the current snapshot")
U.check(history.refunded.iloc[1] < history.refunded.iloc[-1], "the upsert changed statuses")
U.check(history.rows.iloc[1] == batch1.num_rows + batch2.num_rows, "time travel still sees the old version")

# %% [markdown]
# 🔍 Time travel is a superpower for debugging ("what did the table look like when the dashboard broke?") and for
# reproducible ML training sets. But notice the PDPL problem: the erased customer's orders are **still readable in old
# snapshots**, and their Parquet files still sit on disk. True erasure needs **snapshot expiry** too (✱ Stretch, Lab 4.3).
#
# ## 4 · Evolve the schema and the partitioning without rewriting data (5')
#
# Two new requirements. The app now records the sales `channel` (`app` / `web`). And daily partitions turned out to be
# too small (many tiny files per store-day), so the team switches to **monthly** partitioning from September on.
#
# ✏️ **Exercise** · (1) Add an optional string column `channel` (`orders.update_schema()` → `add_column`). (2) Change the
# partition spec: remove `created_day` and add `month(created_at)` named `created_month`. (3) Append September's first week
# with a `channel` value of `"app"`.

# %%
# BEGIN SOLUTION
with orders.update_schema() as upd:
    upd.add_column("channel", StringType())
with orders.update_spec() as spec:
    spec.remove_field("created_day")
    spec.add_field("created_at", MonthTransform(), "created_month")
sept = orders_between("2026-09-01", "2026-09-08")
sept = sept.append_column("channel", pa.array(["app"] * sept.num_rows, pa.string()))
orders.append(sept)
# END SOLUTION
print(orders.schema())
print(orders.specs())

# %%
files = orders.inspect.files().to_pandas()
files["folder"] = files.file_path.str.split("/").str[-2]
files.groupby("spec_id").agg(files=("folder", "size"), rows=("record_count", "sum"), folders=("folder", lambda f: sorted(set(f))))

# %%
U.check("channel" in orders.schema().column_names, "column added without rewriting a single data file")
U.check(set(files.spec_id) == {1, 2}, "files written with two different partition specs live side by side")
U.check(orders.scan(row_filter="created_at >= '2026-09-01T00:00:00'").to_arrow().num_rows == sept.num_rows, "queries don't care which spec a file used")

# %% [markdown]
# 🔍 Old files keep their old layout (daily) and old rows read `channel` as `NULL`; new files use the new layout. Readers
# plan each file with the spec it was written with. In a Hive-style table both changes would mean rewriting everything.
#
# ## 5 · One table, many engines: DuckDB and Polars (4')
#
# The point of an **open** table format: any engine that speaks Iceberg can read (and often write) the same table, with the
# same snapshots. DuckDB reads it through its `iceberg` extension, pointed at the current metadata file.

# %%
U.duckdb_iceberg(con)
meta_path = orders.metadata_location
con.sql(f"SELECT status, count(*) AS orders FROM iceberg_scan('{meta_path}') GROUP BY ALL ORDER BY orders DESC").df()

# %% [markdown]
# ✏️ **Exercise** · Compute orders per `store_id` for August **with DuckDB** (`iceberg_scan`) into `by_store_duck`, and the same
# **with Polars** (`pl.scan_iceberg(orders)`) into `by_store_pl`, both sorted by `store_id`, columns `store_id`, `orders`.

# %%
import polars as pl

# BEGIN SOLUTION
by_store_duck = con.sql(f"""
    SELECT store_id, count(*) AS orders FROM iceberg_scan('{meta_path}')
    WHERE created_at < '2026-09-01' GROUP BY store_id ORDER BY store_id
""").pl()
by_store_pl = (pl.scan_iceberg(orders)
                 .filter(pl.col("created_at") < pl.datetime(2026, 9, 1))
                 .group_by("store_id").agg(pl.len().alias("orders"))
                 .sort("store_id").collect())
# END SOLUTION
U.check(by_store_duck["orders"].to_list() == by_store_pl["orders"].to_list(), "DuckDB and Polars read the same table state")
by_store_duck.head()

# %% [markdown]
# 🔍 Three engines, one copy of the data, one source of truth for "what's in the table right now". That's the lakehouse
# promise. In production a Spark job might write the table, Trino or DuckDB serve ad-hoc queries, and Snowflake read it
# through a catalog integration, all without copies.
#
# ## ✱ Stretch · Expire old snapshots and see which files go away
#
# Expire every snapshot except the current one, then compare the files on disk and try time travel again.

# %%
# BEGIN SOLUTION
before_files = {p for p in table_dir.rglob("*.parquet")}
current = orders.current_snapshot().snapshot_id
old_ids = [s.snapshot_id for s in orders.snapshots() if s.snapshot_id != current]
orders.maintenance.expire_snapshots().by_ids(old_ids).commit()
orders.refresh()
after_files = {p for p in table_dir.rglob("*.parquet")}
print("snapshots left:", [s.snapshot_id for s in orders.snapshots()], "| current kept:", current)
print(f"parquet files on disk: {len(before_files)} -> {len(after_files)}")
still_referenced = {Path(p) for p in orders.inspect.all_files().to_pandas().file_path}
print("unreferenced data files still on disk:", len(after_files - still_referenced))
# END SOLUTION

# %% [markdown]
# 🔍 Expiry removes snapshots from the **metadata**, so time travel to them is gone. Whether the now-unreferenced data files
# are physically deleted depends on the engine (PyIceberg leaves them; Spark's `expire_snapshots` deletes them; a separate
# "remove orphan files" job cleans up the rest). For erasure, you need both steps, and to check.
#
# ## Checkpoint
#
# 1. Why can't you safely `UPDATE` rows in a folder of plain Parquet files that other jobs are reading?
# 2. What does the catalog store, and why is it the single point every engine must agree on?
# 3. Hidden partitioning: what problem from Lab 1.2 does it solve?
# 4. Time travel and the right to erasure pull in opposite directions. How would you reconcile them at Qafila?
