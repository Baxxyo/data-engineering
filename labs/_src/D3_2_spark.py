# %% [markdown]
# # Lab 3.2 · Distributed processing with Spark
#
# Day 3 · Session 2 (Distributed processing with Spark) · 25 min. What you'll do:
# - run **Apache Spark 4** locally (the same engine that runs on 1,000-node clusters);
# - write the same transformation with the **DataFrame API** and with **Spark SQL**, and check it against DuckDB;
# - read physical plans: find the **shuffles**, **broadcast joins** and **partition pruning**;
# - measure **data skew** (hello, National Day) and fix it;
# - write partitioned Parquet output and meet the **small-files problem**.
#
# ⚠️ Spark 4 needs **Java 17+**. On Colab the setup cell installs it (about a minute). Locally: install a JDK (e.g. Temurin 21)
# and `uv sync --extra spark`. Spark on native Windows also needs `winutils.exe` to write files: use Colab or WSL there.
#
# **Tasks**
# - [ ] 1 · Start Spark, read a partitioned dataset, feel lazy evaluation (5')
# - [ ] 2 · Daily net sales per city: DataFrame API vs Spark SQL vs DuckDB (6')
# - [ ] 3 · Read the plan: shuffles and join strategies (5')
# - [ ] 4 · Partitions and skew (5')
# - [ ] 5 · Write partitioned output; count the files (4')
# - [ ] ✱ Stretch · pandas UDF vs built-in function

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

# %% [markdown]
# First, a small "data lake" for Spark to chew on. DuckDB exports the raw tables to Parquet; `orders` is **partitioned by
# day** (Hive-style folders `order_date=2026-09-23/`), just like a nightly ingestion job would land it.

# %%
import duckdb

LAKE = U.lab_dir("D3_2", reset=True)
con = U.connect(U.build_warehouse())
con.sql(f"""COPY (SELECT * EXCLUDE (_loaded_at), created_at[:10]::DATE AS order_date FROM raw.orders)
            TO '{(LAKE / "orders").as_posix()}' (FORMAT parquet, PARTITION_BY (order_date))""")
for t in ["order_items", "stores", "products"]:
    con.sql(f"COPY (SELECT * EXCLUDE (_loaded_at) FROM raw.{t}) TO '{(LAKE / t).as_posix()}.parquet' (FORMAT parquet)")
print("order folders:", len(list((LAKE / "orders").iterdir())), "| files:", U.count_files(LAKE))

# %% [markdown]
# ## 1 · Start Spark, read a partitioned dataset, feel lazy evaluation (5')
#
# `U.get_spark()` builds a **local** `SparkSession`: the driver and executors all live in this one process, using 2 cores.
# On a cluster, only the `.master(...)` changes; your code doesn't.

# %%
from pyspark.sql import functions as F

spark = U.get_spark()
print("Spark", spark.version, "| default parallelism:", spark.sparkContext.defaultParallelism)

orders = spark.read.parquet((LAKE / "orders").as_posix())
items = spark.read.parquet((LAKE / "order_items.parquet").as_posix())
stores = spark.read.parquet((LAKE / "stores.parquet").as_posix())
products = spark.read.parquet((LAKE / "products.parquet").as_posix())
orders.printSchema()
print("orders partitions (tasks):", orders.rdd.getNumPartitions())

# %% [markdown]
# 🔍 Spark discovered `order_date` from the folder names and typed it as a `date`. Nothing has been **read** yet beyond
# the file listing and footers: transformations (`filter`, `select`, `join`, `groupBy`) only build a plan; **actions**
# (`count`, `collect`, `show`, `write`) execute it.
#
# ✏️ **Exercise** · Build `national_day` = delivered orders on 2026-09-23 (a filter on `order_date` and `status`), **without**
# triggering any work. Then run the action `national_day.count()` into `n_national`.

# %%
# BEGIN SOLUTION
national_day = orders.filter((F.col("order_date") == "2026-09-23") & (F.col("status") == "delivered"))
n_national = national_day.count()
# END SOLUTION
print("delivered orders on National Day:", n_national)
national_day.explain()

# %%
expected = con.sql("SELECT count(*) FROM raw.orders WHERE created_at[:10] = '2026-09-23' AND status = 'delivered'").fetchone()[0]
U.check(n_national == expected, "same answer as DuckDB")
U.check("PartitionFilters: [isnotnull(order_date" in national_day._jdf.queryExecution().executedPlan().toString(),
        "partition pruning: Spark only opens one folder")

# %% [markdown]
# 🔍 Look for `PartitionFilters` in the `FileScan` line: Spark lists 61 folders but reads **one**. `PushedFilters` go one
# level further down: into the Parquet reader, which skips row groups using min/max stats (Lab 1.2).
#
# ## 2 · Daily net sales per city: DataFrame API vs Spark SQL vs DuckDB (6')
#
# The metric behind yesterday's `mart_daily_city_sales`: for **delivered** orders, net sales =
# Σ(`quantity × unit_price − discount`) over lines with `quantity > 0`, per `city` and `order_date` (the UTC date here).
#
# ✏️ **Exercise** · Write it twice: (a) `daily_df` with the DataFrame API (`join`, `filter`, `groupBy`, `agg`), (b) `daily_sql`
# with Spark SQL on temp views `orders`, `items`, `stores`. Both: columns `city`, `order_date`, `orders`, `net_sales`
# (rounded to 2 decimals), ordered by `order_date`, `city`.

# %%
for name, df in {"orders": orders, "items": items, "stores": stores, "products": products}.items():
    df.createOrReplaceTempView(name)

# BEGIN SOLUTION
daily_df = (items.filter(F.col("quantity") > 0)
            .join(orders.filter(F.col("status") == "delivered"), "order_id")
            .join(stores, "store_id")
            .groupBy("city", "order_date")
            .agg(F.countDistinct("order_id").alias("orders"),
                 F.round(F.sum(F.col("quantity") * F.col("unit_price") - F.col("discount")), 2).alias("net_sales"))
            .orderBy("order_date", "city"))
daily_sql = spark.sql("""
    SELECT s.city, o.order_date, count(DISTINCT o.order_id) AS orders,
           round(sum(i.quantity * i.unit_price - i.discount), 2) AS net_sales
    FROM items i JOIN orders o USING (order_id) JOIN stores s USING (store_id)
    WHERE o.status = 'delivered' AND i.quantity > 0
    GROUP BY s.city, o.order_date
    ORDER BY o.order_date, s.city
""")
# END SOLUTION
daily_df.show(6)

# %%
a, b = daily_df.toPandas(), daily_sql.toPandas()
duck = con.sql(f"""
    SELECT s.city, o.order_date, count(DISTINCT o.order_id) AS orders,
           round(sum(i.quantity * i.unit_price - i.discount), 2) AS net_sales
    FROM '{(LAKE / "order_items.parquet").as_posix()}' i
    JOIN read_parquet('{(LAKE / "orders").as_posix()}/*/*.parquet', hive_partitioning = true) o USING (order_id)
    JOIN '{(LAKE / "stores.parquet").as_posix()}' s USING (store_id)
    WHERE o.status = 'delivered' AND i.quantity > 0
    GROUP BY ALL ORDER BY o.order_date, s.city
""").df()
U.check(len(a) == len(b) == len(duck) and (a.orders.values == b.orders.values).all(), "DataFrame API == Spark SQL")
U.check(abs(a.net_sales.values - duck.net_sales.values).max() < 0.05, "Spark == DuckDB on the same files")

# %% [markdown]
# 🔍 The DataFrame API and SQL compile to the **same** optimized plan (Catalyst doesn't care how you wrote it). Pick per
# team taste: SQL for analysts and dbt, DataFrames for code reuse and testing.
#
# ## 3 · Read the plan: shuffles and join strategies (5')
#
# In a plan, every `Exchange` is a **shuffle**: rows are re-partitioned across the network by key. Shuffles are where
# Spark jobs spend their time (and fail). `BroadcastExchange` means a small table is copied to every executor instead.

# %%
daily_df.explain()

# %% [markdown]
# ✏️ **Exercise** · `stores` and the filtered `orders` are small, so Spark broadcasts them. Disable broadcasting
# (`spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)`), get the new plan string of `daily_df` into `plan_smj`,
# and time an action on it with `U.best_of(lambda: daily_df.count(), 3)` into `t_smj`. Then restore `"10MB"` and
# time the broadcast version into `t_bhj`.

# %%
# a DataFrame is planned once and remembers its plan; select("*") makes a new one that is planned with the current config
plan = lambda df: df.select("*")._jdf.queryExecution().executedPlan().toString()
# BEGIN SOLUTION
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)
plan_smj = plan(daily_df)
t_smj = U.best_of(lambda: daily_df.count(), 3)
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "10MB")
t_bhj = U.best_of(lambda: daily_df.count(), 3)
# END SOLUTION
print(f"sort-merge join: {t_smj:.2f}s | broadcast hash join: {t_bhj:.2f}s")
print("joins without broadcast:", [l.strip()[3:30] for l in plan_smj.splitlines() if "Join" in l])

# %%
U.check("SortMergeJoin" in plan_smj, "no broadcast -> sort-merge join (both sides shuffled and sorted)")
U.check("BroadcastHashJoin" in plan(daily_df), "broadcast restored")

# %% [markdown]
# 🔍 On 170k rows the difference is small (and the first runs include JIT warm-up). On a cluster with a 2 TB fact table,
# not shuffling the big side is the difference between minutes and hours. Rule of thumb: broadcast dimensions, shuffle facts;
# hint it with `F.broadcast(df)` when Spark's size estimate is wrong.
#
# ## 4 · Partitions and skew (5')
#
# A Spark job is as slow as its **slowest task**. If one key holds far more rows than the others, the task that gets it
# runs long while the rest of the cluster idles. Qafila's obvious hot key: **National Day**.

# %%
lines = items.join(orders.select("order_id", "order_date", "store_id"), "order_id")
by_day = lines.repartition(8, "order_date")
sizes = by_day.groupBy(F.spark_partition_id().alias("partition")).count().orderBy("partition").toPandas()
print(sizes.T)
print(f"skew (max / mean rows per task): {sizes['count'].max() / sizes['count'].mean():.2f}")

# %% [markdown]
# ✏️ **Exercise** · Spread the hot day: add a **salt** column (`F.col("order_id") % 4`) and repartition by
# `order_date` **and** the salt into 8 partitions. Compute the same skew ratio into `skew_salted`. (Aggregations then run in
# two steps: per (day, salt), then per day.)

# %%
# BEGIN SOLUTION
salted = lines.withColumn("salt", F.col("order_id") % 4).repartition(8, "order_date", "salt")
s2 = salted.groupBy(F.spark_partition_id().alias("partition")).count().toPandas()
skew_salted = s2["count"].max() / s2["count"].mean()
# END SOLUTION
print(f"skew after salting: {skew_salted:.2f}")
U.check(skew_salted < sizes["count"].max() / sizes["count"].mean(), "salting evens out the tasks")

# %% [markdown]
# 🔍 With only 61 days hashed into 8 partitions, some partitions get more days than others: that's skew too. In Spark 3+,
# **Adaptive Query Execution** (AQE) fixes much of this at run time: it coalesces tiny shuffle partitions and splits skewed
# ones in joins (`spark.sql.adaptive.skewJoin.enabled`). Salting remains the manual tool for skewed aggregations.
#
# ## 5 · Write partitioned output; count the files (4')
#
# ✏️ **Exercise** · `items_wide` holds every order line with its city, spread over 8 tasks. Write it as Parquet to
# `LAKE / "out_naive"` partitioned by `city`, then to `LAKE / "out_tidy"` after `.repartition("city")` (one task per city →
# one file per folder). Count the files in each.

# %%
items_wide = lines.join(stores, "store_id").repartition(8)
# BEGIN SOLUTION
items_wide.write.mode("overwrite").partitionBy("city").parquet((LAKE / "out_naive").as_posix())
items_wide.repartition("city").write.mode("overwrite").partitionBy("city").parquet((LAKE / "out_tidy").as_posix())
# END SOLUTION
n_naive, n_tidy = U.count_files(LAKE / "out_naive"), U.count_files(LAKE / "out_tidy")
print(f"files: naive {n_naive} vs tidy {n_tidy}")
U.check(n_tidy == 3 and n_naive > n_tidy, "one file per city folder")
con.sql(f"SELECT city, count(*) FROM read_parquet('{(LAKE / 'out_tidy').as_posix()}/*/*.parquet', hive_partitioning = true) GROUP BY ALL").df()

# %% [markdown]
# 🔍 Every task writes one file into **every** partition folder it has rows for: 8 tasks × 3 cities = up to 24 files. At scale
# (1,000 tasks × 365 days) that's the **small-files problem**: slow listings, slow reads, angry object-store bills. Fixes:
# repartition by the partition column before writing, or let the table format compact later (Lab 3.4).
#
# ## ✱ Stretch · pandas UDF vs built-in function
#
# Compute a VAT-inclusive price (15%) two ways on 3 million cached prices and time them. Built-in functions run inside the
# JVM, compiled to bytecode. A **pandas UDF** ships Arrow batches to a Python worker and back. (A plain row-at-a-time
# `@F.udf` pickles every single row: slower still. Avoid it in production code.)

# %%
prices = spark.range(3_000_000).select((F.rand(seed=1) * 100).alias("unit_price")).cache()
prices.count()

# BEGIN SOLUTION
import pandas as pd

@F.pandas_udf("double")
def vat_pandas(p: pd.Series) -> pd.Series:
    return (p * 1.15).round(2)

for label, col in [("built-in", F.round(F.col("unit_price") * 1.15, 2)), ("pandas UDF", vat_pandas("unit_price"))]:
    t = U.best_of(lambda: prices.select(col.alias("v")).agg(F.sum("v")).collect(), 3)
    print(f"{label:10s} {t:.2f}s")
# END SOLUTION

# %% [markdown]
# 🔍 For simple vectorizable math, Arrow-based pandas UDFs get close to built-ins. Use them when no built-in exists (a
# model's `predict`, a custom parser); check `pyspark.sql.functions` first: there are 400+ built-ins.

# %%
spark.stop()

# %% [markdown]
# ## Checkpoint
#
# 1. What's the difference between a transformation and an action? Why does Spark wait?
# 2. Which operations cause a shuffle? How do you spot one in a plan?
# 3. When does Spark pick a broadcast hash join, and when can that go wrong?
# 4. You see one task running 40 minutes while 199 finished in 30 seconds. What's happening and what do you try?
# 5. This lab's data fits on a laptop: DuckDB would do it in one process. When is Spark the right tool anyway?
