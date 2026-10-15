# %% [markdown]
# # Lab 2.1 · Bronze → silver with DuckDB and Polars
#
# Day 2 · Session 1 (Processing engines: DuckDB & Polars) · 25 min. What you'll build:
# - the same transformation in **SQL (DuckDB)** and in **DataFrames (Polars)**, and a feel for when to use which;
# - a silver `delivery_events` table: three timestamp formats parsed, duplicates removed, Riyadh time, ingest delay;
# - a silver `customers` table: E.164 mobiles, normalized Arabic names, duplicate accounts linked to one person;
# - a look at both engines' **query plans**, and silver Parquet files on disk.
#
# **Tasks**
# - [ ] 1 · Same question, two engines (5')
# - [ ] 2 · Parse three timestamp formats into UTC with Polars (6')
# - [ ] 3 · Deduplicate, convert to Riyadh time, measure lateness (5')
# - [ ] 4 · Clean customers in SQL: mobiles, Arabic names, duplicate accounts (5')
# - [ ] 5 · Read the plans; write silver Parquet (4')
# - [ ] ✱ Stretch · Out of core: run a big join with a 100 MB memory limit

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
import polars as pl

pl.Config.set_tbl_rows(8)
pl.Config.set_fmt_str_lengths(40)
U.build_warehouse()                                   # bronze layer: raw.* tables (Day 1's end state)
con = duckdb.connect(str(U.WAREHOUSE_DB), read_only=True)
con.sql("SET enable_progress_bar = false")
OUT = U.lab_dir("D2_1", reset=True)
EVENTS = (U.RAW / "events" / "delivery_events.jsonl").as_posix()
print(con.sql("SELECT table_name FROM information_schema.tables WHERE table_schema = 'raw' ORDER BY 1").fetchall())

# %% [markdown]
# ## 1 · Same question, two engines (5')
#
# *Delivered revenue by store, September only.* In DuckDB it's SQL over the warehouse tables. In Polars it's a chain of
# expressions over **lazy** frames: nothing runs until `.collect()`, so Polars can optimize the whole chain first.

# %%
sql_result = con.sql("""
    SELECT o.store_id, round(sum(i.quantity * i.unit_price - i.discount), 2) AS revenue
    FROM raw.orders o JOIN raw.order_items i USING (order_id)
    WHERE o.status = 'delivered' AND o.created_at >= '2026-09-01'
    GROUP BY o.store_id ORDER BY o.store_id
""").pl()

orders = con.sql("SELECT * FROM raw.orders").pl().lazy()          # hand DuckDB results to Polars (zero-copy Arrow)
items = con.sql("SELECT * FROM raw.order_items").pl().lazy()

# %% [markdown]
# ✏️ **Exercise** · Write the same query with Polars expressions: filter, join, aggregate, sort. Name the result `pl_result`
# (a DataFrame with `store_id`, `revenue`).

# %%
# BEGIN SOLUTION
pl_result = (
    orders.filter((pl.col("status") == "delivered") & (pl.col("created_at") >= "2026-09-01"))
          .join(items, on="order_id")
          .group_by("store_id")
          .agg((pl.col("quantity") * pl.col("unit_price") - pl.col("discount")).sum().round(2).alias("revenue"))
          .sort("store_id")
          .collect()
)
# END SOLUTION
pl_result

# %%
U.check(pl_result.equals(sql_result), "Polars and DuckDB agree, to the halala")

# %% [markdown]
# 🔍 Same engine design underneath (columnar, vectorized, multi-threaded, lazy/optimized), different interface. Use SQL
# when the logic is relational and others must review it (most of a warehouse); use DataFrames when the logic is
# procedural, parametrized or easier to test as Python functions. They interoperate through Apache Arrow at no cost.
#
# ## 2 · Parse three timestamp formats into UTC with Polars (6')
#
# Bronze delivery events, straight from the JSON lines file. `event_time` comes in three shapes (Lab 1.1).

# %%
bronze = pl.scan_ndjson(EVENTS)
bronze.select("event_id", "event_type", "event_time", "ingest_time").head(4).collect()

# %% [markdown]
# ✏️ **Exercise** · Add `event_at_utc` (a UTC `Datetime`): epoch milliseconds (13 digits) via `pl.from_epoch(..., time_unit="ms")`
# made UTC-aware with `.dt.replace_time_zone("UTC")`; everything else (ISO with an offset or `Z`) via
# `.str.to_datetime(time_zone="UTC")`, which converts any offset to UTC. Also parse `ingest_time` into `ingested_at_utc`.

# %%
et = pl.col("event_time")
# BEGIN SOLUTION
parsed = bronze.with_columns(
    pl.when(et.str.contains(r"^\d{13}$"))
      .then(pl.from_epoch(et.cast(pl.Int64, strict=False), time_unit="ms").dt.replace_time_zone("UTC"))
      .otherwise(et.str.to_datetime(time_zone="UTC", strict=False))
      .alias("event_at_utc"),
    pl.col("ingest_time").str.to_datetime(time_zone="UTC").alias("ingested_at_utc"),
)
# END SOLUTION
sample = parsed.select("event_time", "event_at_utc").collect()
sample.group_by(et.str.slice(23).alias("suffix")).agg(pl.len(), pl.col("event_at_utc").first(), et.first())

# %%
U.check(sample["event_at_utc"].null_count() == 0, "every event_time parsed")
U.check(str(sample["event_at_utc"].dtype) == "Datetime(time_unit='us', time_zone='UTC')", "one type: UTC datetime")

# %% [markdown]
# ## 3 · Deduplicate, convert to Riyadh time, measure lateness (5')
#
# ✏️ **Exercise** · Build the lazy frame `silver_events` from `parsed`:
# 1. keep the **first-ingested** copy of each `event_id` (sort by `ingested_at_utc`, then `unique(..., keep="first")`);
# 2. add `event_at_riyadh` (`.dt.convert_time_zone("Asia/Riyadh")`);
# 3. add `ingest_delay_s`: seconds between event time and ingest time (`.dt.total_seconds()`);
# 4. keep the columns `event_id, order_id, store_id, city, courier_id, event_type, event_at_utc, event_at_riyadh, ingested_at_utc, ingest_delay_s`.

# %%
# BEGIN SOLUTION
silver_events = (
    parsed.sort("ingested_at_utc")
          .unique(subset="event_id", keep="first", maintain_order=True)
          .with_columns(
              pl.col("event_at_utc").dt.convert_time_zone("Asia/Riyadh").alias("event_at_riyadh"),
              (pl.col("ingested_at_utc") - pl.col("event_at_utc")).dt.total_seconds().alias("ingest_delay_s"),
          )
          .select("event_id", "order_id", "store_id", "city", "courier_id", "event_type",
                  "event_at_utc", "event_at_riyadh", "ingested_at_utc", "ingest_delay_s")
)
# END SOLUTION
ev = silver_events.collect()
n_bronze = bronze.select(pl.len()).collect().item()
print(f"bronze {n_bronze:,} rows -> silver {ev.height:,} rows ({n_bronze - ev.height} duplicates dropped)")
U.check(ev["event_id"].is_unique().all(), "event_id is unique in silver")

# %% [markdown]
# How late do events arrive? Most within seconds; a few minutes or more when a courier's phone was offline.
# Those late events will matter a lot in tomorrow's streaming labs.

# %%
ev.select(
    (pl.col("ingest_delay_s") <= 10).mean().alias("share_within_10s"),
    (pl.col("ingest_delay_s") > 300).sum().alias("later_than_5min"),
    pl.col("ingest_delay_s").max().alias("max_delay_s"),
)

# %% [markdown]
# ## 4 · Clean customers in SQL: mobiles, Arabic names, duplicate accounts (5')
#
# Back to SQL. Three cleaning rules for `raw.customers`:
# - **mobile → E.164**: strip everything but digits; if what's left matches `(966|0)?5XXXXXXXX`, output `+966` + the last 9
#   digits, else `NULL`;
# - **Arabic name normalization** for matching: remove tatweel `ـ` (U+0640) and diacritics (U+064B–U+0652), and unify
#   `أ إ آ` to `ا` (we keep the original name too: normalization is for matching, not for display);
# - **person_id**: the smallest `customer_id` sharing the same `national_id`, so duplicate accounts roll up to one person.
#
# ✏️ **Exercise** · Write the `SELECT` that produces `silver_customers` with columns `customer_id, person_id, full_name,
# name_normalized, email (lower-cased, trimmed), mobile_e164, city, district, created_at (TIMESTAMP), is_deleted (BOOLEAN)`.

# %%
# BEGIN SOLUTION
silver_customers = con.sql(r"""
    SELECT customer_id,
           min(customer_id) OVER (PARTITION BY national_id)                         AS person_id,
           full_name,
           translate(regexp_replace(full_name, '[\x{0640}\x{064B}-\x{0652}]', '', 'g'), 'أإآ', 'ااا') AS name_normalized,
           lower(trim(email))                                                        AS email,
           CASE WHEN regexp_full_match(regexp_replace(mobile, '[^0-9]', '', 'g'), '(966|0)?5\d{8}')
                THEN '+966' || right(regexp_replace(mobile, '[^0-9]', '', 'g'), 9) END AS mobile_e164,
           city, district,
           created_at::TIMESTAMP                                                     AS created_at,
           is_deleted = 1                                                            AS is_deleted
    FROM raw.customers
""").pl()
# END SOLUTION
silver_customers.filter(pl.col("customer_id") != pl.col("person_id")).head(3)

# %%
U.check(silver_customers["mobile_e164"].null_count() <= 5, f"{silver_customers['mobile_e164'].null_count()} mobiles unrecognisable")
U.check(silver_customers["mobile_e164"].drop_nulls().str.contains(r"^\+9665\d{8}$").all(), "all mobiles in E.164")
dups = silver_customers.filter(pl.col("customer_id") != pl.col("person_id")).height
U.check(dups > 40, f"{dups} duplicate accounts linked to a person")

# %% [markdown]
# Unrecognisable mobiles would not be thrown away: they'd stay `NULL` in the clean column, with the raw value still in
# bronze. Here the rule caught everything, including the three odd values Lab 1.1 flagged as "other":

# %%
con.sql(r"""SELECT mobile AS raw_mobile, '+966' || right(regexp_replace(mobile, '[^0-9]', '', 'g'), 9) AS mobile_e164
            FROM raw.customers
            WHERE NOT regexp_full_match(mobile, '\+9665\d{8}|05\d{8}|9665\d{8}|\+966 5\d \d{3} \d{4}')""").df()

# %% [markdown]
# 🔍 Normalizing to one format (E.164) is what makes joins with other systems (SMS provider, CRM) possible. Matching on
# digits only is more robust than enumerating every format a sign-up form ever produced.
#
# ## 5 · Read the plans; write silver Parquet (4')
#
# Both engines optimize before executing. Polars prints its optimized logical plan; DuckDB prints a physical plan tree.

# %%
print(silver_events.filter(pl.col("event_type") == "delivered").select("order_id", "event_at_utc").explain())

# %%
print(con.sql("EXPLAIN SELECT store_id, count(*) FROM raw.orders WHERE status = 'delivered' GROUP BY ALL").fetchall()[0][1])

# %% [markdown]
# 🔍 In the Polars plan, look for `PROJECT 5/10 COLUMNS` in the scan: the optimizer pushed the column selection all the way
# into the JSON reader. The filter on `event_type`, though, stays **above** the `UNIQUE`: removing rows before
# deduplication could change which copy is kept, so the optimizer is not allowed to push it down. Optimizers are clever,
# not magic.
#
# ✏️ **Exercise** · Write both silver tables as ZSTD Parquet into `OUT/silver/`: `delivery_events.parquet` (from the lazy
# frame, with `sink_parquet`, which streams without collecting first) and `customers.parquet`.

# %%
(OUT / "silver").mkdir(exist_ok=True)
# BEGIN SOLUTION
silver_events.sink_parquet(OUT / "silver" / "delivery_events.parquet", compression="zstd")
silver_customers.write_parquet(OUT / "silver" / "customers.parquet", compression="zstd")
# END SOLUTION
for f in sorted((OUT / "silver").glob("*.parquet")):
    print(f"{f.name:28s} {U.size_mb(f):5.2f} MB  {pl.scan_parquet(f).select(pl.len()).collect().item():,} rows")

# %%
check = duckdb.sql(f"SELECT count(*), count(DISTINCT event_id) FROM '{(OUT / 'silver' / 'delivery_events.parquet').as_posix()}'").fetchone()
U.check(check[0] == check[1] == ev.height, "silver events readable by another engine, still unique")

# %% [markdown]
# ## ✱ Stretch · Out of core: run a big join with a 100 MB memory limit
#
# DuckDB processes data larger than memory by spilling intermediate results to disk. Cap it at 100 MB and run a
# join + aggregation over a 20× replicated order-lines table. Compare the timing with no limit.

# %%
# BEGIN SOLUTION
big = duckdb.connect()
big.sql(f"ATTACH '{U.WAREHOUSE_DB.as_posix()}' AS wh (READ_ONLY)")
big.sql(f"SET temp_directory = '{(OUT / 'spill').as_posix()}'")
big.sql("""CREATE TABLE lines AS SELECT i.*, k FROM wh.raw.order_items i, range(20) r(k)""")
QUERY = """SELECT o.store_id, l.k, sum(l.quantity * l.unit_price) AS revenue
           FROM lines l JOIN wh.raw.orders o USING (order_id) GROUP BY ALL"""
for limit in ["8GB", "100MB"]:
    big.sql(f"SET memory_limit = '{limit}'")
    print(f"memory_limit {limit:>6}: {U.best_of(lambda: big.sql(QUERY).fetchall(), n=1):.2f} s")
# END SOLUTION

# %% [markdown]
# ## Checkpoint
#
# 1. Which of today's cleaning rules belong in silver, and which (if any) in gold? Why keep the raw value in bronze?
# 2. Why did the filter on `event_type` stay above `UNIQUE` in the Polars plan? When is that a performance problem?
# 3. Riyadh has no daylight saving time. Why still store UTC and convert, instead of storing local time?
# 4. A teammate wants to rewrite the SQL cleaning of customers in pandas "because Python is easier to test". Your advice?
