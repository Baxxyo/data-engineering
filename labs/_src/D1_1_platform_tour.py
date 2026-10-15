# %% [markdown]
# # Lab 1.1 · Platform tour with DuckDB
#
# Day 1 · Session 1 (The modern data platform) · 25 min. What you'll do:
# - query every Qafila source **in place** (SQLite, JSON lines, CSV) with one engine, without loading anything first;
# - profile the app database: volumes, keys, relationships, formats;
# - start the **issue log**: the list of data problems the rest of the week fixes.
#
# **Tasks**
# - [ ] 1 · Query CSV, JSON lines and SQLite in place (5')
# - [ ] 2 · SUMMARIZE the orders; daily volume chart (5')
# - [ ] 3 · Check keys: duplicate customers, orphan orders (5')
# - [ ] 4 · Inspect formats: mobiles, timestamps, prices (5')
# - [ ] 5 · Write the issue log as a table (5')
# - [ ] ✱ Stretch · Find the National Day spike and explain it
#
# Please don't open `data/raw/DATA_ISSUES.txt` yet: finding the issues is the exercise.

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
import matplotlib.pyplot as plt
import pandas as pd

pd.set_option("display.max_colwidth", 80)
con = duckdb.connect()          # in-memory database: nothing is copied unless we ask
RAW = U.RAW.as_posix()          # DuckDB likes forward slashes, also on Windows

# %% [markdown]
# ## 1 · Query CSV, JSON lines and SQLite in place (5')
#
# Qafila's data lives in several places (see the table on the course slides). DuckDB reads each of them where it is:
#
# | Source | Path | DuckDB reader |
# |---|---|---|
# | App database | `data/raw/qafila_oltp.sqlite` | `ATTACH ... (TYPE sqlite)` |
# | Payments API dump | `data/raw/api/payments.jsonl` | `read_json_auto` |
# | Delivery events | `data/raw/events/delivery_events.jsonl` | `read_json_auto` |
# | Customer CDC | `data/raw/cdc/customers_changes.jsonl` | `read_json_auto` |
# | Supplier prices | `data/raw/suppliers/prices_2026-W*.csv` | `read_csv` |
# | Reviews | `data/raw/reviews.csv` | `read_csv` |
#
# A raw file always deserves a look with your own eyes first:

# %%
U.peek(f"{RAW}/events/delivery_events.jsonl", 2)
print()
U.peek(f"{RAW}/reviews.csv", 3)

# %%
# CSV: the header and types are inferred (sniffed) from the file
con.sql(f"SELECT * FROM read_csv('{RAW}/reviews.csv') LIMIT 5").df()

# %%
# The app database: attach it read-only and list its tables
con.sql(f"ATTACH '{RAW}/qafila_oltp.sqlite' AS app (TYPE sqlite, READ_ONLY)")
con.sql("SELECT table_name, estimated_size FROM duckdb_tables() WHERE database_name = 'app'").df()

# %% [markdown]
# ✏️ **Exercise** · Count the delivery events by `event_type`, straight from the JSON lines file, sorted by count.
# Store the result as a pandas DataFrame in `events_by_type` (columns `event_type`, `n`).

# %%
# BEGIN SOLUTION
events_by_type = con.sql(f"""
    SELECT event_type, count(*) AS n
    FROM read_json_auto('{RAW}/events/delivery_events.jsonl')
    GROUP BY event_type
    ORDER BY n DESC
""").df()
# END SOLUTION
events_by_type

# %%
U.check(set(events_by_type.columns) == {"event_type", "n"}, "columns are event_type, n")
U.check(int(events_by_type.n.sum()) == 22518, f"{int(events_by_type.n.sum()):,} events in total")

# %% [markdown]
# 🔍 **Look at** the counts: there are more `assigned` events than `delivered` + `cancelled` + `failed` together.
# Some orders were cancelled before pickup, but keep this in mind for Task 3: are all of those events unique?
#
# A join **across sources** works the same way. Revenue isn't stored anywhere: it is quantity × price − discount on
# the order lines. Here is gross merchandise value (GMV) by city:

# %%
con.sql("""
    SELECT s.city,
           count(DISTINCT o.order_id)                                   AS orders,
           round(sum(i.quantity * i.unit_price - i.discount), 0)        AS gmv_sar
    FROM app.orders o
    JOIN app.stores s USING (store_id)
    JOIN app.order_items i USING (order_id)
    WHERE o.status = 'delivered'
    GROUP BY ALL
    ORDER BY gmv_sar DESC
""").df()

# %% [markdown]
# ## 2 · SUMMARIZE the orders; daily volume chart (5')
#
# `SUMMARIZE` profiles every column at once: type, min, max, approximate distinct count, null percentage.

# %%
con.sql("SUMMARIZE app.orders").df()[["column_name", "column_type", "min", "max", "approx_unique", "null_percentage"]]

# %% [markdown]
# Notice `created_at` is a **string** (`VARCHAR`): SQLite has no real timestamp type. The app stores UTC as text.
# The business, however, counts days in Riyadh time (UTC+3): an order at 22:30 UTC on 22 September is a 23 September order.
#
# ✏️ **Exercise** · Build `daily`: one row per **Riyadh** calendar day with the number of orders, columns `day` and `orders`,
# ordered by day. Hint: `created_at::TIMESTAMP + INTERVAL 3 HOUR` then `::DATE`.

# %%
# BEGIN SOLUTION
daily = con.sql("""
    SELECT (created_at::TIMESTAMP + INTERVAL 3 HOUR)::DATE AS day, count(*) AS orders
    FROM app.orders
    GROUP BY day
    ORDER BY day
""").df()
# END SOLUTION
U.check(len(daily) == 61, f"{len(daily)} days (Aug + Sep 2026)")
ax = daily.plot(x="day", y="orders", figsize=(10, 3.2), legend=False, title="Qafila orders per day (Riyadh time)")
ax.set_xlabel("")
plt.tight_layout()
plt.show()

# %% [markdown]
# 🔍 **Look at** the weekly rhythm (Thursday and Friday peaks), the slow growth, and one day that stands out (the ✱ Stretch).
#
# ## 3 · Check keys: duplicate customers, orphan orders (5')
#
# A primary key in the source database only guarantees the **ID** is unique, not that each **person** appears once.
#
# ✏️ **Exercise** · Compute three numbers:
# - `dup_people`: how many `national_id` values appear on more than one customer row;
# - `orphan_orders`: orders whose `customer_id` has no row in `app.customers`;
# - `negative_lines`: order lines with `quantity < 0`.

# %%
# BEGIN SOLUTION
dup_people = con.sql("""
    SELECT count(*) FROM (
        SELECT national_id FROM app.customers GROUP BY national_id HAVING count(*) > 1
    )""").fetchone()[0]
orphan_orders = con.sql("""
    SELECT count(*) FROM app.orders o
    LEFT JOIN app.customers c USING (customer_id)
    WHERE c.customer_id IS NULL""").fetchone()[0]
negative_lines = con.sql("SELECT count(*) FROM app.order_items WHERE quantity < 0").fetchone()[0]
# END SOLUTION
print(f"duplicate people: {dup_people} | orphan orders: {orphan_orders} | negative lines: {negative_lines}")
U.check(dup_people > 40, "some people have several customer accounts")
U.check(orphan_orders > 0, "some orders point to customers that no longer exist")

# %% [markdown]
# Look at one duplicated person: what differs between the two rows?

# %%
con.sql("""
    SELECT customer_id, full_name, email, mobile, national_id, created_at
    FROM app.customers
    WHERE national_id = (SELECT national_id FROM app.customers GROUP BY 1 HAVING count(*) > 1 ORDER BY 1 LIMIT 1)
""").df()

# %% [markdown]
# 🔍 Same person, same national ID, but the email is upper-cased and the mobile number is written differently. A
# `DISTINCT` on email or mobile would not catch this. And the orphan orders? Those customers were **hard-deleted**
# (an erasure request under the PDPL, as we'll see in Lab 1.4). Their orders remain, so every report that inner-joins
# orders to customers silently loses revenue.
#
# ## 4 · Inspect formats: mobiles, timestamps, prices (5')
#
# Formats are where integrations break. Classify the shapes of a column with regular expressions.

# %%
con.sql(r"""
    SELECT CASE
             WHEN regexp_full_match(mobile, '\+9665\d{8}')          THEN '+9665XXXXXXXX'
             WHEN regexp_full_match(mobile, '05\d{8}')              THEN '05XXXXXXXX'
             WHEN regexp_full_match(mobile, '9665\d{8}')            THEN '9665XXXXXXXX'
             WHEN regexp_full_match(mobile, '\+966 5\d \d{3} \d{4}') THEN '+966 5X XXX XXXX'
             ELSE 'other' END AS mobile_format,
           count(*) AS n
    FROM app.customers GROUP BY ALL ORDER BY n DESC
""").df()

# %% [markdown]
# ✏️ **Exercise** · Do the same for `event_time` in the delivery events. Build `time_formats` with columns `format`, `n`,
# distinguishing: ISO with `+03:00` offset, ISO with `Z` (UTC), and epoch milliseconds (digits only).

# %%
# BEGIN SOLUTION
time_formats = con.sql(rf"""
    SELECT CASE
             WHEN event_time LIKE '%+03:00'                 THEN 'ISO +03:00'
             WHEN event_time LIKE '%Z'                      THEN 'ISO Z (UTC)'
             WHEN regexp_full_match(event_time, '\d{{13}}') THEN 'epoch ms'
             ELSE 'other' END AS format,
           count(*) AS n
    FROM read_json_auto('{RAW}/events/delivery_events.jsonl')
    GROUP BY ALL ORDER BY n DESC
""").df()
# END SOLUTION
time_formats

# %%
U.check(len(time_formats) == 3 and "other" not in set(time_formats.format), "three timestamp formats, nothing else")

# %% [markdown]
# And the supplier price files: compare the inferred schema of two weeks.

# %%
for week in ["W37", "W38", "W39"]:
    cols = con.sql(f"DESCRIBE SELECT * FROM read_csv('{RAW}/suppliers/prices_2026-{week}.csv')").df()
    print(week, dict(zip(cols.column_name, cols.column_type)))

# %%
con.sql(f"SELECT sku, unit_cost FROM read_csv('{RAW}/suppliers/prices_2026-W38.csv') WHERE unit_cost NOT LIKE '%,%' LIMIT 3").df()

# %% [markdown]
# 🔍 Week 37: `unit_cost` is a `DOUBLE`. Week 38: the same column is `VARCHAR` because the supplier's Excel export used
# decimal commas (`9,49`) and some Arabic-Indic digits (`٩٫٤٩`). Week 39: `unit_cost` is gone, `cost_sar` and `currency`
# appeared. Nobody told us. This is **schema drift**.
#
# ## 5 · Write the issue log as a table (5')
#
# An issue log is the cheapest documentation a data team has. Every row becomes a test (Day 2) or a contract clause (Day 4).
#
# ✏️ **Exercise** · Add the issues you found as rows of `issues` (at least 6). Keep the columns:
# `source`, `issue`, `evidence` (a number or example), `severity` (`high`/`medium`/`low`), `fix_in` (where it gets fixed).
# Two rows are given.

# %%
issues = [
    {"source": "app.orders", "issue": "orders of erased customers (orphan customer_id)", "evidence": orphan_orders,
     "severity": "high", "fix_in": "Day 2 model: left join + 'unknown customer' member"},
    {"source": "suppliers", "issue": "W38 decimal commas and Arabic-Indic digits", "evidence": "9,49 / ٩٫٤٩",
     "severity": "medium", "fix_in": "Day 2 Pandera validation + parsing"},
]
# BEGIN SOLUTION
issues += [
    {"source": "app.customers", "issue": "duplicate accounts for the same national_id", "evidence": dup_people,
     "severity": "high", "fix_in": "Day 2 staging: dedupe on national_id"},
    {"source": "app.customers", "issue": "mobile numbers in 4 formats", "evidence": "+9665…, 05…, 9665…, +966 5…",
     "severity": "medium", "fix_in": "Day 2 staging: normalize to E.164"},
    {"source": "app.order_items", "issue": "negative quantities", "evidence": negative_lines,
     "severity": "medium", "fix_in": "Day 2 tests + quarantine"},
    {"source": "app.orders", "issue": "timestamps are UTC text; business days are Riyadh time", "evidence": "VARCHAR created_at",
     "severity": "medium", "fix_in": "Day 2 staging: cast + convert time zone"},
    {"source": "delivery_events", "issue": "three timestamp formats", "evidence": len(time_formats),
     "severity": "high", "fix_in": "Day 2 staging / Day 3 streaming parser"},
    {"source": "suppliers", "issue": "W39 renamed unit_cost -> cost_sar, added currency", "evidence": "schema drift",
     "severity": "high", "fix_in": "Day 4 data contract"},
]
# END SOLUTION
issue_log = pd.DataFrame(issues)
U.check(len(issue_log) >= 8, f"{len(issue_log)} issues logged")
U.check(set(issue_log.severity) <= {"high", "medium", "low"}, "severity uses the agreed levels")
issue_log

# %% [markdown]
# Save it: issue logs belong in the repository next to the code, not in someone's head.

# %%
out = U.lab_dir("D1_1") / "issue_log.csv"
issue_log.to_csv(out, index=False)
print("saved", out)

# %% [markdown]
# Now compare your log with `data/raw/DATA_ISSUES.txt`. Which problems did you miss? Two hints: look for duplicate
# `event_id`s in the delivery events, and for SKUs with trailing spaces in `app.products`.

# %%
print(open(U.RAW / "DATA_ISSUES.txt", encoding="utf-8").read())
con.sql(f"""
    SELECT count(*) - count(DISTINCT event_id) AS duplicate_events
    FROM read_json_auto('{RAW}/events/delivery_events.jsonl')
""").df()

# %% [markdown]
# ## ✱ Stretch · Find the National Day spike and explain it
#
# One day in `daily` has almost twice the usual orders. Which day is it, and what does the `promo_code` column say about it?
# Saudi National Day is 23 September.

# %%
# BEGIN SOLUTION
peak = daily.loc[daily.orders.idxmax()]
print("peak day:", peak.day.date(), "| orders:", peak.orders)
con.sql("""
    SELECT coalesce(promo_code, '(none)') AS promo, count(*) AS orders
    FROM app.orders
    WHERE (created_at::TIMESTAMP + INTERVAL 3 HOUR)::DATE = DATE '2026-09-23'
    GROUP BY ALL ORDER BY orders DESC
""").df()
# END SOLUTION

# %% [markdown]
# ## Checkpoint
#
# 1. Why did we never copy the SQLite tables into DuckDB in this lab? When would you copy them?
# 2. Orders are stored with UTC timestamps. Which reports change if you count days in UTC instead of Riyadh time?
# 3. Two customer rows share a national ID. Should the source system or the data platform fix that? Who do you tell?
# 4. Pick the one issue from your log that would hurt a revenue dashboard most. Why that one?
