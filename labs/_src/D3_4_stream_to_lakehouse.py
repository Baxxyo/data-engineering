# %% [markdown]
# # Lab 3.4 · Streams into the lakehouse
#
# Day 3 · Session 4 (Streams into the lakehouse) · 25 min. Lab 3.3 built the stream; Lab 3.1 built the table. Now join them:
# a **micro-batch** job that lands courier events in an Iceberg table, survives crashes without duplicates, keeps the table
# healthy, and feeds a near-real-time **delivery SLA** dashboard.
#
# **Tasks**
# - [ ] 1 · A bronze Iceberg table and a micro-batch sink that checkpoints **in the table** (6')
# - [ ] 2 · Crash, restart, no duplicates (5')
# - [ ] 3 · The small-files problem and compaction (5')
# - [ ] 4 · The SLA query, straight off the table (5')
# - [ ] 5 · The table is a stream: incremental reads for the next job (4')
# - [ ] ✱ Stretch · Late data rewrites yesterday's numbers: time travel the dashboard

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
from datetime import datetime, timedelta, timezone

import pandas as pd
import pyarrow as pa
from pyiceberg.transforms import DayTransform

def parse_ts(s: str) -> datetime:
    """The three event_time formats from Lab 3.3 -> aware UTC datetime."""
    if s.isdigit():
        return datetime.fromtimestamp(int(s) / 1000, tz=timezone.utc)
    return datetime.fromisoformat(s.replace("Z", "+00:00")).astimezone(timezone.utc)

events = [json.loads(line) for line in open(U.RAW / "events" / "delivery_events.jsonl", encoding="utf-8")]
for e in events:
    e["event_ts"], e["ingest_ts"] = parse_ts(e["event_time"]), parse_ts(e["ingest_time"])

DAYS = U.pick(4, 2)                               # how many days of the stream to replay
START = datetime(2026, 9, 24, tzinfo=timezone.utc)
STOP = START + timedelta(days=DAYS)
stream = [e for e in events if e["ingest_ts"] < STOP]
print(f"replaying {len(stream):,} events, {DAYS} days")

SCHEMA = pa.schema([("event_id", pa.string()), ("order_id", pa.int64()), ("store_id", pa.string()), ("city", pa.string()),
                    ("courier_id", pa.string()), ("event_type", pa.string()), ("event_ts", pa.timestamp("us", tz="UTC")),
                    ("ingest_ts", pa.timestamp("us", tz="UTC"))])

def to_arrow(rows: list[dict]) -> pa.Table:
    return pa.Table.from_pylist([{f: r[f] for f in SCHEMA.names} for r in rows], schema=SCHEMA)

def micro_batches(rows, every=timedelta(minutes=30)):
    """Yield (end_offset, rows, batch_end) per processing-time interval; offsets index the arrival-ordered stream."""
    i, t = 0, START
    while i < len(rows):
        t += every
        j = i
        while j < len(rows) and rows[j]["ingest_ts"] < t:
            j += 1
        if j > i:
            yield j, rows[i:j], t
        i = j

print(sum(1 for _ in micro_batches(stream)), "micro-batches of 30 minutes")

# %% [markdown]
# ## 1 · A bronze Iceberg table and a micro-batch sink that checkpoints **in the table** (6')
#
# Spark Structured Streaming and Flink both work in **micro-batches** or **checkpoints**: process a chunk, commit output,
# record progress. The trick for exactly-once into a table: store the **source offset in the same commit** as the data.
# Iceberg snapshots carry a free-form `summary`, so one atomic commit = data + offset.

# %%
LAKE = U.lab_dir("D3_4", reset=True) / "lakehouse"
catalog = U.get_catalog(LAKE)
catalog.create_namespace("bronze")
events_tbl = catalog.create_table("bronze.delivery_events", schema=SCHEMA)
with events_tbl.update_spec() as spec:
    spec.add_field("event_ts", DayTransform(), "event_day")
print(events_tbl.spec())

# %% [markdown]
# ✏️ **Exercise** · Write the two halves of the checkpoint:
# - `commit_batch(tbl, rows, end_offset, batch_end)`: append `to_arrow(rows)` with
#   `snapshot_properties={"qafila.offset": str(end_offset), "qafila.batch_end": batch_end.isoformat()}`;
# - `committed_offset(tbl) -> int`: the `"qafila.offset"` of the **most recent** snapshot that has one (walk
#   `tbl.snapshots()` backwards; read it with `snap.summary.additional_properties.get(...)`), or `0` if none has.
#   Maintenance commits (compaction, Task 3) won't carry an offset.

# %%
# BEGIN SOLUTION
def commit_batch(tbl, rows, end_offset, batch_end):
    tbl.append(to_arrow(rows), snapshot_properties={"qafila.offset": str(end_offset), "qafila.batch_end": batch_end.isoformat()})

def committed_offset(tbl) -> int:
    for snap in reversed(tbl.snapshots()):
        offset = snap.summary.additional_properties.get("qafila.offset")
        if offset is not None:
            return int(offset)
    return 0
# END SOLUTION

# %%
end, rows, t = next(micro_batches(stream))
commit_batch(events_tbl, rows, end, t)
U.check(committed_offset(events_tbl) == end, f"offset {end} committed atomically with {len(rows)} rows")
events_tbl.inspect.snapshots().to_pandas()[["snapshot_id", "operation", "summary"]]

# %% [markdown]
# ## 2 · Crash, restart, no duplicates (5')
#
# The job below resumes from the table's checkpoint, skips batches already committed, and drops the producer's duplicate
# events using a **bounded** set of recently seen ids (duplicates arrive within 90 s, so 2 batches of memory is plenty;
# that's Spark's `dropDuplicatesWithinWatermark`). It crashes once, mid-run.

# %%
class Crash(Exception):
    pass

def run_job(tbl, crash_after_batches=None):
    start = committed_offset(tbl)
    recent, written, n = [set(), set()], 0, 0
    for end, rows, batch_end in micro_batches(stream):
        if end <= start:
            for r in rows:                        # rebuild dedupe state from the replayed tail, write nothing
                recent[-1].add(r["event_id"])
            recent = [recent[-1], set()]
            continue
        fresh = []
        for r in rows:
            if r["event_id"] not in recent[0] and r["event_id"] not in recent[1]:
                recent[1].add(r["event_id"])
                fresh.append(r)
        commit_batch(tbl, fresh, end, batch_end)
        recent = [recent[1], set()]
        written, n = written + len(fresh), n + 1
        if crash_after_batches and n == crash_after_batches:
            raise Crash(f"💥 killed after {n} batches (offset {end})")
    return written

try:
    run_job(events_tbl, crash_after_batches=40)
except Crash as e:
    print(e)
print("restarting from offset", committed_offset(events_tbl), "...")
print("rows written after restart:", run_job(events_tbl))

# %% [markdown]
# ✏️ **Exercise** · Prove it: compute `n_rows` (rows in the table), `n_distinct` (distinct `event_id`s in the table) and
# `n_expected` (distinct `event_id`s in `stream`).

# %%
# BEGIN SOLUTION
ids = events_tbl.scan(selected_fields=("event_id",)).to_arrow()["event_id"]
n_rows, n_distinct = len(ids), len(set(ids.to_pylist()))
n_expected = len({e["event_id"] for e in stream})
# END SOLUTION
print(f"rows {n_rows:,} | distinct {n_distinct:,} | expected {n_expected:,} | raw events incl. duplicates {len(stream):,}")
U.check(n_rows == n_distinct == n_expected, "crash + restart + producer duplicates -> every event exactly once")

# %% [markdown]
# 🔍 Why this works: the data and the offset are committed **together** (one snapshot). There's no window where one is
# saved and the other isn't. If the checkpoint lived in a separate file written after the commit, a crash in between would
# replay the batch: duplicates. (Spark writes its checkpoint to a folder and uses Iceberg/Delta commit ids to make the sink
# idempotent; Flink uses two-phase commit. Same idea.)
#
# ## 3 · The small-files problem and compaction (5')

# %%
def table_health(tbl):
    f = tbl.inspect.files().to_pandas()
    return {"snapshots": len(tbl.snapshots()), "data_files": len(f), "avg_file_kB": round(float(f.file_size_in_bytes.mean()) / 1e3, 1),
            "rows": int(f.record_count.sum())}

before = table_health(events_tbl)
t_before = U.best_of(lambda: events_tbl.scan().to_arrow(), 3)
print(before, f"| full scan {t_before:.2f}s")

# %% [markdown]
# Every 30-minute batch wrote a new file into each day partition it touched (late events touch older days too):
# hundreds of tiny files. Each file costs an open, a footer read and a manifest entry; object stores charge per request.
#
# ✏️ **Exercise** · Compact: for each day `d` in the table, read that day's rows (`scan(row_filter=...)` on `event_ts`) and
# rewrite them **in one file**, sorted by `event_ts`, with `events_tbl.overwrite(df, overwrite_filter=same_filter)`.
# (A string filter works: `"event_ts >= '2026-09-24T00:00:00+00:00' and event_ts < '2026-09-25T00:00:00+00:00'"`.)

# %%
# BEGIN SOLUTION
for i in range(DAYS):
    lo, hi = START + timedelta(days=i), START + timedelta(days=i + 1)
    flt = f"event_ts >= '{lo.isoformat()}' and event_ts < '{hi.isoformat()}'"
    day = events_tbl.scan(row_filter=flt).to_arrow()
    events_tbl.overwrite(day.sort_by("event_ts"), overwrite_filter=flt)
# END SOLUTION
after = table_health(events_tbl)
t_after = U.best_of(lambda: events_tbl.scan().to_arrow(), 3)
print(after, f"| full scan {t_after:.2f}s")

# %%
U.check(after["rows"] == before["rows"], "compaction never changes the data")
U.check(after["data_files"] == DAYS < before["data_files"], "one file per day partition")

# %% [markdown]
# 🔍 In production you don't hand-roll this: Spark's `rewrite_data_files` procedure, managed services (S3 Tables,
# Snowflake/Databricks auto-optimise) or a nightly maintenance job do it, then `expire_snapshots` + `remove_orphan_files`
# (Lab 3.1). Compaction is a **commit** too: if the streaming writer commits the same partition at the same moment, one of
# them retries (optimistic concurrency).
#
# ## 4 · The SLA query, straight off the table (5')
#
# Qafila promises delivery in **30 minutes** from checkout. Join the stream (delivered events) with the orders from the app
# database, per store and day (Riyadh time).
#
# ✏️ **Exercise** · With DuckDB over `iceberg_scan`, compute `sla`: one row per `store_id`, `day` (Riyadh date of the order):
# `delivered` (orders), `p50_min` (median minutes from `created_at` to the delivered event), `sla_rate` (share ≤ 30 min,
# 3 decimals). App `created_at` is naive UTC: compare with `event_ts` via `created_at::TIMESTAMP AT TIME ZONE 'UTC'`.

# %%
con = U.connect()
U.duckdb_iceberg(con)
U.attach_app(con)
con.sql("SET TimeZone = 'Asia/Riyadh'")
src = events_tbl.metadata_location

# BEGIN SOLUTION
sla = con.sql(f"""
    WITH delivered AS (
        SELECT order_id, min(event_ts) AS delivered_ts
        FROM iceberg_scan('{src}') WHERE event_type = 'delivered' GROUP BY order_id
    ), timed AS (
        SELECT o.store_id, (o.created_at::TIMESTAMP AT TIME ZONE 'UTC')::DATE AS day,
               date_diff('second', o.created_at::TIMESTAMP AT TIME ZONE 'UTC', d.delivered_ts) / 60 AS minutes
        FROM delivered d JOIN app.orders o USING (order_id)
    )
    SELECT store_id, day, count(*) AS delivered, round(median(minutes), 1) AS p50_min,
           round(avg((minutes <= 30)::INT), 3) AS sla_rate
    FROM timed GROUP BY ALL ORDER BY day, store_id
""").df()
# END SOLUTION
sla.query("delivered >= 20").pivot(index="store_id", columns="day", values="sla_rate")   # full days only

# %%
U.check(set(sla.columns) >= {"store_id", "day", "delivered", "p50_min", "sla_rate"}, "SLA table shape")
U.check(sla.groupby("store_id").sla_rate.mean().idxmin() == "RUH-01", "one store is clearly struggling: which one?")

# %% [markdown]
# 🔍 RUH-01 (Olaya) consistently misses the SLA: pickers there are slower (the data generator made it 1.6× busier). That's
# the kind of finding a **real-time** pipeline is for: the ops lead sees it at 10:00, not in tomorrow's report.
#
# ## 5 · The table is a stream: incremental reads for the next job (4')
#
# A downstream silver job shouldn't rescan the whole table every 30 minutes. Iceberg knows which snapshots **appended**
# which files, so a reader can ask for "rows added since snapshot X", like a consumer offset.
#
# ✏️ **Exercise** · Remember `last_seen = events_tbl.current_snapshot().snapshot_id`, stream **one more day** (extend
# `stream` and call `run_job`), then read only the new rows with
# `events_tbl.incremental_append_scan(from_snapshot_id_exclusive=last_seen).to_arrow()` into `new_rows`.

# %%
# BEGIN SOLUTION
last_seen = events_tbl.current_snapshot().snapshot_id
STOP = STOP + timedelta(days=1)
stream = [e for e in events if e["ingest_ts"] < STOP]
added = run_job(events_tbl)
new_rows = events_tbl.incremental_append_scan(from_snapshot_id_exclusive=last_seen).to_arrow()
# END SOLUTION
print(f"job wrote {added:,} rows; the incremental read returned {new_rows.num_rows:,}")
U.check(new_rows.num_rows == added, "incremental read = exactly the new appends")

# %% [markdown]
# 🔍 This is how Flink and Spark stream **from** Iceberg tables, and how Kappa-style architectures chain jobs through the
# lakehouse instead of through more Kafka topics. Overwrites (like your compaction) are skipped: they add no new data.
#
# ## ✱ Stretch · Late data rewrites yesterday's numbers: time travel the dashboard
#
# The ops dashboard refreshed at **22:00 Riyadh on Sep 25** (19:00 UTC) and showed deliveries for the peak hour 21:00-22:00.
# Find the snapshot that was current then (the last one whose `qafila.batch_end` ≤ 19:00 UTC), count that hour's delivered
# orders **as of then**, and compare with what the table says **now**.

# %%
# BEGIN SOLUTION
cutoff = datetime(2026, 9, 25, 19, tzinfo=timezone.utc)
then = [s for s in events_tbl.snapshots()
        if "qafila.batch_end" in s.summary.additional_properties
        and datetime.fromisoformat(s.summary.additional_properties["qafila.batch_end"]) <= cutoff][-1]
hour = ("event_type = 'delivered' and event_ts >= '2026-09-25T18:00:00+00:00' and event_ts < '2026-09-25T19:00:00+00:00'")
as_then = events_tbl.scan(snapshot_id=then.snapshot_id, row_filter=hour).to_arrow()
as_now = events_tbl.scan(row_filter=hour).to_arrow()
cmp = pd.DataFrame({"version": ["dashboard at 22:00", "table now"],
                    "delivered_21_22h": [len(set(as_then["order_id"].to_pylist())), len(set(as_now["order_id"].to_pylist()))]})
U.check(cmp.delivered_21_22h[0] < cmp.delivered_21_22h[1], "late events filled in the hour after the dashboard showed it")
# END SOLUTION
cmp

# %% [markdown]
# 🔍 Some deliveries that **happened** in that hour only **arrived** after 22:00 (couriers offline). A dashboard snapshot
# under-counts the most recent hour; the table, re-read later, is complete. Decide which version each consumer needs (and say it in
# the data contract, Day 4).
#
# ## Checkpoint
#
# 1. Why must the offset be committed in the **same** transaction as the data?
# 2. Smaller micro-batches mean fresher data. What do they cost, and what pays that cost back?
# 3. Where would you compute the SLA: in the streaming job, or as a query on the table? What changes with late events?
# 4. Kappa vs Lambda: with replayable logs and incremental table reads, do you still need a separate batch pipeline?
