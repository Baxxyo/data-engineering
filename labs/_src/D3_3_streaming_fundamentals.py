# %% [markdown]
# # Lab 3.3 · Streaming fundamentals: logs, offsets, windows, watermarks
#
# Day 3 · Session 3 (Streaming fundamentals) · 25 min. Kafka and Flink are big systems; the ideas underneath are small.
# In this lab you build them in plain Python, on a week of Qafila **courier events** (Sep 24-30):
# - a **partitioned, append-only log** (a tiny Kafka topic) and **consumer offsets**;
# - what "at-least-once" really means after a crash, and how **idempotent** processing fixes it;
# - **event time vs processing time**, and how late events really are;
# - **tumbling windows** closed by a **watermark**: the latency vs completeness trade-off, measured;
# - **replay**: recomputing everything from the log.
#
# **Tasks**
# - [ ] 1 · Produce the events into a 4-partition log, keyed by order (5')
# - [ ] 2 · Consume with offsets; crash; make processing idempotent (6')
# - [ ] 3 · Event time vs processing time (5')
# - [ ] 4 · Windows and watermarks: measure the trade-off (7')
# - [ ] 5 · Replay from offset 0 (2')
# - [ ] ✱ Stretch · Exactly-once: commit results and offsets in one transaction

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
import zlib
from collections import Counter, defaultdict
from datetime import datetime, timedelta, timezone

import pandas as pd

events = [json.loads(line) for line in open(U.RAW / "events" / "delivery_events.jsonl", encoding="utf-8")]
print(f"{len(events):,} events, in arrival order")
pd.DataFrame(events[:4])

# %% [markdown]
# ## 1 · Produce the events into a 4-partition log, keyed by order (5')
#
# A Kafka **topic** is split into **partitions**; each partition is an append-only list where a record's position is its
# **offset**. The producer picks the partition by hashing the record **key**. Here's the whole idea in 15 lines:

# %%
class Log:
    """A tiny Kafka topic: N partitions, each an append-only list; a record's offset is its index."""

    def __init__(self, partitions: int = 4):
        self.parts = [[] for _ in range(partitions)]

    def partition_for(self, key) -> int:
        return zlib.crc32(str(key).encode()) % len(self.parts)

    def produce(self, key, value) -> tuple[int, int]:
        p = self.partition_for(key)
        self.parts[p].append((key, value))
        return p, len(self.parts[p]) - 1          # (partition, offset)

    def read(self, partition: int, offset: int, max_records: int) -> list:
        return self.parts[partition][offset: offset + max_records]

    def end_offsets(self) -> list[int]:
        return [len(p) for p in self.parts]

# %% [markdown]
# ✏️ **Exercise** · Create `log = Log(4)` and produce every event, **keyed by `order_id`**, in arrival order. Then build
# `per_partition` = the number of records in each partition.

# %%
# BEGIN SOLUTION
log = Log(4)
for e in events:
    log.produce(e["order_id"], e)
per_partition = log.end_offsets()
# END SOLUTION
print("records per partition:", per_partition)

# %%
order_parts = defaultdict(set)
for p, part in enumerate(log.parts):
    for key, _ in part:
        order_parts[key].add(p)
U.check(sum(per_partition) == len(events), "every event is in the log")
U.check(all(len(ps) == 1 for ps in order_parts.values()), "all events of an order land in the same partition")

# %% [markdown]
# 🔍 Ordering is guaranteed **per partition**, not across the topic. Keying by `order_id` keeps each order's
# `assigned → picked_up → delivered` in order, while the 4 partitions can be consumed in parallel by 4 consumers.
# Choose keys for the ordering you need; a hot key (one huge store?) makes one partition hot.
#
# ## 2 · Consume with offsets; crash; make processing idempotent (6')
#
# A consumer reads from each partition starting at its **committed offset**, processes a batch, then commits the new
# offsets. Kafka stores them per **consumer group**. The job: count **delivered** orders per city.

# %%
class Consumer:
    """One consumer of a group, reading every partition; offsets live in `store[group]` until committed."""

    def __init__(self, log: Log, group: str, store: dict):
        self.log, self.group, self.store = log, group, store
        self.position = list(store.get(group, [0] * len(log.parts)))   # resume from committed offsets

    def poll(self, max_records: int = 200) -> list:
        batch = []
        for p in range(len(self.log.parts)):
            recs = self.log.read(p, self.position[p], max_records // len(self.log.parts))
            batch += [(p, self.position[p] + i, k, v) for i, (k, v) in enumerate(recs)]
            self.position[p] += len(recs)
        return batch

    def commit(self) -> None:
        self.store[self.group] = list(self.position)


truth = Counter()
for e in {e["event_id"]: e for e in events}.values():           # deduplicated, the "right answer"
    if e["event_type"] == "delivered":
        truth[e["city"]] += 1
print("truth:", dict(truth))

# %% [markdown]
# A naive consumer that **crashes once** between processing a batch and committing it, then restarts:

# %%
def run_naive(crash_at_batch: int | None = 10):
    store, counts, batches = {}, Counter(), 0
    c = Consumer(log, "naive", store)
    while batch := c.poll():
        for _, _, _, e in batch:
            if e["event_type"] == "delivered":
                counts[e["city"]] += 1
        batches += 1
        if batches == crash_at_batch:                       # 💥 crash before commit: restart from committed offsets
            c = Consumer(log, "naive", store)
            crash_at_batch = None
            continue
        c.commit()
    return counts

naive = run_naive()
print("naive:", dict(naive), "| over-count:", sum(naive.values()) - sum(truth.values()))

# %% [markdown]
# 🔍 Two sources of double counting: the batch replayed after the crash (**at-least-once** delivery), and the producer's own
# duplicates (phones retrying, 312 of them). Exactly-once delivery across a network is (practically) impossible; what you
# build instead is **idempotent processing**: applying the same record twice has the same effect as once.
#
# ✏️ **Exercise** · Write `run_idempotent(crash_at_batch=10)`: same loop and crash, but count each **`event_id`** once
# (keep a set of seen ids in the consumer's state). Store the result in `idem`.

# %%
# BEGIN SOLUTION
def run_idempotent(crash_at_batch: int | None = 10):
    store, counts, seen, batches = {}, Counter(), set(), 0
    c = Consumer(log, "idempotent", store)
    while batch := c.poll():
        for _, _, _, e in batch:
            if e["event_id"] in seen:
                continue
            seen.add(e["event_id"])
            if e["event_type"] == "delivered":
                counts[e["city"]] += 1
        batches += 1
        if batches == crash_at_batch:
            c = Consumer(log, "idempotent", store)
            crash_at_batch = None
            continue
        c.commit()
    return counts

idem = run_idempotent()
# END SOLUTION
print("idempotent:", dict(idem))
U.check(idem == truth, "crash + duplicates, still exactly the right counts")

# %% [markdown]
# 🔍 In production the "seen" set lives in the **sink**: a `MERGE` keyed by `event_id` (Lab 1.4), an upsert into Iceberg
# (Lab 3.4), or Flink's keyed state with a TTL. "Exactly-once" in Kafka/Flink marketing means **effectively-once**:
# at-least-once delivery + idempotent or transactional writes.
#
# ## 3 · Event time vs processing time (5')
#
# Each event has two clocks: `event_time` (when it happened, on the courier's phone) and `ingest_time` (when the platform
# received it). And the phone app writes `event_time` in **three formats** (two app versions in the field...).

# %%
print(Counter(("epoch-ms" if e["event_time"].isdigit() else e["event_time"][-6:] if "+" in e["event_time"] else "Z")
              for e in events))

# %% [markdown]
# ✏️ **Exercise** · Write `parse_event_time(s) -> datetime` (UTC, timezone-aware) for all three formats. Then compute
# `lateness` = a pandas Series of `ingest_time − event_time` in **seconds** for every event.

# %%
def parse_ingest(s):
    return datetime.fromisoformat(s.replace("Z", "+00:00"))

# BEGIN SOLUTION
def parse_event_time(s: str) -> datetime:
    if s.isdigit():
        return datetime.fromtimestamp(int(s) / 1000, tz=timezone.utc)
    return datetime.fromisoformat(s.replace("Z", "+00:00")).astimezone(timezone.utc)

lateness = pd.Series([(parse_ingest(e["ingest_time"]) - parse_event_time(e["event_time"])).total_seconds() for e in events])
# END SOLUTION
for e in events:
    e["ts"], e["arrived"] = parse_event_time(e["event_time"]), parse_ingest(e["ingest_time"])
print(lateness.describe(percentiles=[0.5, 0.9, 0.99, 0.999]).round(1))

# %%
U.check(parse_event_time("1790000000000") == datetime(2026, 9, 21, 14, 13, 20, tzinfo=timezone.utc), "epoch millis")
U.check(parse_event_time("2026-09-24T05:14:52.016+03:00") == parse_event_time("2026-09-24T02:14:52.016Z"), "same instant, two formats")
U.check(lateness.min() >= 0 and lateness.max() > 30 * 60, "nothing arrives before it happens; some events are >30 min late")

# %% [markdown]
# 🔍 Most events arrive within seconds; a long tail (couriers' phones offline in a basement car park) arrives up to
# ~50 minutes late. Any "per 15 minutes" metric must decide **which clock** to use, and **how long to wait** for stragglers.
#
# ## 4 · Windows and watermarks: measure the trade-off (7')
#
# Metric: **delivered orders per city per 15-minute window** (by event time). A streaming engine keeps one open window per
# (city, window start) and must decide when a window is **final**. It uses a **watermark**: "I believe I have seen
# everything up to time W", here W = (max event time seen) − (allowed lateness). When W passes a window's end, the window
# is emitted and its state dropped; events for an already-emitted window are **late** and dropped.

# %%
WINDOW = timedelta(minutes=15)

def window_start(ts: datetime) -> datetime:
    return ts - timedelta(minutes=ts.minute % 15, seconds=ts.second, microseconds=ts.microsecond)

batch_truth = defaultdict(set)
for e in events:
    if e["event_type"] == "delivered":
        batch_truth[(e["city"], window_start(e["ts"]))].add(e["order_id"])
batch_truth = {k: len(v) for k, v in batch_truth.items()}
print(len(batch_truth), "non-empty (city, window) cells")

# %% [markdown]
# ✏️ **Exercise** · Complete `stream_windows(stream, allowed_lateness)`. For each event (in arrival order):
# 1. advance `max_ts` and compute the watermark `wm = max_ts − allowed_lateness`;
# 2. if the event's window **end** ≤ the last emitted watermark → it's late: `dropped += 1`, skip it;
# 3. otherwise add its `order_id` to `state[(city, window_start)]` (a set: duplicates are harmless);
# 4. emit (move to `emitted`) every open window whose end ≤ `wm`, recording the emit **delay** = `e["arrived"] − window end`.
#
# Only `delivered` events matter.

# %%
def stream_windows(stream, allowed_lateness: timedelta):
    state, emitted, delays = {}, {}, []
    max_ts, wm, dropped = datetime.min.replace(tzinfo=timezone.utc), datetime.min.replace(tzinfo=timezone.utc), 0
    for e in stream:
        if e["event_type"] != "delivered":
            continue
        # BEGIN SOLUTION
        max_ts = max(max_ts, e["ts"])
        wm = max_ts - allowed_lateness
        key = (e["city"], window_start(e["ts"]))
        if key[1] + WINDOW <= wm:
            dropped += 1
            continue
        state.setdefault(key, set()).add(e["order_id"])
        for k in [k for k in state if k[1] + WINDOW <= wm]:
            emitted[k] = len(state.pop(k))
            delays.append((e["arrived"] - (k[1] + WINDOW)).total_seconds())
        # END SOLUTION
    for k, v in state.items():          # end of stream: flush what's left
        emitted[k] = len(v)
    return emitted, dropped, delays

# %%
rows = []
for minutes in [0, 2, 10, 30, 60]:
    out, dropped, delays = stream_windows(events, timedelta(minutes=minutes))
    exact = sum(out.get(k) == v for k, v in batch_truth.items()) / len(batch_truth)
    rows.append({"allowed_lateness_min": minutes, "late_events_dropped": dropped, "windows_exactly_right": f"{exact:.1%}",
                 "median_emit_delay_min": round(pd.Series(delays).median() / 60, 1)})
tradeoff = pd.DataFrame(rows)
tradeoff

# %%
U.check(tradeoff.late_events_dropped.is_monotonic_decreasing, "waiting longer drops fewer late events")
U.check(tradeoff.median_emit_delay_min.is_monotonic_increasing, "...but results arrive later")
U.check(tradeoff.late_events_dropped.iloc[-1] == 0, "with 60 minutes of patience, nothing is dropped")

# %% [markdown]
# 🔍 There's no free lunch: **latency vs completeness** is a business decision. A live ops dashboard might wait 2 minutes and
# accept that ~1 window in 10 is slightly low; finance wants the complete numbers and can wait (or recompute in batch tonight). Engines add escape
# hatches: Flink's **allowed lateness** re-emits a window as an update; Spark Structured Streaming's `withWatermark` drops
# state after the threshold; Kafka Streams has **grace periods**.
#
# ## 5 · Replay from offset 0 (2')
#
# The log is **retained**, not deleted on read. A new consumer group starting at offset 0 recomputes everything: that's how
# you fix a bug in a streaming job, backfill a new metric, or bootstrap a new downstream system.

# %%
replayed = run_idempotent(crash_at_batch=None)        # a fresh group: offsets start at 0
U.check(replayed == truth, "replaying the retained log reproduces the result")
print("committed offsets per group are independent; the log itself is untouched:", log.end_offsets())

# %% [markdown]
# ## ✱ Stretch · Exactly-once: commit results and offsets in one transaction
#
# Keep the per-city counts **and** the consumer offsets in the same DuckDB database, and update both in one transaction
# per batch. Crash between processing and commit → the transaction rolls back → no double counting, even without a seen-set.

# %%
# BEGIN SOLUTION
import duckdb

db = duckdb.connect()
db.sql("CREATE TABLE counts (city VARCHAR PRIMARY KEY, delivered INT)")
db.sql("CREATE TABLE offsets (part INT PRIMARY KEY, next_offset INT)")
db.executemany("INSERT INTO offsets VALUES (?, 0)", [[p] for p in range(4)])

def run_transactional(crash_at_batch=10):
    batches = 0
    while True:
        start = [r[0] for r in db.sql("SELECT next_offset FROM offsets ORDER BY part").fetchall()]
        c = Consumer(log, "tx", {"tx": start})
        batch = c.poll()
        if not batch:
            return
        delta = Counter(e["city"] for *_, e in batch if e["event_type"] == "delivered")
        db.begin()
        for city, n in delta.items():
            db.execute("INSERT INTO counts VALUES (?, ?) ON CONFLICT DO UPDATE SET delivered = delivered + excluded.delivered", [city, n])
        batches += 1
        if batches == crash_at_batch:
            db.rollback()                                   # 💥 crash mid-transaction: nothing was applied
            crash_at_batch = None
            continue
        db.executemany("UPDATE offsets SET next_offset = ? WHERE part = ?", [[c.position[p], p] for p in range(4)])
        db.commit()

run_transactional()
tx = dict(db.sql("SELECT city, delivered FROM counts").fetchall())
print("transactional:", tx)
print("raw-duplicate-inclusive truth:", dict(Counter(e["city"] for e in events if e["event_type"] == "delivered")))
# END SOLUTION

# %% [markdown]
# 🔍 The crash no longer double-counts, but the producer's duplicates still do: transactions make **your** processing
# exactly-once; they can't undo duplicates already in the input. You still need a key to dedupe on. (Kafka's idempotent
# producer and transactions solve the same two problems on the Kafka side.)
#
# ## Checkpoint
#
# 1. Why does Kafka guarantee ordering only within a partition? How do you choose the key?
# 2. Your consumer commits offsets **before** processing a batch. What failure does that cause?
# 3. Event time or processing time for "deliveries per 15 minutes"? When would you choose the other one?
# 4. The ops team wants the dashboard updated within 2 minutes and finance wants exact daily totals. Design both with one stream.
