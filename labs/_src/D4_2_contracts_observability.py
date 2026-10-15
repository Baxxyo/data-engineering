# %% [markdown]
# # Lab 4.2 · Data contracts and observability
#
# Day 4 · Session 2 (Observability & data contracts) · 25 min. Qafila's payment provider sends a daily feed. Today you
# make the expectations on both sides **explicit and executable**:
# - a **data contract** in the Open Data Contract Standard (ODCS v3.1, YAML);
# - a small **validator** that checks the real feed against it;
# - a **breaking-change** detector for when the provider proposes v2;
# - **freshness** and **volume** monitors that know about weekends and National Day;
# - **impact analysis** from dbt lineage: who must be told?
#
# **Tasks**
# - [ ] 1 · Read the contract; add the `status` property (5')
# - [ ] 2 · Validate the feed against the contract (7')
# - [ ] 3 · The provider proposes v2: breaking or not? (5')
# - [ ] 4 · Freshness and volume monitors (6')
# - [ ] 5 · Impact analysis with dbt lineage (2')
# - [ ] ✱ Stretch · Turn the contract into dbt tests automatically

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
import copy
import json
import re

import duckdb
import pandas as pd
import yaml

pd.set_option("display.max_colwidth", 80)
con = duckdb.connect()
con.sql(f"ATTACH '{U.build_warehouse().as_posix()}' AS wh (READ_ONLY)")
con.sql("CREATE TABLE payments AS SELECT * FROM wh.raw.payments")      # today's feed from the provider
print(con.sql("DESCRIBE payments").df()[["column_name", "column_type"]].T)

# %% [markdown]
# ## 1 · Read the contract; add the `status` property (5')
#
# A **data contract** is an agreement between a data **producer** (here: the payment provider) and its **consumers**
# (Qafila's finance and analytics teams): the schema, the meaning of each field, quality rules, service levels, owners
# and how to get support. ODCS is the vendor-neutral YAML format for it (Bitol project, Linux Foundation AI & Data).

# %%
CONTRACT_V1 = yaml.safe_load("""
apiVersion: v3.1.0
kind: DataContract
id: urn:datacontract:qafila:payments-feed
name: Payments feed
version: 1.0.0
status: active
domain: finance
dataProduct: payments
description:
  purpose: Every payment attempt for Qafila orders, as captured by the payment provider.
  usage: Revenue reconciliation (finance), payment-method analytics, refund monitoring.
  limitations: Amounts are in SAR including VAT. Card data is limited to the last 4 digits.
team:
  - username: payments-provider-integration
    role: owner
  - username: qafila-data-platform
    role: consumer
support:
  - channel: "#payments-data"
    tool: slack
schema:
  - name: payments
    physicalName: payments
    physicalType: table
    description: One row per payment attempt (failed attempts are real rows).
    quality:
      - id: payments_row_count
        metric: rowCount
        mustBeGreaterThan: 1000
      - id: payments_unique_id
        metric: duplicateValues
        arguments:
          properties: [payment_id]
        mustBe: 0
      - id: payments_positive_amount
        type: sql
        description: A payment can't be zero or negative; refunds are a status, not a negative amount.
        query: SELECT count(*) FROM {object} WHERE amount <= 0
        mustBe: 0
    properties:
      - name: payment_id
        logicalType: string
        physicalType: VARCHAR
        primaryKey: true
        required: true
        unique: true
      - name: order_id
        logicalType: integer
        physicalType: BIGINT
        required: true
      - name: amount
        logicalType: number
        physicalType: DOUBLE
        required: true
        description: Amount charged in SAR, VAT included.
      - name: currency
        logicalType: string
        physicalType: VARCHAR
        required: true
        quality:
          - metric: invalidValues
            arguments:
              validValues: [SAR]
            mustBe: 0
      - name: method
        logicalType: string
        physicalType: VARCHAR
        required: true
        quality:
          - metric: invalidValues
            arguments:
              validValues: [card, apple_pay, wallet]
            mustBe: 0
      - name: card_last4
        logicalType: string
        physicalType: VARCHAR
        classification: restricted
        description: Last 4 digits of the card; null for wallets.
        quality:
          - metric: invalidValues
            arguments:
              pattern: '^[0-9]{4}$'
            mustBe: 0
      - name: wallet_provider
        logicalType: string
        physicalType: VARCHAR
        description: Added 2026-09-15 (non-breaking, optional).
      - name: created_at
        logicalType: string
        physicalType: VARCHAR
        required: true
        description: ISO-8601 UTC timestamp.
        quality:
          - metric: invalidValues
            arguments:
              pattern: '^\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}(\\.\\d+)?Z$'
            mustBe: 0
      - name: updated_at
        logicalType: string
        physicalType: VARCHAR
        required: true
slaProperties:
  - property: latency
    value: 1
    unit: d
    element: payments.updated_at
  - property: frequency
    value: 1
    unit: d
""")
props = CONTRACT_V1["schema"][0]["properties"]
print([p["name"] for p in props])

# %% [markdown]
# ✏️ **Exercise** · The contract is missing `status`. Write `status_property` (a dict, e.g. via `yaml.safe_load`) for a
# required string column whose only valid values are `captured`, `failed` and `refunded`, with a quality rule like
# `currency`'s. Insert it after `method`.

# %%
# BEGIN SOLUTION
status_property = yaml.safe_load("""
name: status
logicalType: string
physicalType: VARCHAR
required: true
description: captured, failed or refunded. A refund updates the row and its updated_at.
quality:
  - metric: invalidValues
    arguments:
      validValues: [captured, failed, refunded]
    mustBe: 0
""")
# END SOLUTION
names = [p["name"] for p in props]
if "status" not in names:
    props.insert(names.index("method") + 1, status_property)
U.check([p["name"] for p in props][5] == "status", "status sits after method")
U.check(status_property["quality"][0]["arguments"]["validValues"] == ["captured", "failed", "refunded"], "status values")

# %% [markdown]
# ## 2 · Validate the feed against the contract (7')
#
# A contract nobody checks is documentation. `validate()` turns each line of the contract into a query: schema checks
# (columns, physical types, required, unique) and the quality rules. Most of it is given; you write the `invalidValues`
# metric.

# %%
OPS = {"mustBe": lambda v, t: v == t, "mustNotBe": lambda v, t: v != t,
       "mustBeGreaterThan": lambda v, t: v > t, "mustBeGreaterOrEqualTo": lambda v, t: v >= t,
       "mustBeLessThan": lambda v, t: v < t, "mustBeLessOrEqualTo": lambda v, t: v <= t,
       "mustBeBetween": lambda v, t: t[0] <= v <= t[1], "mustNotBeBetween": lambda v, t: not t[0] <= v <= t[1]}

def compare(rule, value):
    op = next(k for k in OPS if k in rule)
    return OPS[op](value, rule[op]), f"{op} {rule[op]}"

def q(sql_text):
    return con.sql(sql_text).fetchone()[0]

# %% [markdown]
# ✏️ **Exercise** · `invalid_values(table, column, arguments)` returns the number of **non-null** values in `column` that
# are not in `arguments["validValues"]` or don't match `arguments["pattern"]` (DuckDB: `regexp_full_match(col, pattern)`).

# %%
def invalid_values(table, column, arguments):
    # BEGIN SOLUTION
    if "validValues" in arguments:
        values = ", ".join(f"'{v}'" for v in arguments["validValues"])
        return q(f"SELECT count(*) FROM {table} WHERE {column} IS NOT NULL AND {column} NOT IN ({values})")
    return q(f"SELECT count(*) FROM {table} WHERE {column} IS NOT NULL "
             f"AND NOT regexp_full_match({column}, '{arguments['pattern']}')")
    # END SOLUTION

U.check(invalid_values("payments", "currency", {"validValues": ["SAR"]}) == 0, "all SAR")
U.check(invalid_values("payments", "card_last4", {"pattern": "^[0-9]{3}$"}) > 0, "a wrong pattern finds violations")

# %%
def metric_value(table, rule, column=None):
    m = rule.get("metric")
    if rule.get("type") == "sql":
        return q(rule["query"].replace("{object}", table))
    if m == "rowCount":
        return q(f"SELECT count(*) FROM {table}")
    if m == "nullValues":
        return q(f"SELECT count(*) FROM {table} WHERE {column} IS NULL")
    if m == "duplicateValues":
        cols = ", ".join(rule.get("arguments", {}).get("properties", [column]))
        return q(f"SELECT count(*) - count(DISTINCT ({cols})) FROM {table}")
    if m == "invalidValues":
        return invalid_values(table, column, rule["arguments"])
    raise ValueError(f"unsupported rule: {rule}")

def validate(contract, table):
    obj = contract["schema"][0]
    actual = dict(con.sql(f"DESCRIBE {table}").df()[["column_name", "column_type"]].values)
    rows = []
    def add(check, passed, observed, expected):
        rows.append({"check": check, "passed": bool(passed), "observed": observed, "expected": expected})
    for p in obj["properties"]:
        col = p["name"]
        if col not in actual:
            add(f"{col}: exists", False, "missing", "present")
            continue
        add(f"{col}: type", actual[col] == p["physicalType"], actual[col], p["physicalType"])
        if p.get("required"):
            add(f"{col}: required", metric_value(table, {"metric": "nullValues"}, col) == 0,
                metric_value(table, {"metric": "nullValues"}, col), "0 nulls")
        if p.get("unique"):
            n = metric_value(table, {"metric": "duplicateValues"}, col)
            add(f"{col}: unique", n == 0, n, "0 duplicates")
        for rule in p.get("quality", []):
            v = metric_value(table, rule, col)
            ok, exp = compare(rule, v)
            add(f"{col}: {rule.get('metric', rule.get('type'))}", ok, v, exp)
    for rule in obj.get("quality", []):
        v = metric_value(table, rule)
        ok, exp = compare(rule, v)
        add(rule.get("id", rule.get("metric")), ok, v, exp)
    return pd.DataFrame(rows)

report = validate(CONTRACT_V1, "payments")
print(f"{report.passed.sum()} of {len(report)} checks passed")
report[~report.passed]

# %%
U.check(len(report) >= 20, "every contract line became a check")
U.check(report.loc[report.check == "payments_positive_amount", "observed"].item() == 16, "16 payments with amount <= 0")

# %% [markdown]
# 🔍 The contract found a **real** problem: 16 payments with zero or negative amounts. They come from order lines with
# negative quantities (Lab 2.4's quarantine). Now it's not "the data is weird": it's a **contract breach** with an owner
# (`team.role: owner`) and a support channel. That's the social half of contracts.
#
# ## 3 · The provider proposes v2: breaking or not? (5')
#
# The provider sends a draft v2 of the contract. Some changes are safe for consumers; others will break dashboards and
# dbt models. Rules of thumb:
# - **Breaking**: a property removed or renamed; its `logicalType` changed; a `required` property becoming optional; the
#   set of `validValues` **growing** (consumers' `CASE` statements don't know the new value).
# - **Non-breaking**: a new property; a property becoming required; `validValues` shrinking; descriptions.

# %%
CONTRACT_V2 = copy.deepcopy(CONTRACT_V1)
CONTRACT_V2["version"] = "2.0.0"
p2 = {p["name"]: p for p in CONTRACT_V2["schema"][0]["properties"]}
p2["amount"]["name"] = "amount_sar"                                                          # renamed
p2["card_last4"].update(logicalType="integer", physicalType="INTEGER")                       # type change
p2["status"]["quality"][0]["arguments"]["validValues"].append("chargeback")                  # new status value
p2["wallet_provider"]["required"] = True                                                     # now required
CONTRACT_V2["schema"][0]["properties"].append({"name": "installments", "logicalType": "integer", "physicalType": "INTEGER"})

# %% [markdown]
# ✏️ **Exercise** · Write `diff_contracts(old, new)` returning a list of `(property, change, breaking)` tuples, following
# the rules above. (Helper: `valid_values(p)` returns a property's `validValues` set, or `None`.)

# %%
def valid_values(p):
    for rule in p.get("quality", []):
        if rule.get("metric") == "invalidValues" and "validValues" in rule.get("arguments", {}):
            return set(rule["arguments"]["validValues"])
    return None

# BEGIN SOLUTION
def diff_contracts(old, new):
    o = {p["name"]: p for p in old["schema"][0]["properties"]}
    n = {p["name"]: p for p in new["schema"][0]["properties"]}
    changes = []
    for name in o.keys() - n.keys():
        changes.append((name, "removed", True))
    for name in n.keys() - o.keys():
        changes.append((name, "added", False))           # consumers can ignore a column they don't know
    for name in o.keys() & n.keys():
        a, b = o[name], n[name]
        if a["logicalType"] != b["logicalType"]:
            changes.append((name, f"type {a['logicalType']} -> {b['logicalType']}", True))
        if a.get("required") and not b.get("required"):
            changes.append((name, "now optional", True))
        if not a.get("required") and b.get("required"):
            changes.append((name, "now required", False))
        va, vb = valid_values(a), valid_values(b)
        if va is not None and vb is not None and va != vb:
            changes.append((name, f"valid values {sorted(va)} -> {sorted(vb)}", bool(vb - va)))
    return sorted(changes)
# END SOLUTION

changes = pd.DataFrame(diff_contracts(CONTRACT_V1, CONTRACT_V2), columns=["property", "change", "breaking"])
changes

# %%
U.check(set(changes[changes.breaking].property) == {"amount", "card_last4", "status"}, "three breaking changes")
U.check(set(changes[~changes.breaking].property) == {"amount_sar", "installments", "wallet_provider"}, "three safe changes")
U.check(diff_contracts(CONTRACT_V1, CONTRACT_V1) == [], "no changes, no diff")

# %% [markdown]
# 🔍 A breaking change bumps the **major** version (semantic versioning) and needs a migration plan: publish v2 alongside
# v1, let consumers move, then retire v1. Running `diff_contracts` in the producer's CI turns that into a merge check.
# (The `datacontract` CLI does all of this for real: `datacontract changelog`, `datacontract test`.)
#
# ## 4 · Freshness and volume monitors (6')
#
# Contracts check what you **expect**. Monitors catch what you didn't think of, by watching every table's
# **freshness** (when did it last change?) and **volume** (how many rows arrived?) over time.

# %%
NOW = pd.Timestamp("2026-10-01 06:00", tz="UTC")       # pretend it's the morning after the last load
sla = {s["property"]: s for s in CONTRACT_V1["slaProperties"]}["latency"]
last = pd.Timestamp(q("SELECT max(updated_at) FROM payments"))
lag_h = (NOW - last).total_seconds() / 3600
print(f"payments: last update {last}, {lag_h:.1f} h ago | SLA latency: {sla['value']} {sla['unit']}")
U.check(lag_h <= 24 * sla["value"], "payments feed is fresh enough")

# %% [markdown]
# Volume next: orders per day, with a simulated incident. On Sep 29 an ingestion job silently loaded only 40% of the day.

# %%
con.sql("""CREATE TABLE orders AS SELECT * FROM wh.raw.orders
           WHERE NOT (created_at[:10] = '2026-09-29' AND order_id % 10 >= 4)""")
daily = con.sql("SELECT created_at[:10]::DATE AS day, count(*) AS orders FROM orders GROUP BY 1 ORDER BY 1").df()
daily["day"] = pd.to_datetime(daily["day"])

naive = daily.copy()
naive["expected"] = naive.orders.rolling(7).median().shift(1)        # median of the previous 7 days
naive["alert"] = (naive.orders / naive.expected - 1).abs() > 0.2
print("naive monitor alerts on:", naive[naive.alert].day.dt.strftime("%a %d %b").tolist())

# %% [markdown]
# The naive monitor cries wolf every Thursday and Friday (weekend peaks). Noisy monitors get muted, and then they miss real
# incidents.
#
# ✏️ **Exercise** · Build `smart`: for each day, `expected` = the **median of the same weekday over the previous 4 weeks**,
# and `alert` when the actual differs from it by more than 30%. Then annotate known events: build `explained` = alerts
# on dates listed in `CALENDAR` (planned events), and `incidents` = the other alerts.

# %%
CALENDAR = {"2026-09-23": "Saudi National Day promo (WATANI26)"}

# BEGIN SOLUTION
smart = daily.copy()
smart["expected"] = [
    smart[(smart.day < d) & (smart.day >= d - pd.Timedelta(days=28)) & (smart.day.dt.dayofweek == d.dayofweek)].orders.median()
    for d in smart.day]
smart["deviation"] = smart.orders / smart.expected - 1
smart["alert"] = smart.deviation.abs() > 0.3
alerts = smart[smart.alert]
explained = alerts[alerts.day.dt.strftime("%Y-%m-%d").isin(CALENDAR)]
incidents = alerts[~alerts.day.dt.strftime("%Y-%m-%d").isin(CALENDAR)]
# END SOLUTION
print("explained:", explained.day.dt.strftime("%a %d %b").tolist())
incidents[["day", "orders", "expected", "deviation"]]

# %%
U.check(len(naive[naive.alert]) > 10, "the naive monitor is noisy")
U.check(incidents.day.dt.strftime("%Y-%m-%d").tolist() == ["2026-09-29"], "the smart monitor finds exactly the incident")
U.check(explained.day.dt.strftime("%Y-%m-%d").tolist() == ["2026-09-23"], "and knows about National Day")

# %% [markdown]
# 🔍 This is what observability tools (Monte Carlo, Elementary, Soda, Datadog Data Observability, Dagster's freshness
# checks) automate: learn the pattern per table, alert on deviations, route the alert to the **owner** from the contract.
# `dbt source freshness` does the freshness half for dbt sources (our `_sources.yml` has `loaded_at_field`s).
#
# ## 5 · Impact analysis with dbt lineage (2')
#
# The provider will ship v2 next month. Which dbt models and tests read `raw.payments`, directly or indirectly? That's the
# list of things to update, and the owners to notify.

# %%
U.run_dbt(["parse"], quiet=True)
manifest = json.loads((U.DBT_PROJECT / "target" / "manifest.json").read_text(encoding="utf-8"))
children = manifest["child_map"]

def downstream(node):
    seen, todo = set(), [node]
    while todo:
        for c in children.get(todo.pop(), []):
            if c not in seen:
                seen.add(c)
                todo.append(c)
    return seen

impacted = downstream("source.qafila.raw.payments")
print("models:", sorted(n.split(".")[-1] for n in impacted if n.startswith("model.")))
print("tests:", len([n for n in impacted if n.startswith("test.")]), "| unit tests:", len([n for n in impacted if n.startswith("unit_test.")]))
U.check(any(n.endswith("fct_orders") for n in impacted), "fct_orders depends on payments")

# %% [markdown]
# 🔍 Column-level lineage (dbt Cloud, SQLMesh, OpenLineage-based catalogs) narrows this down to models that use
# `amount` specifically.
#
# ## ✱ Stretch · Turn the contract into dbt tests automatically
#
# Generate a dbt `sources:` YAML block for `raw.payments` from the contract: `not_null` for required, `unique` for unique,
# `accepted_values` for `validValues`. One source of truth, many enforcement points.

# %%
# BEGIN SOLUTION
def contract_to_dbt(contract, source="raw"):
    obj = contract["schema"][0]
    columns = []
    for p in obj["properties"]:
        tests = []
        if p.get("required"):
            tests.append("not_null")
        if p.get("unique"):
            tests.append("unique")
        vv = valid_values(p)
        if vv:
            tests.append({"accepted_values": {"arguments": {"values": sorted(vv)}}})
        col = {"name": p["name"]}
        if p.get("description"):
            col["description"] = p["description"]
        if tests:
            col["data_tests"] = tests
        columns.append(col)
    return {"version": 2, "sources": [{"name": source, "tables": [{"name": obj["name"], "columns": columns}]}]}

print(yaml.safe_dump(contract_to_dbt(CONTRACT_V1), sort_keys=False, allow_unicode=True)[:900])
# END SOLUTION

# %% [markdown]
# ## Checkpoint
#
# 1. Who owns a data contract: the producer or the consumer? Who should be paged when it's breached?
# 2. Why is "a new allowed value" a breaking change for consumers?
# 3. Your volume monitor alerts every Friday. What do you change, and why does it matter?
# 4. Name one thing a contract can't protect you from, and the monitor that would catch it.
