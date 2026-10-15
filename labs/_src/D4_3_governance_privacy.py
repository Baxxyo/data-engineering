# %% [markdown]
# # Lab 4.3 · Governance, security and privacy
#
# Day 4 · Session 3 (Governance, security & privacy) · 25 min. Qafila handles Saudi customers' personal data, so the
# **Personal Data Protection Law (PDPL)** applies, and government partners expect data classified by the
# **NDMO** levels. In this lab you build the technical half of that:
# - **discover** personal data by content, not by column name (national ID / Iqama, IBAN, mobile, email);
# - **classify** every column;
# - **pseudonymize** identifiers so analysts can still join and deduplicate;
# - give each role a **masked view**;
# - honour a **right-to-erasure** request on an Iceberg table, and prove the data is gone.
#
# **Tasks**
# - [ ] 1 · Find personal data by content: validators, not just regexes (6')
# - [ ] 2 · Classify every column (4')
# - [ ] 3 · Pseudonymize with a keyed hash (5')
# - [ ] 4 · Masked views per role (5')
# - [ ] 5 · Right to erasure on an Iceberg table, verified (5')
# - [ ] ✱ Stretch · Redact free-text notes before they reach an LLM
#
# ⚖️ This is an engineering exercise, not legal advice: classification levels and retention rules come from your
# organization's policies and its data protection officer.

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
import hashlib
import hmac
import random
import re

import duckdb
import pandas as pd

pd.set_option("display.max_colwidth", 90)
con = duckdb.connect()
con.sql(f"ATTACH '{U.build_warehouse().as_posix()}' AS wh (READ_ONLY)")
con.sql("CREATE TABLE customers AS SELECT * EXCLUDE (_loaded_at) FROM wh.raw.customers")

# Customer-support notes: free text typed by agents. Personal data hides in places like this.
rng = random.Random(7)
cust = con.sql("SELECT customer_id, email, mobile, national_id, iban FROM customers WHERE iban IS NOT NULL").fetchall()
templates = [
    "Customer called from {mobile} about a late order.",
    "Refund to {iban} requested; identity checked against ID {nid}.",
    "Asked us to change the account email to {email}.",
    "Missing items in the bag, customer was polite, no follow-up needed.",
    "Courier could not find the building; tracking no. {tracking}.",
    "Order {order} delivered to the wrong district.",
]
notes = []
for i in range(400):
    cid, email, mob, nid, iban = rng.choice(cust)
    notes.append((i + 1, cid, rng.choice(templates).format(
        mobile=mob, iban=iban, nid=nid, email=email, order=rng.randint(5_000_000, 5_045_000),
        tracking="".join(str(rng.randint(0, 9)) for _ in range(10)))))
con.sql("CREATE TABLE support_notes (note_id INT, customer_id BIGINT, note VARCHAR)")
con.executemany("INSERT INTO support_notes VALUES (?, ?, ?)", notes)
con.sql("SELECT * FROM support_notes LIMIT 6").df()

# %% [markdown]
# ## 1 · Find personal data by content: validators, not just regexes (6')
#
# Column names lie (`ref`, `notes`, `extra_1`), so discovery tools scan **values**. A regex finds candidates; a **checksum**
# confirms them and removes false positives such as the 10-digit tracking numbers in the notes.
# - Saudi **national ID / Iqama**: 10 digits, first digit `1` (citizen) or `2` (resident), valid **Luhn** check digit.
#   Luhn: from the right, double every second digit (the 2nd, 4th, ...), subtract 9 from results above 9, sum all digits:
#   the total must be divisible by 10.
# - Saudi **IBAN**: `SA` + 22 characters. Move the first 4 characters to the end, replace letters by numbers (A=10 ... Z=35):
#   the resulting integer mod 97 must equal 1 (ISO 13616).
#
# ✏️ **Exercise** · Write `is_saudi_id(s)` and `is_saudi_iban(s)`.

# %%
# BEGIN SOLUTION
def is_saudi_id(s: str) -> bool:
    if not (len(s) == 10 and s.isdigit() and s[0] in "12"):
        return False
    total = 0
    for i, ch in enumerate(reversed(s)):
        d = int(ch)
        if i % 2 == 1:
            d *= 2
            if d > 9:
                d -= 9
        total += d
    return total % 10 == 0

def is_saudi_iban(s: str) -> bool:
    s = s.replace(" ", "").upper()
    if not re.fullmatch(r"SA\d{22}", s):
        return False
    rearranged = s[4:] + s[:4]
    return int("".join(str(int(c, 36)) for c in rearranged)) % 97 == 1
# END SOLUTION

U.check(is_saudi_id(cust[0][3]) and not is_saudi_id("1234567890"), "national IDs pass Luhn; random digits don't")
iban = cust[0][4]
U.check(is_saudi_iban(iban) and not is_saudi_iban(iban[:-1] + str((int(iban[-1]) + 1) % 10)), "one wrong digit breaks the IBAN checksum")

# %%
DETECTORS = {   # type -> (candidate regex, validator)
    "national_id": (r"\b[12]\d{9}\b", is_saudi_id),
    "iban":        (r"\bSA\d{22}\b", is_saudi_iban),
    "mobile":      (r"(?<![\w+])(?:\+?966|0)\s?5\d(?:\s?\d){7}(?!\d)", lambda s: True),   # not inside longer numbers
    "email":       (r"[\w.+-]+@[\w-]+\.[\w.]+", lambda s: True),
}

def find_pii(text: str) -> dict:
    """{pii type: [matches]} for one value; candidates must pass their validator."""
    found = {}
    for kind, (pattern, valid) in DETECTORS.items():
        hits = [m for m in re.findall(pattern, text or "") if valid(m)]
        if hits:
            found[kind] = hits
    return found

def scan_table(table: str, sample: int = 2000) -> pd.DataFrame:
    """For each text column: share of sampled values containing each PII type."""
    cols = [r[0] for r in con.sql(f"DESCRIBE {table}").fetchall() if r[1] == "VARCHAR"]
    rows = []
    for col in cols:
        values = [v for (v,) in con.sql(f"SELECT {col} FROM {table} WHERE {col} IS NOT NULL USING SAMPLE {sample} ROWS (reservoir, 1)").fetchall()]
        hits = pd.Series([k for v in values for k in find_pii(str(v))]).value_counts()
        for kind, n in hits.items():
            rows.append({"table": table, "column": col, "pii": kind, "share": round(n / len(values), 3)})
    return pd.DataFrame(rows)

inventory = pd.concat([scan_table("customers"), scan_table("support_notes")], ignore_index=True)
inventory

# %%
naive_ids = sum(len(re.findall(r"\b[12]\d{9}\b", n)) for _, _, n in notes)
real_ids = sum(len(find_pii(n).get("national_id", [])) for _, _, n in notes)
print(f"10-digit candidates in notes: {naive_ids} | valid national IDs: {real_ids}")
U.check(set(inventory[inventory.table == "support_notes"].pii) == {"national_id", "iban", "mobile", "email"},
        "all four PII types found hiding in free text")
U.check(real_ids < naive_ids, "the Luhn check removes false positives (tracking numbers)")

# %% [markdown]
# ## 2 · Classify every column (4')
#
# Saudi Arabia's **NDMO** data classification uses four levels: **Top Secret, Secret, Restricted, Public**. Below is an
# *illustrative* Qafila policy that maps what we found to a level and a handling rule.

# %%
LEVELS = ["Public", "Restricted", "Secret", "Top Secret"]
POLICY = {   # pii type -> (level, handling)
    "national_id": ("Secret", "pseudonymize; raw value only in the restricted vault"),
    "iban":        ("Secret", "never leaves the payments domain"),
    "mobile":      ("Restricted", "mask for everyone except support"),
    "email":       ("Restricted", "mask for everyone except support"),
    "name":        ("Restricted", "mask outside support"),
}
NAME_HINTS = {"full_name": "name"}   # some PII can't be found by pattern: declare it

# %% [markdown]
# ✏️ **Exercise** · Build `catalog`: one row per column of `customers` and `support_notes` with `table`, `column`,
# `pii` (sorted list of PII types from `inventory` plus `NAME_HINTS`), `level` (the **highest** level among them, by
# `LEVELS` order) and `handling` (of that highest one). A column with no PII of its own in a table that holds personal
# data is still `"Restricted"` ("personal data by association": `city` next to a name identifies someone); only columns of
# tables without personal data are `"Public"`.

# %%
# BEGIN SOLUTION
rows = []
for table in ["customers", "support_notes"]:
    table_has_pii = table in set(inventory.table)
    for (col,) in con.sql(f"SELECT column_name FROM (DESCRIBE {table})").fetchall():
        kinds = sorted(set(inventory[(inventory.table == table) & (inventory.column == col)].pii)
                       | ({NAME_HINTS[col]} if col in NAME_HINTS else set()))
        if kinds:
            top = max(kinds, key=lambda k: LEVELS.index(POLICY[k][0]))
            level, handling = POLICY[top]
        else:
            level, handling = ("Restricted", "personal data by association") if table_has_pii else ("Public", "-")
        rows.append({"table": table, "column": col, "pii": kinds, "level": level, "handling": handling})
catalog = pd.DataFrame(rows)
# END SOLUTION
catalog

# %%
lvl = catalog.set_index(["table", "column"]).level
U.check(lvl["customers", "national_id"] == "Secret" and lvl["customers", "iban"] == "Secret", "IDs and IBANs are Secret")
U.check(lvl["support_notes", "note"] == "Secret", "free text inherits its most sensitive content")
U.check((catalog.level != "Public").all(), "nothing in a personal-data table is Public on its own")

# %% [markdown]
# 🔍 In a real platform these labels become **tags** in the catalog (Unity Catalog, Polaris, DataHub, OpenMetadata) and
# drive masking and access policies automatically. The classification is code: review it like code.
#
# ## 3 · Pseudonymize with a keyed hash (5')
#
# Analysts need to count **people**, not accounts (Day 2: 58 people registered twice), without seeing national IDs.
# Replace each ID by a **token**. A plain SHA-256 isn't enough: anyone with a list of IDs can hash them and match.

# %%
plain = {hashlib.sha256(nid.encode()).hexdigest(): cid for cid, nid in con.sql("SELECT customer_id, national_id FROM customers").fetchall()}
leaked_list = [nid for (nid,) in con.sql("SELECT national_id FROM customers USING SAMPLE 500 ROWS (reservoir, 3)").fetchall()]
reidentified = sum(hashlib.sha256(n.encode()).hexdigest() in plain for n in leaked_list)
print(f"attacker with 500 leaked IDs re-identifies {reidentified} 'anonymous' customers from plain hashes")

# %% [markdown]
# ✏️ **Exercise** · Write `pseudonymize(value, key)`: **HMAC-SHA256** of the value with a secret key (`hmac.new(key, msg,
# hashlib.sha256)`), returned as the first 20 hex characters. Then register it as a DuckDB function `pseudo(VARCHAR)` with
# the key `PSEUDO_KEY` and count `people` = distinct tokens and `twice` = tokens with more than one account.

# %%
PSEUDO_KEY = os.environ.get("QAFILA_PSEUDO_KEY", "lab-only-key-keep-real-ones-in-a-secret-manager").encode()

# BEGIN SOLUTION
def pseudonymize(value: str, key: bytes = PSEUDO_KEY) -> str:
    return hmac.new(key, value.encode(), hashlib.sha256).hexdigest()[:20]

con.create_function("pseudo", lambda v: pseudonymize(v), ["VARCHAR"], "VARCHAR")
people, twice = con.sql("""
    SELECT count(DISTINCT t), count(DISTINCT t) FILTER (WHERE n > 1)
    FROM (SELECT pseudo(national_id) AS t, count(*) OVER (PARTITION BY pseudo(national_id)) AS n FROM customers)
""").fetchone()
# END SOLUTION
accounts = con.sql("SELECT count(*) FROM customers").fetchone()[0]
print(f"{accounts:,} accounts, {people:,} people, {twice} people with more than one account")
U.check(twice == 58, "the 58 duplicate people are still detectable, without seeing a single ID")
U.check(pseudonymize("1000000008") == pseudonymize("1000000008") != pseudonymize("1000000008", b"other-key"), "stable per key")
U.check(sum(pseudonymize(n) in plain for n in leaked_list) == 0, "without the key, the leaked list matches nothing")

# %% [markdown]
# 🔍 Pseudonymized data is **still personal data** under PDPL (with the key, it's reversible by matching). It lowers risk
# and is the right default for analytics; true **anonymization** (aggregation, k-anonymity, differential privacy) is a
# much higher bar. Rotate the key and you break all joins: store it in a secret manager, version it.
#
# ## 4 · Masked views per role (5')
#
# DuckDB has no built-in column security, so we model it the classic way: each role may only read its own **views**.
# Warehouses do the same with grants plus masking policies (Snowflake, BigQuery, Unity Catalog column masks).

# %%
con.sql("CREATE SCHEMA analyst; CREATE SCHEMA support")
con.sql("""CREATE MACRO mask_mobile(m) AS
           CASE WHEN m IS NULL THEN NULL ELSE '+9665•••••' || right(regexp_replace(m, '\\D', '', 'g'), 2) END""")
con.sql("CREATE MACRO mask_email(e) AS left(e, 1) || '•••@' || split_part(e, '@', 2)")
con.sql("""CREATE VIEW support.customers AS
           SELECT customer_id, full_name, email, mask_mobile(mobile) AS mobile, city, district FROM main.customers""")
GRANTS = {"support": "support", "analyst": "analyst"}

def query_as(role: str, sql_text: str) -> pd.DataFrame:
    """Toy access control: a role may only reference its own schema."""
    referenced = set(re.findall(r"\b(\w+)\.\w+", sql_text))
    if referenced - {GRANTS[role]}:
        raise PermissionError(f"{role} may not read {referenced - {GRANTS[role]}}")
    return con.sql(sql_text).df()

query_as("support", "SELECT * FROM support.customers LIMIT 3")

# %% [markdown]
# ✏️ **Exercise** · Create `analyst.customers`: `person_token` (pseudonymized national ID), `customer_id`, `city`,
# `district`, `email_domain` (the part after `@`), `created_at`. No names, no mobiles, no IBANs, no raw IDs.

# %%
# BEGIN SOLUTION
con.sql("""CREATE VIEW analyst.customers AS
           SELECT pseudo(national_id) AS person_token, customer_id, city, district,
                  lower(split_part(email, '@', 2)) AS email_domain, created_at
           FROM main.customers""")
# END SOLUTION
sample = query_as("analyst", "SELECT * FROM analyst.customers")
print(query_as("analyst", "SELECT email_domain, count(*) AS n FROM analyst.customers GROUP BY 1 ORDER BY n DESC LIMIT 3"))

# %%
leaks = sample.map(lambda v: bool(find_pii(str(v)))).sum()
U.check(int(leaks.sum()) == 0, "the PII scanner finds nothing in the analyst view")
try:
    query_as("analyst", "SELECT national_id FROM main.customers")
    U.check(False, "analysts can't read the base table")
except PermissionError as e:
    U.check(True, f"blocked: {e}")

# %% [markdown]
# ## 5 · Right to erasure on an Iceberg table, verified (5')
#
# A customer asks Qafila to delete their data (PDPL gives data subjects this right). Our lakehouse keeps customers in
# Iceberg. Day 3 showed the catch: a delete creates a new snapshot, but **old snapshots and files still hold the row**.

# %%
import pyarrow.compute as pc

LAKE = U.lab_dir("D4_3", reset=True) / "lakehouse"
catalog_ice = U.get_catalog(LAKE)
catalog_ice.create_namespace("crm")
arrow_customers = con.sql("SELECT * FROM customers").to_arrow_table()
tbl = catalog_ice.create_table("crm.customers", schema=arrow_customers.schema)
tbl.append(arrow_customers)

subject = cust[0]                                   # the customer exercising their right
subject_id, subject_nid = subject[0], subject[3]
tbl.delete(f"customer_id = {subject_id}")
first_snapshot = tbl.snapshots()[0].snapshot_id
still_there = tbl.scan(snapshot_id=first_snapshot, row_filter=f"customer_id = {subject_id}").to_arrow().num_rows

def files_containing(value: str) -> list[str]:
    """Every Parquet file on disk under the table location whose national_id column contains `value`."""
    hits = []
    for f in (LAKE / "crm" / "customers" / "data").rglob("*.parquet"):
        n = duckdb.sql(f"SELECT count(*) FROM '{f.as_posix()}' WHERE national_id = '{value}'").fetchone()[0]
        if n:
            hits.append(f.name)
    return hits

print(f"current table: {tbl.scan(row_filter=f'customer_id = {subject_id}').to_arrow().num_rows} rows | "
      f"time travel: {still_there} row | files on disk containing the ID: {files_containing(subject_nid)}")

# %% [markdown]
# ✏️ **Exercise** · Finish the erasure: (1) expire every snapshot except the current one
# (`tbl.maintenance.expire_snapshots().by_ids([...]).commit()`); (2) delete every data file on disk that no remaining
# snapshot references (compare the files under `data/` with `tbl.inspect.all_files()`'s `file_path`s), like Spark's
# `remove_orphan_files`. Then `files_containing(subject_nid)` must be empty.

# %%
# BEGIN SOLUTION
current = tbl.current_snapshot().snapshot_id
old = [s.snapshot_id for s in tbl.snapshots() if s.snapshot_id != current]
tbl.maintenance.expire_snapshots().by_ids(old).commit()
tbl = catalog_ice.load_table("crm.customers")
referenced = {Path(p).resolve() for p in tbl.inspect.all_files().to_pandas().file_path}
removed = []
for f in (LAKE / "crm" / "customers" / "data").rglob("*.parquet"):
    if f.resolve() not in referenced:
        f.unlink()
        removed.append(f.name)
print("removed orphan files:", removed)
# END SOLUTION

# %%
U.check(len(tbl.snapshots()) == 1, "time travel to the old version is gone")
U.check(files_containing(subject_nid) == [], "no file on disk contains the national ID any more")
U.check(tbl.scan().to_arrow().num_rows == len(arrow_customers) - 1, "everyone else is still there")

# %% [markdown]
# 🔍 Still on your erasure checklist in real life: **column statistics** in old manifest files (min/max bounds can hold a
# value), **backups** and replicas, **derived tables** (dbt rebuilds them; incremental models need a delete step), files
# exported to partners, and the **logs** of the erasure itself (keep the request, not the data). Doing this per request
# is expensive: many teams batch erasures weekly, or use **crypto-shredding** (encrypt each person's data with their own
# key, then delete the key).
#
# ## ✱ Stretch · Redact free-text notes before they reach an LLM
#
# Lab 4.4 sends text to a language model. Write `redact(text)` that replaces every detected PII value with a typed
# placeholder (`[NATIONAL_ID]`, `[IBAN]`, `[MOBILE]`, `[EMAIL]`) and check that `find_pii` finds nothing in the result.

# %%
# BEGIN SOLUTION
def redact(text: str) -> str:
    for kind, values in find_pii(text).items():
        for v in values:
            text = text.replace(v, f"[{kind.upper()}]")
    return text

redacted = [redact(n) for _, _, n in notes]
print("\n".join(redacted[:6]))
U.check(not any(find_pii(t) for t in redacted), "no PII left in any note")
# END SOLUTION

# %% [markdown]
# ## Checkpoint
#
# 1. Why validate with a checksum (Luhn, mod-97) instead of a regex alone?
# 2. Pseudonymized data: is it still personal data under PDPL? What does that mean for where you can store it?
# 3. Masking in a view vs. masking policies in the warehouse: which is harder to bypass, and why?
# 4. List everything that must happen, beyond `DELETE`, before you can tell a customer their data is erased.
