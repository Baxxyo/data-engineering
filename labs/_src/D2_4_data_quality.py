# %% [markdown]
# # Lab 2.4 · Make it trustworthy: tests, contracts, validation
#
# Day 2 · Session 4 (Testing & data quality) · 25 min. What you'll do:
# - run the Qafila project's tests and learn to read **pass / warn / error**;
# - add a test that catches a **real** problem, and decide whether it should warn or fail;
# - enforce a **model contract** and watch it stop a breaking change;
# - write a dbt **unit test** for a cleaning rule;
# - validate the supplier price files with **Pandera** before they enter the warehouse, quarantining bad rows.
#
# Like Lab 2.3, you work on a copy of the project and the warehouse.
#
# **Tasks**
# - [ ] 1 · Run the existing tests; read warn vs. error (4')
# - [ ] 2 · Add a relationships test that catches real orphans (6')
# - [ ] 3 · Enforce a model contract; watch a type change break the build (5')
# - [ ] 4 · Write a dbt unit test for mobile normalization (5')
# - [ ] 5 · Validate supplier files with Pandera; quarantine bad rows (5')
# - [ ] ✱ Stretch · Store test failures in the warehouse and query them

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
import shutil
import warnings

import duckdb
import pandas as pd
import polars as pl

warnings.filterwarnings("ignore", category=FutureWarning)          # pyspark's pandas-3 notice, via pandera
import pandera.polars as pa

U.build_warehouse()
OUT = U.lab_dir("D2_4", reset=True)
PROJ = OUT / "dbt_qafila"
DB = OUT / "qafila.duckdb"
shutil.copytree(U.DBT_PROJECT, PROJ, ignore=shutil.ignore_patterns("target", "logs", "dbt_packages"))
shutil.copy(U.WAREHOUSE_DB, DB)


def dbt(*args, quiet=True):
    """Run dbt on this lab's project; return a table of results instead of raising on test failures."""
    from dbt.cli.main import dbtRunner

    os.environ["QAFILA_DUCKDB"] = DB.resolve().as_posix()
    cli = (["--quiet"] if quiet else []) + list(args) + ["--project-dir", str(PROJ), "--profiles-dir", str(PROJ)]
    res = dbtRunner().invoke(cli)
    if res.result is None or not hasattr(res.result, "results"):
        raise RuntimeError(f"dbt {' '.join(args)} could not run: {res.exception}")
    return pd.DataFrame([{"node": r.node.name, "status": str(r.status), "failures": r.failures, "message": (r.message or "")[:90]}
                         for r in res.result.results])


def sql(query):
    with duckdb.connect(str(DB)) as c:
        return c.sql(query).df()


dbt("seed")
build = dbt("build")
build.status.value_counts()

# %% [markdown]
# ## 1 · Run the existing tests; read warn vs. error (4')
#
# `dbt build` ran every model **and** its tests in DAG order: a model's tests run right after it's built, and if an
# error-severity test fails, everything downstream is **skipped**. Let's look at the tests that didn't pass:

# %%
build[build.status != "pass"]

# %% [markdown]
# Three warnings, and each one is a real issue from your Day 1 issue log:
# - `positive_stg_order_items_quantity`: 142 negative quantities (they're routed to `quarantine.qrn_order_items`);
# - `positive_stg_supplier_prices_unit_cost_sar`: 4 zero or negative supplier costs in week 39;
# - `assert_order_totals_match_payments`: a singular test (plain SQL in `tests/`) finding orders whose payment differs from the total.
#
# **Warn or error?** Error stops the pipeline, which is right when bad data would do more harm than stale data (wrong
# invoices, a broken fact grain). Warn keeps data flowing and makes noise, which is right for known, tolerated issues
# that someone is following up. Every warning needs an owner, or it becomes wallpaper.
#
# ## 2 · Add a relationships test that catches real orphans (6')
#
# `stg_orders.customer_id` should point to a customer. We know it doesn't always (erased customers).
#
# A model is described in **one** YAML entry, so the test goes where `stg_orders` is already described: in
# `models/staging/_staging.yml`, under `stg_orders` → `columns`.
#
# ✏️ **Exercise** · Write `NEW_TEST`, the YAML for a `customer_id` column with a `relationships` test to
# `ref('stg_customers')`, field `customer_id`, with `severity: warn` (under `config:`). The cell inserts it after the
# existing `store_id` column. Mind the indentation: it must line up with `- name: store_id`.

# %%
staging_yml = PROJ / "models/staging/_staging.yml"
text = staging_yml.read_text(encoding="utf-8")
anchor = """      - name: store_id
        data_tests:
          - relationships:
              arguments:
                to: ref('stg_stores')
                field: store_id
"""
assert anchor in text
# BEGIN SOLUTION
NEW_TEST = """      - name: customer_id
        data_tests:
          - relationships:
              arguments:
                to: ref('stg_customers')
                field: customer_id
              config:
                severity: warn
"""
# END SOLUTION
staging_yml.write_text(text.replace(anchor, anchor + NEW_TEST), encoding="utf-8")
res = dbt("test", "--select", "stg_orders")
res

# %%
orphans = res[res.node.str.startswith("relationships_stg_orders_customer_id")]
U.check(len(orphans) == 1 and orphans.failures.iloc[0] == 1040, "the test finds the 1,040 orders of erased customers")

# %% [markdown]
# 🔍 This test **should** warn, not fail: orphans are expected after PDPL erasure, and the marts handle them with the
# unknown member (`customer_id = -1`). The same test with `error` on `fct_order_items.customer_id → dim_customers` (already
# in `_marts.yml`) guards the star schema itself, where an orphan would be a bug.
#
# ## 3 · Enforce a model contract; watch a type change break the build (5')
#
# `fct_order_items` has an **enforced contract** in `models/marts/_marts.yml`: dbt checks the model's column names and types
# against the YAML **before** it replaces the table. Downstream dashboards can rely on the shape.

# %%
print("\n".join(l for l in (PROJ / "models/marts/_marts.yml").read_text(encoding="utf-8").splitlines()[:28]))

# %% [markdown]
# A colleague "improves" the model: quantities can be fractional now (weighed vegetables), so they change the column to a decimal.
#
# ✏️ **Exercise** · Make that change in a copy of the model text (`i.quantity,` → `i.quantity::double as quantity,`), write it,
# run `dbt run --select fct_order_items --full-refresh`, and store the result table in `broken`. Then restore the original file.

# %%
fct_path = PROJ / "models/marts/fct_order_items.sql"
original = fct_path.read_text(encoding="utf-8")
# BEGIN SOLUTION
fct_path.write_text(original.replace("    i.quantity,\n", "    i.quantity::double as quantity,\n"), encoding="utf-8")
broken = dbt("run", "--select", "fct_order_items", "--full-refresh")
fct_path.write_text(original, encoding="utf-8")
# END SOLUTION
print(broken.message.iloc[0])
U.check(broken.status.iloc[0] == "error", "the contract stopped the change before it reached the table")
U.check(sql("SELECT data_type FROM information_schema.columns WHERE table_name = 'fct_order_items' AND column_name = 'quantity'").data_type[0] == "BIGINT",
        "the published table still has the agreed type")

# %% [markdown]
# 🔍 The fix isn't to delete the contract: it's a **versioned** change. Agree on it with consumers, update the contract,
# and (for big changes) publish `fct_order_items_v2` alongside v1 for a while. dbt supports model versions for exactly this.
#
# ## 4 · Write a dbt unit test for mobile normalization (5')
#
# Data tests check the data you **have**. Unit tests check the logic against the edge cases you **can imagine**, with
# fixed inputs and expected outputs, before any real data arrives. Look at the existing unit tests in `_staging.yml`
# (bottom of the file) for the format.
#
# ✏️ **Exercise** · Write `models/staging/_unit_customers.yml` with a unit test `test_mobiles_are_normalized_to_e164` on
# `stg_customers`. Inputs (`source('raw', 'customers')`): customer IDs 1–5 with mobiles `+966501234567`, `0501234567`,
# `966501234567`, `+966 50 123 4567`, `12345` (and any `national_id`). Expect `mobile_e164` = `+966501234567` for the first four
# and `null` for the last.

# %%
# BEGIN SOLUTION
UNIT = """
version: 2
unit_tests:
  - name: test_mobiles_are_normalized_to_e164
    model: stg_customers
    given:
      - input: source('raw', 'customers')
        rows:
          - {customer_id: 1, national_id: "1000000001", mobile: "+966501234567"}
          - {customer_id: 2, national_id: "1000000002", mobile: "0501234567"}
          - {customer_id: 3, national_id: "1000000003", mobile: "966501234567"}
          - {customer_id: 4, national_id: "1000000004", mobile: "+966 50 123 4567"}
          - {customer_id: 5, national_id: "1000000005", mobile: "12345"}
    expect:
      rows:
        - {customer_id: 1, mobile_e164: "+966501234567"}
        - {customer_id: 2, mobile_e164: "+966501234567"}
        - {customer_id: 3, mobile_e164: "+966501234567"}
        - {customer_id: 4, mobile_e164: "+966501234567"}
        - {customer_id: 5, mobile_e164: null}
"""
# END SOLUTION
(PROJ / "models/staging/_unit_customers.yml").write_text(UNIT.strip() + "\n", encoding="utf-8")
unit = dbt("test", "--select", "stg_customers,test_type:unit")
unit

# %%
U.check((unit.status == "pass").all() and unit.node.str.contains("e164").any(), "the mobile normalization passes its unit test")

# %% [markdown]
# Try breaking the macro on purpose (e.g. drop the `(966|0)?` group in `macros/text.sql`) and re-run: the unit test points
# at the exact rows that changed. Unit tests are cheap; put one on every tricky cleaning rule.
#
# ## 5 · Validate supplier files with Pandera; quarantine bad rows (5')
#
# Some checks belong **before** the warehouse: a supplier file with garbage should be caught at the door, with a
# clear report back to the supplier. Pandera validates DataFrames (pandas, Polars, PySpark) against a schema written as a class.
#
# Parse all four weekly files into one Polars frame (handling the drift we found on Day 1):

# %%
def read_supplier_file(path: Path) -> pl.DataFrame:
    df = pl.read_csv(path, infer_schema=False, encoding="utf8-lossy")             # everything as text first
    cost = "cost_sar" if "cost_sar" in df.columns else "unit_cost"
    digits = str.maketrans("٠١٢٣٤٥٦٧٨٩٫", "0123456789.")
    return df.select(
        pl.col("supplier_id"), pl.col("sku").str.strip_chars(), pl.col("product_name_ar"),
        pl.col(cost).map_elements(lambda s: s.translate(digits).replace(",", "."), return_dtype=pl.String)
          .cast(pl.Float64, strict=False).alias("unit_cost_sar"),
        (pl.col("currency") if "currency" in df.columns else pl.lit("SAR")).alias("currency"),
        pl.col("valid_from").str.to_date(), pl.col("min_order_qty").cast(pl.Int64),
        pl.lit(path.name).alias("file"),
    )

prices = pl.concat([read_supplier_file(p) for p in sorted((U.RAW / "suppliers").glob("prices_*.csv"))])
known_skus = sql("SELECT trim(sku) AS sku FROM raw.products").sku.tolist()
print(prices.shape)
prices.head(3)

# %% [markdown]
# ✏️ **Exercise** · Complete the Pandera model `SupplierPrice`: `supplier_id` starts with `SUP-`; `sku` is one of `known_skus`;
# `unit_cost_sar` is greater than 0 (and not null); `currency` is `SAR`; `min_order_qty` is one of 6, 12, 24, 48.
# Then validate `prices` with `lazy=True` (collect **all** failures instead of stopping at the first) and catch the
# `SchemaErrors`: store its `failure_cases` in `failures`.

# %%
class SupplierPrice(pa.DataFrameModel):
    # BEGIN SOLUTION
    supplier_id: str = pa.Field(str_startswith="SUP-")
    sku: str = pa.Field(isin=known_skus)
    unit_cost_sar: float = pa.Field(gt=0, nullable=False)
    currency: str = pa.Field(isin=["SAR"])
    min_order_qty: int = pa.Field(isin=[6, 12, 24, 48])
    # END SOLUTION

    class Config:
        strict = False          # other columns are allowed


failures = None
try:
    SupplierPrice.validate(prices, lazy=True)
except pa.errors.SchemaErrors as e:
    failures = e.failure_cases
failures

# %% [markdown]
# Now **quarantine**: valid rows go on to the warehouse, invalid rows go to a quarantine file with the reason, and the
# supplier gets the list. `failure_cases` gives row `index` positions in `prices`.

# %%
bad_idx = failures["index"].unique().to_list()
reasons = failures.group_by("index").agg(pl.col("check").str.join("; ").alias("reason"))
prices_i = prices.with_row_index("index").with_columns(pl.col("index").cast(pl.Int32))
quarantine = prices_i.join(reasons, on="index", how="inner")
clean = prices_i.join(reasons, on="index", how="anti").drop("index")
quarantine.write_csv(OUT / "supplier_prices_quarantine.csv")
clean.write_parquet(OUT / "supplier_prices_clean.parquet")
print(f"{clean.height} clean rows, {quarantine.height} quarantined")
quarantine.select("file", "sku", "unit_cost_sar", "reason")

# %%
U.check(quarantine.height == 7, "4 bad costs + 3 unknown SKUs quarantined")
U.check(clean.filter(pl.col("file") == "prices_2026-W38.csv").height == 160, "week 38's commas and Arabic digits parsed, not rejected")

# %% [markdown]
# 🔍 Week 38 passes now because we **parse** it correctly; week 39 passes **structurally** (we mapped `cost_sar`) but
# seven rows are quarantined. Validation at the door, tests in the warehouse, and contracts with producers (Day 4)
# are three layers of the same defence.
#
# ## ✱ Stretch · Store test failures in the warehouse and query them
#
# `dbt test --store-failures` writes the failing rows of each test into tables so
# you can inspect, alert on, or report them (here in a `dbt_test__audit` schema). Run it for the warning tests and look at the stored rows of the payments test.

# %%
# BEGIN SOLUTION
dbt("test", "--select", "assert_order_totals_match_payments positive_stg_order_items_quantity", "--store-failures")
audit = sql("SELECT table_schema, table_name FROM information_schema.tables WHERE table_schema LIKE '%audit%'")
display(audit)
display(sql(f"SELECT * FROM {audit.table_schema[0]}.assert_order_totals_match_payments ORDER BY abs(difference) DESC LIMIT 5"))
# END SOLUTION

# %% [markdown]
# ## Checkpoint
#
# 1. Name one Qafila check that should **error** and one that should **warn**, and say who gets told in each case.
# 2. What does a contract protect that a `not_null` test doesn't?
# 3. Why write unit tests if data tests already run on every build?
# 4. Where in the pipeline would you validate the supplier files if they arrived by SFTP every Monday at 06:00?
