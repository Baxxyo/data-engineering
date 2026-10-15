"""Capstone starter: the Qafila Lakehouse as one Dagster code location.

Sources -> raw (DuckDB) -> dbt (staging, marts) -> gold tables published to Iceberg, with
asset checks from a data contract, a volume monitor and a nightly schedule.

    uv run python capstone/pipeline.py              # materialize everything once, in-process
    uv run dagster dev -f capstone/pipeline.py      # the UI: lineage, runs, checks, schedules

Run from labs/. Re-running is safe: every asset replaces what it owns.
"""

import os
import sys
import warnings
from pathlib import Path

warnings.filterwarnings("ignore")
LABS = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(LABS))

import dagster as dg  # noqa: E402
import yaml  # noqa: E402
from dagster_dbt import DbtCliResource, dbt_assets  # noqa: E402

import de_utils as U  # noqa: E402

HOME = U.ARTIFACTS / "capstone"
DB = HOME / "qafila.duckdb"
LAKE = HOME / "lakehouse"
CONTRACT = Path(__file__).with_name("contracts") / "payments.odcs.yaml"
HOME.mkdir(parents=True, exist_ok=True)
os.environ["QAFILA_DUCKDB"] = DB.as_posix()          # dbt's profiles.yml reads this


def sql(query: str, params: list | None = None):
    con = U.connect(DB)
    con.execute("SET python_enable_replacements = false")
    try:
        return con.execute(query, params or []).fetchall()
    finally:
        con.close()


# ---------------------------------------------------------------------------
# 1. Ingestion: one asset per source table (keys match dbt sources: raw.<table>)
# ---------------------------------------------------------------------------
RAW = U.RAW.as_posix()
APP_TABLES = ["customers", "stores", "products", "orders", "order_items"]


def _load_app_table(table: str):
    sql("INSTALL sqlite; LOAD sqlite;")
    sql("CREATE SCHEMA IF NOT EXISTS raw")
    con = U.connect(DB)
    try:
        con.sql(f"ATTACH '{RAW}/qafila_oltp.sqlite' AS app (TYPE sqlite, READ_ONLY)")
        con.sql(f"CREATE OR REPLACE TABLE raw.{table} AS SELECT *, now()::TIMESTAMP AS _loaded_at FROM app.{table}")
        return con.sql(f"SELECT count(*) FROM raw.{table}").fetchone()[0]
    finally:
        con.close()


def _make_app_asset(table: str):
    @dg.asset(key=["raw", table], group_name="ingest", kinds={"sqlite", "duckdb"}, description=f"App database: {table}")
    def _asset():
        return dg.MaterializeResult(metadata={"rows": _load_app_table(table)})
    return _asset


app_assets = [_make_app_asset(t) for t in APP_TABLES]

FILE_SOURCES = {
    "payments": f"""read_json('{RAW}/api/payments.jsonl', format='newline_delimited', columns={{
        payment_id: 'VARCHAR', order_id: 'BIGINT', amount: 'DOUBLE', currency: 'VARCHAR', method: 'VARCHAR',
        status: 'VARCHAR', card_last4: 'VARCHAR', created_at: 'VARCHAR', updated_at: 'VARCHAR', wallet_provider: 'VARCHAR'}})""",
    "delivery_events": f"""read_json('{RAW}/events/delivery_events.jsonl', format='newline_delimited', columns={{
        event_id: 'VARCHAR', order_id: 'BIGINT', store_id: 'VARCHAR', city: 'VARCHAR', courier_id: 'VARCHAR',
        event_type: 'VARCHAR', event_time: 'VARCHAR', lat: 'DOUBLE', lon: 'DOUBLE', ingest_time: 'VARCHAR'}})""",
    "supplier_prices": f"read_csv('{RAW}/suppliers/*.csv', union_by_name=true, all_varchar=true, filename=true)",
    "reviews": f"read_csv('{RAW}/reviews.csv')",
}


def _make_file_asset(table: str, reader: str):
    @dg.asset(key=["raw", table], group_name="ingest", kinds={"duckdb"}, description=f"File/API source: {table}")
    def _asset():
        sql("CREATE SCHEMA IF NOT EXISTS raw")
        sql(f"CREATE OR REPLACE TABLE raw.{table} AS SELECT * FROM {reader}")
        return dg.MaterializeResult(metadata={"rows": sql(f"SELECT count(*) FROM raw.{table}")[0][0]})
    return _asset


file_assets = [_make_file_asset(t, r) for t, r in FILE_SOURCES.items()]


# ---------------------------------------------------------------------------
# 2. Contract checks on the payments feed (subset of ODCS: rowCount, duplicateValues, invalidValues, sql)
# ---------------------------------------------------------------------------
@dg.asset_check(asset=dg.AssetKey(["raw", "payments"]), description="ODCS contract: capstone/contracts/payments.odcs.yaml")
def payments_contract():
    contract = yaml.safe_load(CONTRACT.read_text(encoding="utf-8"))
    obj = contract["schema"][0]
    failures, checked = [], 0
    rules = [(None, r) for r in obj.get("quality", [])] + [(p["name"], r) for p in obj["properties"] for r in p.get("quality", [])]
    for col, rule in rules:
        if rule.get("type") == "sql":
            value = sql(rule["query"].replace("{object}", "raw.payments"))[0][0]
        elif rule["metric"] == "rowCount":
            value = sql("SELECT count(*) FROM raw.payments")[0][0]
        elif rule["metric"] == "duplicateValues":
            cols = ", ".join(rule["arguments"]["properties"])
            value = sql(f"SELECT count(*) - count(DISTINCT ({cols})) FROM raw.payments")[0][0]
        elif rule["metric"] == "invalidValues" and "validValues" in rule["arguments"]:
            allowed = ", ".join(f"'{v}'" for v in rule["arguments"]["validValues"])
            value = sql(f"SELECT count(*) FROM raw.payments WHERE {col} IS NOT NULL AND {col} NOT IN ({allowed})")[0][0]
        elif rule["metric"] == "invalidValues":
            value = sql(f"SELECT count(*) FROM raw.payments WHERE {col} IS NOT NULL AND NOT regexp_full_match({col}, ?)",
                        [rule["arguments"]["pattern"]])[0][0]
        else:
            continue
        checked += 1
        op, target = next((k, v) for k, v in rule.items() if k.startswith("must"))
        ok = {"mustBe": value == target, "mustBeGreaterThan": value > target, "mustBeLessThan": value < target}[op]
        if not ok:
            failures.append(f"{rule.get('id', col)}: {value} ({op} {target})")
    return dg.AssetCheckResult(passed=not failures, severity=dg.AssetCheckSeverity.WARN,
                               metadata={"rules_checked": checked, "failures": "; ".join(failures) or "none"})


# ---------------------------------------------------------------------------
# 3. dbt: staging, intermediate, marts (every dbt test becomes an asset check)
# ---------------------------------------------------------------------------
MANIFEST = U.DBT_PROJECT / "target" / "manifest.json"
if not MANIFEST.exists():
    U.run_dbt(["parse"], db_path=DB, quiet=True)


@dbt_assets(manifest=MANIFEST)
def qafila_dbt(context: dg.AssetExecutionContext, dbt: DbtCliResource):
    yield from dbt.cli(["build"], context=context).stream()


# ---------------------------------------------------------------------------
# 4. Gold: publish marts to Iceberg so any engine can read them; monitor volume
# ---------------------------------------------------------------------------
@dg.asset(key=["gold", "daily_city_sales"], group_name="publish", kinds={"iceberg"},
          deps=[dg.AssetKey(["marts", "mart_daily_city_sales"])])
def gold_daily_city_sales():
    """The dashboard mart as an Iceberg table (full refresh: overwrite)."""
    con = U.connect(DB)
    try:
        data = con.sql("SELECT * FROM marts.mart_daily_city_sales ORDER BY order_date, city").to_arrow_table()
    finally:
        con.close()
    catalog = U.get_catalog(LAKE)
    catalog.create_namespace_if_not_exists("gold")
    if catalog.table_exists("gold.daily_city_sales"):
        table = catalog.load_table("gold.daily_city_sales")
        table.overwrite(data)
    else:
        table = catalog.create_table("gold.daily_city_sales", schema=data.schema)
        table.append(data)
    return dg.MaterializeResult(metadata={"rows": data.num_rows, "snapshots": len(table.snapshots()),
                                          "metadata_location": table.metadata_location})


@dg.asset_check(asset=dg.AssetKey(["marts", "fct_orders"]), description="Volume: no day below 70% of its weekday's 4-week median")
def orders_volume():
    rows = sql("""
        WITH d AS (SELECT order_date, count(*)::DOUBLE AS n FROM marts.fct_orders GROUP BY 1)
        SELECT order_date, n,
               (SELECT median(n) FROM d d2 WHERE d2.order_date < d.order_date AND d2.order_date >= d.order_date - 28
                  AND dayofweek(d2.order_date) = dayofweek(d.order_date)) AS expected
        FROM d ORDER BY order_date""")
    low = [str(day) for day, n, exp in rows if exp and n < 0.7 * exp]
    return dg.AssetCheckResult(passed=not low, metadata={"days_checked": len(rows), "low_days": ", ".join(low) or "none"})


# ---------------------------------------------------------------------------
# 5. The code location
# ---------------------------------------------------------------------------
everything = dg.define_asset_job("refresh_all", selection=dg.AssetSelection.all())
defs = dg.Definitions(
    assets=[*app_assets, *file_assets, qafila_dbt, gold_daily_city_sales],
    asset_checks=[payments_contract, orders_volume],
    jobs=[everything],
    schedules=[dg.ScheduleDefinition(job=everything, cron_schedule="30 1 * * *", execution_timezone="Asia/Riyadh")],
    resources={"dbt": DbtCliResource(project_dir=str(U.DBT_PROJECT), profiles_dir=str(U.DBT_PROJECT))},
)


if __name__ == "__main__":
    U.ensure_data()
    result = dg.materialize(
        [*app_assets, *file_assets, qafila_dbt, gold_daily_city_sales, payments_contract, orders_volume],
        resources={"dbt": DbtCliResource(project_dir=str(U.DBT_PROJECT), profiles_dir=str(U.DBT_PROJECT))},
        run_config={"loggers": {"console": {"config": {"log_level": "ERROR"}}}},
    )
    mats = result.get_asset_materialization_events()
    checks = result.get_asset_check_evaluations()
    print(f"\nrun {'succeeded' if result.success else 'FAILED'}: {len(mats)} assets materialized, {len(checks)} checks")
    for c in checks:
        if not c.passed:
            meta = {k: v.value for k, v in c.metadata.items()}
            detail = meta.get("failures") or f"{meta.get('dagster_dbt/failed_row_count', '?')} failing rows"
            print(f"  ⚠️  {c.asset_key.to_user_string()} · {c.check_name}: {detail}")
    print(f"warehouse: {DB}\nlakehouse: {LAKE}")
