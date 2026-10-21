"""Capstone: the Qafila Lakehouse as one Dagster code location (solo submission).

Sources -> raw (DuckDB; incremental where it matters) -> dbt (staging, marts) -> gold on Iceberg,
with contract checks (blocking + warn + quarantine), a volume monitor, PII protection,
a daily-partitioned ops asset with backfills, and nightly schedules.

    uv run python capstone/pipeline.py              # materialize everything once, in-process
    uv run python capstone/pipeline.py backfill     # backfill the daily ops partitions
    uv run dagster dev -f capstone/pipeline.py      # the UI: lineage, runs, checks, schedules

Run from labs/. Re-running is safe: every asset is idempotent.
"""

import hashlib
import hmac
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

LOOKBACK_DAYS = 3                                    # catches late changes (refunds, status updates)
PII_KEY = os.environ.get("QAFILA_PII_KEY", "dev-only-change-me").encode()


def sql(query: str, params: list | None = None):
    con = U.connect(DB)
    con.execute("SET python_enable_replacements = false")
    try:
        return con.execute(query, params or []).fetchall()
    finally:
        con.close()


def _ts(col: str) -> str:
    """Parse a timestamp that may be naive or carry an offset/Z, to UTC."""
    return (f"coalesce(try_cast({col} AS TIMESTAMP), "
            f"timezone('UTC', try_cast({col} AS TIMESTAMPTZ)))")


# ---------------------------------------------------------------------------
# 1. Ingestion (keys match dbt sources: raw.<table>)
# ---------------------------------------------------------------------------
RAW = U.RAW.as_posix()
APP_FULL_TABLES = ["customers", "stores", "products"]      # small: full replace is fine


def _attach(con):
    con.sql("INSTALL sqlite; LOAD sqlite;")
    con.sql(f"ATTACH IF NOT EXISTS '{RAW}/qafila_oltp.sqlite' AS app (TYPE sqlite, READ_ONLY)")


def _incremental_load(con, target, source, key, cursor, after_sql=()):
    """Delete+insert on `key` for rows whose `cursor` is at/after the watermark minus a look-back.
    Data, child reloads (after_sql) and the new watermark commit in ONE transaction."""
    con.sql("SET TimeZone = 'UTC'")
    con.sql("CREATE SCHEMA IF NOT EXISTS raw")
    con.sql("CREATE SCHEMA IF NOT EXISTS meta")
    con.sql("CREATE TABLE IF NOT EXISTS meta.watermarks "
            "(source VARCHAR PRIMARY KEY, last_value TIMESTAMP, loaded_at TIMESTAMP)")
    con.sql(f"CREATE TABLE IF NOT EXISTS raw.{target} AS "
            f"SELECT *, now()::TIMESTAMP AS _loaded_at FROM {source} WHERE false")
    con.begin()
    try:
        row = con.execute("SELECT last_value FROM meta.watermarks WHERE source = ?", [target]).fetchone()
        since = row[0] if row else None
        con.execute(f"""
            CREATE OR REPLACE TEMP TABLE _batch AS
            SELECT *, now()::TIMESTAMP AS _loaded_at FROM {source}
            WHERE ?::TIMESTAMP IS NULL
               OR {_ts(cursor)} >= ?::TIMESTAMP - to_days(?::INTEGER)""",
            [since, since, LOOKBACK_DAYS])
        con.sql(f"DELETE FROM raw.{target} WHERE {key} IN (SELECT {key} FROM _batch)")
        con.sql(f"INSERT INTO raw.{target} SELECT * FROM _batch")
        for stmt in after_sql:
            con.sql(stmt)
        new_wm = con.sql(f"SELECT max({_ts(cursor)}) FROM raw.{target}").fetchone()[0]
        con.execute("INSERT OR REPLACE INTO meta.watermarks VALUES (?, ?, now())", [target, new_wm])
        changed = con.sql("SELECT count(*) FROM _batch").fetchone()[0]
        con.commit()
        return changed, since
    except Exception:
        con.rollback()
        raise


def _load_app_table(table: str):
    con = U.connect(DB)
    try:
        _attach(con)
        con.sql("CREATE SCHEMA IF NOT EXISTS raw")
        con.sql(f"CREATE OR REPLACE TABLE raw.{table} AS SELECT *, now()::TIMESTAMP AS _loaded_at FROM app.{table}")
        return con.sql(f"SELECT count(*) FROM raw.{table}").fetchone()[0]
    finally:
        con.close()


def _make_app_asset(table: str):
    @dg.asset(key=["raw", table], group_name="ingest", kinds={"sqlite", "duckdb"},
              description=f"App database: {table} (full replace)")
    def _asset():
        return dg.MaterializeResult(metadata={"rows": _load_app_table(table)})
    return _asset


app_assets = [_make_app_asset(t) for t in APP_FULL_TABLES]


@dg.multi_asset(
    specs=[
        dg.AssetSpec(["raw", "orders"], group_name="ingest", kinds={"sqlite", "duckdb"},
                     description="App orders: incremental on updated_at, 3-day look-back, delete+insert"),
        dg.AssetSpec(["raw", "order_items"], group_name="ingest", kinds={"sqlite", "duckdb"},
                     description="Order lines: reloaded for every changed order, same transaction"),
    ]
)
def raw_orders_and_items():
    con = U.connect(DB)
    try:
        _attach(con)
        con.sql("CREATE SCHEMA IF NOT EXISTS raw")
        con.sql("CREATE TABLE IF NOT EXISTS raw.order_items AS "
                "SELECT *, now()::TIMESTAMP AS _loaded_at FROM app.order_items WHERE false")
        changed, since = _incremental_load(
            con, "orders", "app.orders", "order_id", "updated_at",
            after_sql=[
                "DELETE FROM raw.order_items WHERE order_id IN (SELECT order_id FROM _batch)",
                "INSERT INTO raw.order_items SELECT *, now()::TIMESTAMP FROM app.order_items "
                "WHERE order_id IN (SELECT order_id FROM _batch)",
            ])
        n_orders = con.sql("SELECT count(*) FROM raw.orders").fetchone()[0]
        n_items = con.sql("SELECT count(*) FROM raw.order_items").fetchone()[0]
    finally:
        con.close()
    yield dg.MaterializeResult(asset_key=["raw", "orders"], metadata={
        "rows_total": n_orders, "rows_changed_this_run": changed, "watermark_before": str(since)})
    yield dg.MaterializeResult(asset_key=["raw", "order_items"], metadata={"rows_total": n_items})


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
INCREMENTAL_FILES = {"payments": ("payment_id", "updated_at")}   # table: (key, cursor)


def _make_file_asset(table: str, reader: str):
    if table in INCREMENTAL_FILES:
        key, cursor = INCREMENTAL_FILES[table]

        @dg.asset(key=["raw", table], group_name="ingest", kinds={"duckdb"},
                  description=f"API source: {table} (incremental on {cursor}, merge on {key})")
        def _inc_asset():
            con = U.connect(DB)
            try:
                changed, since = _incremental_load(con, table, reader, key, cursor)
                total = con.sql(f"SELECT count(*) FROM raw.{table}").fetchone()[0]
            finally:
                con.close()
            return dg.MaterializeResult(metadata={"rows_total": total, "rows_changed_this_run": changed,
                                                  "watermark_before": str(since)})
        return _inc_asset

    @dg.asset(key=["raw", table], group_name="ingest", kinds={"duckdb"},
              description=f"File source: {table} (full replace)")
    def _asset():
        sql("CREATE SCHEMA IF NOT EXISTS raw")
        sql(f"CREATE OR REPLACE TABLE raw.{table} AS SELECT * FROM {reader}")
        return dg.MaterializeResult(metadata={"rows": sql(f"SELECT count(*) FROM raw.{table}")[0][0]})
    return _asset


file_assets = [_make_file_asset(t, r) for t, r in FILE_SOURCES.items()]


# ---------------------------------------------------------------------------
# 2. Contract on the payments feed: structure rules BLOCK, value rules WARN + quarantine
# ---------------------------------------------------------------------------
def _contract_rules():
    contract = yaml.safe_load(CONTRACT.read_text(encoding="utf-8"))
    obj = contract["schema"][0]
    return [(None, r) for r in obj.get("quality", [])] + \
           [(p["name"], r) for p in obj["properties"] for r in p.get("quality", [])]


def _contract_results():
    results = []
    for col, rule in _contract_rules():
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
        op, target = next((k, v) for k, v in rule.items() if k.startswith("must"))
        ok = {"mustBe": value == target, "mustBeGreaterThan": value > target, "mustBeLessThan": value < target}[op]
        kind = "values" if rule.get("type") == "sql" else "structure"
        results.append((kind, rule.get("id", col), value, op, target, ok))
    return results


def _contract_check(kind: str, severity):
    res = [r for r in _contract_results() if r[0] == kind]
    failures = [f"{rid}: {v} ({op} {t})" for _, rid, v, op, t, ok in res if not ok]
    return dg.AssetCheckResult(passed=not failures, severity=severity,
                               metadata={"rules_checked": len(res), "failures": "; ".join(failures) or "none"})


@dg.asset_check(asset=dg.AssetKey(["raw", "payments"]), blocking=True,
                description="ODCS structure rules (keys, row count, allowed values): failure BLOCKS dbt")
def payments_contract_structure():
    return _contract_check("structure", dg.AssetCheckSeverity.ERROR)


@dg.asset_check(asset=dg.AssetKey(["raw", "payments"]),
                description="ODCS value rules (custom SQL): bad rows are quarantined; warn the owner")
def payments_contract_values():
    return _contract_check("values", dg.AssetCheckSeverity.WARN)


@dg.asset(key=["quarantine", "payments"], group_name="quality", kinds={"duckdb"},
          deps=[dg.AssetKey(["raw", "payments"])],
          description="Payments that break a contract value rule, with the reason")
def quarantine_payments():
    selects = []
    for _, rule in _contract_rules():
        q = rule.get("query", "")
        if rule.get("type") == "sql" and q.lower().startswith("select count(*)"):
            reason = str(rule.get("id", "contract_sql_rule")).replace("'", "")
            selects.append(f"SELECT *, '{reason}' AS reject_reason "
                           + q[len("select count(*)"):].replace("{object}", "raw.payments"))
    sql("CREATE SCHEMA IF NOT EXISTS quarantine")
    body = " UNION ALL BY NAME ".join(selects) if selects else \
        "SELECT *, '' AS reject_reason FROM raw.payments WHERE false"
    sql(f"CREATE OR REPLACE TABLE quarantine.payments AS {body}")
    return dg.MaterializeResult(metadata={"rows_quarantined": sql("SELECT count(*) FROM quarantine.payments")[0][0]})


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
    """The dashboard mart as an Iceberg table (overwrite = idempotent)."""
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
# 5. Governance: pseudonymize, mask, classify; prove no national IDs leak into marts
# ---------------------------------------------------------------------------
def _pseudo(value):
    """Keyed hash (HMAC-SHA256). A pseudonym: still personal data, but not re-identifiable without the key."""
    return hmac.new(PII_KEY, str(value).encode(), hashlib.sha256).hexdigest()[:16]


def _luhn_ok(s: str) -> bool:
    total = 0
    for i, ch in enumerate(reversed(s)):
        d = int(ch) * (2 if i % 2 else 1)
        total += d - 9 if d > 9 else d
    return total % 10 == 0


@dg.asset(key=["governed", "customers"], group_name="governance", kinds={"duckdb"},
          deps=[dg.AssetKey(["raw", "customers"])],
          description="Analyst-safe customers: HMAC token, masked mobile, email domain only")
def governed_customers():
    con = U.connect(DB)
    try:
        con.create_function("pseudo", _pseudo, ["VARCHAR"], "VARCHAR")
        con.sql("CREATE SCHEMA IF NOT EXISTS governed")
        con.sql("""
            CREATE OR REPLACE TABLE governed.customers AS
            WITH d AS (SELECT *, right(regexp_replace(CAST(mobile AS VARCHAR), '[^0-9]', '', 'g'), 9) AS m9
                       FROM raw.customers)
            SELECT customer_id,
                   pseudo(CAST(national_id AS VARCHAR)) AS person_token,
                   CASE WHEN regexp_full_match(m9, '5[0-9]{8}')
                        THEN '+966' || left(m9, 1) || '******' || right(m9, 2) END AS mobile_masked,
                   lower(split_part(CAST(email AS VARCHAR), '@', 2)) AS email_domain,
                   city, district, created_at
            FROM d""")
        con.sql("""
            CREATE OR REPLACE TABLE governed.pii_classification AS
            SELECT * FROM (VALUES
              ('raw.customers',      'iban',          'Secret',     'never copied to governed or marts'),
              ('raw.customers',      'full_name',     'Restricted', 'never copied to governed or marts'),
              ('raw.customers',      'email',         'Restricted', 'only the domain is published'),
              ('raw.customers',      'mobile',        'Restricted', 'masked: +9665******NN'),
              ('raw.payments',       'card_last4',    'Restricted', 'not exposed in marts'),
              ('governed.customers', 'person_token',  'Restricted', 'keyed pseudonym = still personal data'),
              ('marts.*',            'city/district', 'Public',     'aggregate context')
            ) t(table_name, column_name, ndmo_level, protection)""")
        n = con.sql("SELECT count(*) FROM governed.customers").fetchone()[0]
        dupes = con.sql("SELECT count(person_token) - count(DISTINCT person_token) FROM governed.customers").fetchone()[0]
    finally:
        con.close()
    return dg.MaterializeResult(metadata={"rows": n, "people_with_duplicate_accounts": dupes})


@dg.asset_check(asset=dg.AssetKey(["governed", "customers"]),
                description="No Luhn-valid Saudi national IDs in any marts or governed table")
def no_national_ids_in_marts():
    cols = sql("""SELECT table_schema, table_name, column_name FROM information_schema.columns
                  WHERE table_schema IN ('marts', 'governed')""")
    leaks = []
    for schema, table, col in cols:
        vals = sql(f"""SELECT DISTINCT CAST("{col}" AS VARCHAR) FROM {schema}."{table}"
                       WHERE regexp_full_match(CAST("{col}" AS VARCHAR), '[12][0-9]{{9}}') LIMIT 500""")
        hits = sum(1 for (v,) in vals if _luhn_ok(v))
        if hits:
            leaks.append(f"{schema}.{table}.{col}: {hits}")
    return dg.AssetCheckResult(passed=not leaks, severity=dg.AssetCheckSeverity.ERROR,
                               metadata={"columns_scanned": len(cols), "leaks": "; ".join(leaks) or "none"})


# ---------------------------------------------------------------------------
# 6. Partitions + backfill: daily delivery KPIs, delete-then-insert per day
# ---------------------------------------------------------------------------
DAYS = dg.DailyPartitionsDefinition(start_date="2026-09-24", end_date="2026-10-01")


@dg.asset(key=["ops", "delivery_kpis_daily"], partitions_def=DAYS, group_name="ops", kinds={"duckdb"},
          deps=[dg.AssetKey(["raw", "delivery_events"])],
          description="Per store per Riyadh day: delivered orders and events, deduplicated by event_id")
def delivery_kpis_daily(context: dg.AssetExecutionContext):
    day = context.partition_key
    con = U.connect(DB)
    try:
        con.sql("CREATE SCHEMA IF NOT EXISTS ops")
        con.sql("""CREATE TABLE IF NOT EXISTS ops.delivery_kpis_daily
                   (day DATE, store_id VARCHAR, city VARCHAR, delivered_orders BIGINT, events BIGINT)""")
        con.begin()
        con.execute("DELETE FROM ops.delivery_kpis_daily WHERE day = ?::DATE", [day])
        con.execute(r"""
            INSERT INTO ops.delivery_kpis_daily
            SELECT ?::DATE, store_id, city,
                   count(DISTINCT order_id) FILTER (WHERE event_type = 'delivered'),
                   count(*)
            FROM (
                SELECT *,
                       CASE WHEN regexp_full_match(event_time, '\d{13}')
                            THEN make_timestamp(try_cast(event_time AS BIGINT) * 1000)
                            ELSE timezone('UTC', try_cast(event_time AS TIMESTAMPTZ)) END AS event_at_utc
                FROM raw.delivery_events
                QUALIFY row_number() OVER (PARTITION BY event_id ORDER BY ingest_time) = 1
            )
            WHERE CAST(event_at_utc + INTERVAL 3 HOUR AS DATE) = ?::DATE
            GROUP BY ALL""", [day, day])
        con.commit()
        n = con.execute("SELECT count(*) FROM ops.delivery_kpis_daily WHERE day = ?::DATE", [day]).fetchone()[0]
    finally:
        con.close()
    return dg.MaterializeResult(metadata={"day": day, "rows": n})


# ---------------------------------------------------------------------------
# 7. The code location
# ---------------------------------------------------------------------------
UNPARTITIONED = [*app_assets, raw_orders_and_items, *file_assets, quarantine_payments,
                 qafila_dbt, gold_daily_city_sales, governed_customers]
CHECKS = [payments_contract_structure, payments_contract_values, orders_volume, no_national_ids_in_marts]
DBT_RESOURCE = {"dbt": DbtCliResource(project_dir=str(U.DBT_PROJECT), profiles_dir=str(U.DBT_PROJECT))}
QUIET = {"loggers": {"console": {"config": {"log_level": "ERROR"}}}}

refresh_all = dg.define_asset_job(
    "refresh_all", selection=dg.AssetSelection.all() - dg.AssetSelection.keys(["ops", "delivery_kpis_daily"]))
ops_daily = dg.define_asset_job(
    "ops_daily", selection=dg.AssetSelection.keys(["ops", "delivery_kpis_daily"]), partitions_def=DAYS)

defs = dg.Definitions(
    assets=[*UNPARTITIONED, delivery_kpis_daily],
    asset_checks=CHECKS,
    jobs=[refresh_all, ops_daily],
    schedules=[
        dg.ScheduleDefinition(job=refresh_all, cron_schedule="30 1 * * *", execution_timezone="Asia/Riyadh"),
        dg.build_schedule_from_partitioned_job(ops_daily, hour_of_day=1, minute_of_hour=45),
    ],
    resources=DBT_RESOURCE,
)


if __name__ == "__main__":
    U.ensure_data()

    if len(sys.argv) > 1 and sys.argv[1] == "backfill":
        for day in DAYS.get_partition_keys():
            r = dg.materialize([delivery_kpis_daily], partition_key=day, run_config=QUIET)
            print(f"{day}: {'ok' if r.success else 'FAILED'}")
        print(f"ops.delivery_kpis_daily: {sql('SELECT count(*) FROM ops.delivery_kpis_daily')[0][0]} rows")
        sys.exit(0)

    result = dg.materialize([*UNPARTITIONED, *CHECKS], resources=DBT_RESOURCE, run_config=QUIET)
    mats = result.get_asset_materialization_events()
    checks = result.get_asset_check_evaluations()
    print(f"\nrun {'succeeded' if result.success else 'FAILED'}: {len(mats)} assets materialized, {len(checks)} checks")
    for c in checks:
        if not c.passed:
            meta = {k: v.value for k, v in c.metadata.items()}
            detail = meta.get("failures") or meta.get("leaks") or meta.get("low_days") \
                or f"{meta.get('dagster_dbt/failed_row_count', '?')} failing rows"
            print(f"  ⚠️  {c.asset_key.to_user_string()} · {c.check_name}: {detail}")
    for e in mats:
        md = e.materialization.metadata
        if "rows_changed_this_run" in md:
            print(f"  ↻ {e.asset_key.to_user_string()}: {md['rows_changed_this_run'].value} rows changed this run")
    print(f"warehouse: {DB}\nlakehouse: {LAKE}")
