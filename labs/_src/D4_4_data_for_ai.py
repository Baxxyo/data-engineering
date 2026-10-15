# %% [markdown]
# # Lab 4.4 · Data for AI: semantic layer, embeddings and safe text-to-SQL
#
# Day 4 · Session 4 (Data for AI) · 25 min. Qafila's managers want to ask questions in Arabic or English and get the
# **same numbers** as the dashboards. Letting a language model write SQL against raw tables gives confident, inconsistent
# answers. The data engineer's job is to give AI good **interfaces** to the data:
# - a **semantic layer**: metrics defined once, in YAML, compiled to SQL;
# - **embeddings** for meaning-based search across Arabic and English;
# - **question → semantic query → SQL**, with a guardrail, so the model never writes raw SQL.
#
# **Tasks**
# - [ ] 1 · Define metrics once; compile them to SQL (7')
# - [ ] 2 · Bilingual product search with embeddings in DuckDB (6')
# - [ ] 3 · Retrieve the right metric for a question (4')
# - [ ] 4 · Question → validated semantic query → answer (6')
# - [ ] ✱ Stretch · Evaluate the question planner
#
# 🔑 An LLM is **optional**: put an OpenAI-compatible endpoint in `labs/.env` (see `.env.example`) to use one. Without it
# the lab uses a simple rule-based planner, and everything still runs.

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
import re
import shutil

import numpy as np
import pandas as pd
import yaml

pd.set_option("display.max_colwidth", 90)
LAB = U.lab_dir("D4_4", reset=True)
DB = LAB / "qafila.duckdb"
shutil.copy(U.build_warehouse(), DB)
U.run_dbt(["build"], db_path=DB, quiet=True)     # the Day 2 star schema, freshly built (~30 s)
con = U.connect(DB)
print(con.sql("SELECT table_schema, count(*) AS tables FROM information_schema.tables GROUP BY 1 ORDER BY 1").df())

# %% [markdown]
# ## 1 · Define metrics once; compile them to SQL (7')
#
# A **semantic layer** (dbt Semantic Layer / MetricFlow, Cube, LookML, Snowflake semantic views) stores the business
# definitions: which table, which joins, what "revenue" means. Every consumer (BI, notebooks, AI) asks for
# **metrics by dimensions**, and the layer writes the SQL. Ours is a small YAML file.

# %%
SEMANTIC = yaml.safe_load("""
model: orders
from: marts.fct_orders AS o
joins:
  - marts.dim_stores AS s USING (store_id)
dimensions:
  order_date:     {expr: o.order_date, description: Order date in Riyadh time (يوم الطلب)}
  month:          {expr: "date_trunc('month', o.order_date)::DATE", description: Calendar month (الشهر)}
  city:           {expr: s.city, description: "Store city: Riyadh, Jeddah, Dammam (المدينة)"}
  store_id:       {expr: o.store_id, description: Dark store (المتجر)}
  payment_method: {expr: o.payment_method, description: "card, apple_pay or wallet (طريقة الدفع)"}
  promo_code:     {expr: "coalesce(o.promo_code, 'none')", description: Promo code used (كود الخصم)}
metrics:
  orders:
    expr: count(*)
    description: Number of orders placed, any status. عدد الطلبات
  delivered_orders:
    expr: "count(*) FILTER (WHERE o.status = 'delivered')"
    description: Number of orders delivered to the customer. الطلبات المسلمة
  revenue_sar:
    expr: "round(sum(o.items_amount) FILTER (WHERE o.status = 'delivered'), 2)"
    description: Revenue, sales, money made from delivered orders in SAR, excluding delivery fees. المبيعات والإيرادات بالريال
  avg_basket_sar:
    expr: "round(avg(o.items_amount) FILTER (WHERE o.status = 'delivered'), 2)"
    description: Average basket size, average order value in SAR for delivered orders. متوسط قيمة السلة
  sla_rate:
    expr: round(avg(o.within_sla::INT), 3)
    description: Share of orders delivered within the 30-minute promise, on-time rate. نسبة التوصيل في الوقت
  refund_rate:
    expr: "round(avg((o.status = 'refunded')::INT), 3)"
    description: Share of orders refunded to the customer. نسبة الاسترجاع
""")
print("metrics:", list(SEMANTIC["metrics"]), "| dimensions:", list(SEMANTIC["dimensions"]))

# %% [markdown]
# ✏️ **Exercise** · Write `compile_query(q)` for a **semantic query** like
# `{"metrics": ["revenue_sar"], "dimensions": ["city"], "filters": [["month", "=", "2026-09-01"]], "limit": 10}`.
# Return `(sql, params)`:
# - `SELECT <dimension exprs AS names>, <metric exprs AS names> FROM <from> JOIN <joins>`;
# - each filter becomes `<dimension expr> <op> ?` with its value appended to `params` (**never** paste values into SQL);
#   op `in` takes a list: `<expr> IN (?, ?, ...)`;
# - `GROUP BY ALL` if there are dimensions, `ORDER BY` the dimensions, then `LIMIT` if given.

# %%
# BEGIN SOLUTION
def compile_query(q: dict) -> tuple[str, list]:
    dims, mets = SEMANTIC["dimensions"], SEMANTIC["metrics"]
    select = [f"{dims[d]['expr']} AS {d}" for d in q.get("dimensions", [])]
    select += [f"{mets[m]['expr']} AS {m}" for m in q["metrics"]]
    sql = f"SELECT {', '.join(select)}\nFROM {SEMANTIC['from']}"
    for j in SEMANTIC.get("joins", []):
        sql += f"\nJOIN {j}"
    where, params = [], []
    for dim, op, value in q.get("filters", []):
        if op == "in":
            where.append(f"{dims[dim]['expr']} IN ({', '.join('?' for _ in value)})")
            params += list(value)
        else:
            where.append(f"{dims[dim]['expr']} {op} ?")
            params.append(value)
    if where:
        sql += "\nWHERE " + " AND ".join(where)
    if q.get("dimensions"):
        sql += "\nGROUP BY ALL\nORDER BY " + ", ".join(q["dimensions"])
    if q.get("limit"):
        sql += f"\nLIMIT {int(q['limit'])}"
    return sql, params
# END SOLUTION

def run(q: dict) -> pd.DataFrame:
    sql, params = compile_query(q)
    return con.execute(sql, params).df()

example = {"metrics": ["revenue_sar", "avg_basket_sar"], "dimensions": ["city"], "filters": [["month", "=", "2026-09-01"]]}
print(compile_query(example)[0])
run(example)

# %%
ours = run({"metrics": ["revenue_sar", "sla_rate"], "dimensions": ["order_date", "city"]})
mart = con.sql("SELECT order_date, city, revenue_sar, sla_rate FROM marts.mart_daily_city_sales ORDER BY 1, 2").df()
U.check(len(ours) == len(mart) and np.allclose(ours.revenue_sar.fillna(0), mart.revenue_sar.fillna(0)), "same revenue as the dashboard mart")
sql, params = compile_query({"metrics": ["orders"], "filters": [["city", "=", "Riyadh'; DROP TABLE x; --"]]})
U.check("DROP" not in sql and params == ["Riyadh'; DROP TABLE x; --"], "values travel as parameters, never as SQL")

# %% [markdown]
# 🔍 One definition of `revenue_sar` now feeds the mart, this notebook, and (in Task 4) the AI assistant. When finance
# changes the definition (say, to include delivery fees), it changes in one place.
#
# ## 2 · Bilingual product search with embeddings in DuckDB (6')
#
# An **embedding** maps text to a vector so that texts with similar meaning are close, even across languages. We use a
# small, fast **static** multilingual model (`model2vec`, CPU-only) and store vectors in DuckDB as `FLOAT[n]` arrays.

# %%
products = con.sql("""
    SELECT product_id, name_en, name_ar, category, description_en, description_ar FROM raw.products WHERE is_active = 1
""").df()
texts = (products.name_en + " | " + products.name_ar + " | " + products.category + " | " + products.description_en).tolist()
vectors = U.embed(texts)
DIM = vectors.shape[1]
con.sql(f"CREATE OR REPLACE TABLE product_vectors (product_id BIGINT, vec FLOAT[{DIM}])")
con.executemany("INSERT INTO product_vectors VALUES (?, ?)", [[int(i), v.tolist()] for i, v in zip(products.product_id, vectors)])
print(f"{len(products)} products embedded, {DIM} dimensions")

# %% [markdown]
# ✏️ **Exercise** · Write `search(query, k=5)`: embed the query, then let DuckDB rank products by
# `array_cosine_similarity(vec, ?::FLOAT[DIM])`; return the top `k` with `name_en`, `name_ar`, `category`, `score`.

# %%
# BEGIN SOLUTION
def search(query: str, k: int = 5) -> pd.DataFrame:
    qv = U.embed([query])[0].tolist()
    return con.execute(f"""
        SELECT p.name_en, p.name_ar, p.category, round(array_cosine_similarity(v.vec, ?::FLOAT[{DIM}]), 3) AS score
        FROM product_vectors v JOIN raw.products p USING (product_id)
        ORDER BY score DESC LIMIT ?
    """, [qv, k]).df()
# END SOLUTION
search("حليب للفطور")

# %%
for q in ["حليب للفطور", "something to clean the dishes", "قهوة عربية", "frozen food for the weekend"]:
    top = search(q, 3)
    print(f"{q!r:34} -> {top.category.tolist()}")
U.check(search("حليب للفطور", 3).category.iloc[0] == "Dairy", "Arabic query finds dairy")
U.check(search("something to clean the dishes", 3).category.iloc[0] == "Household", "English query finds dish soap")

# %% [markdown]
# 🔍 At catalog scale, add an index (DuckDB's `vss` extension builds HNSW; Postgres has `pgvector`; LanceDB, Qdrant and
# OpenSearch are vector-native). The data engineering part is the same: keep vectors **in sync** with the source rows,
# re-embed on change, and version the model name next to the vectors.
#
# ## 3 · Retrieve the right metric for a question (4')
#
# The same trick finds **metrics**: embed each metric's description once, then match questions to them. This is the
# retrieval half of "RAG over the semantic layer": the model only sees the few definitions that matter.

# %%
metric_names = list(SEMANTIC["metrics"])
metric_vecs = U.embed([f"{m}: {SEMANTIC['metrics'][m]['description']}" for m in metric_names])

def top_metrics(question: str, k: int = 2) -> list[tuple[str, float]]:
    scores = metric_vecs @ U.embed([question])[0]
    return [(metric_names[i], round(float(scores[i]), 3)) for i in np.argsort(-scores)[:k]]

for q in ["How much did we sell in Jeddah?", "كم متوسط قيمة السلة في الرياض؟", "Are we delivering on time?"]:
    print(f"{q!r:42} -> {top_metrics(q)}")

# %% [markdown]
# ✏️ **Exercise** · Retrieval isn't perfect. Find one question (English or Arabic) where `top_metrics(q, 1)` picks the
# **wrong** metric, store it in `hard_question` with the metric you expected in `expected_metric`, then improve that
# metric's `description` in `SEMANTIC` (add words people actually use) and re-embed so it's retrieved correctly.

# %%
# BEGIN SOLUTION
hard_question = "How punctual are the couriers?"
expected_metric = "sla_rate"
print("before:", top_metrics(hard_question, 2))
SEMANTIC["metrics"]["sla_rate"]["description"] += " Punctual, on time, late deliveries, courier punctuality, delivery speed. دقة مواعيد التوصيل والتأخير"
metric_vecs = U.embed([f"{m}: {SEMANTIC['metrics'][m]['description']}" for m in metric_names])
# END SOLUTION
print("after: ", top_metrics(hard_question, 2))
U.check(top_metrics(hard_question, 1)[0][0] == expected_metric, "better descriptions, better retrieval")

# %% [markdown]
# 🔍 Descriptions are now **product surface**: they're what AI assistants read. Writing them well (synonyms, both
# languages, what's excluded) is a data engineering task, and it belongs in code review like any other definition.
#
# ## 4 · Question → validated semantic query → answer (6')
#
# The planner turns a question into a **semantic query** (JSON), never into SQL. With an LLM, the prompt contains only the
# retrieved metrics and the dimension list. Without one, a rule-based planner does the same job for simple questions.

# %%
llm = U.get_llm()
CITY_WORDS = {"riyadh": "Riyadh", "الرياض": "Riyadh", "jeddah": "Jeddah", "جدة": "Jeddah", "dammam": "Dammam", "الدمام": "Dammam"}

def plan_rules(question: str) -> dict:
    """Deterministic fallback: top retrieved metric + keyword dimensions and filters."""
    ql = question.lower()
    q = {"metrics": [top_metrics(question, 1)[0][0]], "dimensions": [], "filters": []}
    if re.search(r"by city|per city|each city|لكل مدينة|حسب المدينة", ql):
        q["dimensions"].append("city")
    if re.search(r"daily|per day|by day|يومي|لكل يوم", ql):
        q["dimensions"].append("order_date")
    if re.search(r"monthly|by month|per month|شهري", ql):
        q["dimensions"].append("month")
    cities = [c for w, c in CITY_WORDS.items() if w in ql]
    if cities and "city" not in q["dimensions"]:
        q["filters"].append(["city", "in", sorted(set(cities))])
    if re.search(r"national day|اليوم الوطني", ql):
        q["filters"].append(["order_date", "=", "2026-09-23"])
    if re.search(r"september|سبتمبر", ql):
        q["filters"].append(["month", "=", "2026-09-01"])
    return q

def plan_llm(question: str) -> dict:
    catalog = {"metrics": {m: SEMANTIC["metrics"][m]["description"] for m, _ in top_metrics(question, 3)},
               "dimensions": {d: v["description"] for d, v in SEMANTIC["dimensions"].items()}}
    prompt = (f"Turn the question into a JSON semantic query with keys metrics (list), dimensions (list), filters "
              f"(list of [dimension, op, value], op one of = != > >= < <= in). Dates are ISO strings; month values are the "
              f"first day of the month. Data covers Aug-Sep 2026. Use only these names:\n{json.dumps(catalog, ensure_ascii=False)}\n"
              f"Question: {question}\nAnswer with JSON only.")
    text = llm([{"role": "user", "content": prompt}])
    return json.loads(re.search(r"\{.*\}", text, re.S).group(0))

plan = plan_llm if llm else plan_rules
print("planner:", "LLM" if llm else "rule-based fallback")

# %% [markdown]
# ✏️ **Exercise** · Whatever writes the plan (a model or rules), **validate it before running it**. Write
# `validate(q)` that raises `ValueError` if: a metric or dimension isn't in `SEMANTIC`; a filter's dimension isn't
# known; an op isn't one of `= != > >= < <= in`; `in` doesn't get a list; there are no metrics or more than 3; or a
# `limit` above 1000 is asked for. Return `q` unchanged if it's fine.

# %%
OPS = {"=", "!=", ">", ">=", "<", "<=", "in"}

# BEGIN SOLUTION
def validate(q: dict) -> dict:
    mets, dims = q.get("metrics", []), q.get("dimensions", [])
    if not 1 <= len(mets) <= 3:
        raise ValueError("ask for 1 to 3 metrics")
    for m in mets:
        if m not in SEMANTIC["metrics"]:
            raise ValueError(f"unknown metric {m!r}")
    for d in dims:
        if d not in SEMANTIC["dimensions"]:
            raise ValueError(f"unknown dimension {d!r}")
    for f in q.get("filters", []):
        if len(f) != 3:
            raise ValueError(f"bad filter {f!r}")
        d, op, v = f
        if d not in SEMANTIC["dimensions"]:
            raise ValueError(f"unknown filter dimension {d!r}")
        if op not in OPS:
            raise ValueError(f"operator {op!r} not allowed")
        if op == "in" and not isinstance(v, list):
            raise ValueError("'in' needs a list")
    if int(q.get("limit") or 0) > 1000:
        raise ValueError("limit too large")
    return q
# END SOLUTION

def ask(question: str) -> pd.DataFrame:
    q = validate(plan(question))
    print(f"{question}\n  -> {json.dumps(q, ensure_ascii=False)}")
    return run(q)

ask("What was the revenue by city in September?")

# %%
print(ask("كم متوسط قيمة السلة في جدة يوم اليوم الوطني؟"))
bad = [{"metrics": ["revenue_sar"], "dimensions": ["national_id"]},
       {"metrics": ["revenue_sar"], "filters": [["city", "; DROP", "x"]]},
       {"metrics": ["salary"]}, {"metrics": []}]
blocked = 0
for q in bad:
    try:
        validate(q)
    except ValueError as e:
        blocked += 1
        print("blocked:", e)
U.check(blocked == len(bad), "every malformed or unsafe plan is rejected before it touches the database")

# %% [markdown]
# 🔍 The pattern: **the model chooses, the platform executes.** The LLM can only pick from metrics and dimensions you
# defined, values go in as parameters, and the semantic layer guarantees "revenue" means the same thing as on the
# dashboard. That's how Snowflake Cortex Analyst, Databricks Genie and the dbt Semantic Layer's AI integrations work, and
# why the semantic layer (≈5% adoption in 2026 surveys) is suddenly a priority.
#
# ## ✱ Stretch · Evaluate the question planner
#
# Prompts and rules change; measure them like any other code. Build a small **eval set** of questions with the semantic
# query you expect, run the planner on each, and report accuracy (exact match on metrics, dimensions and filters).

# %%
# BEGIN SOLUTION
EVALS = [
    ("What was the revenue by city in September?", {"metrics": ["revenue_sar"], "dimensions": ["city"], "filters": [["month", "=", "2026-09-01"]]}),
    ("Daily orders in Riyadh", {"metrics": ["orders"], "dimensions": ["order_date"], "filters": [["city", "in", ["Riyadh"]]]}),
    ("كم متوسط قيمة السلة في جدة يوم اليوم الوطني؟", {"metrics": ["avg_basket_sar"], "dimensions": [],
                                                      "filters": [["city", "in", ["Jeddah"]], ["order_date", "=", "2026-09-23"]]}),
    ("Are we delivering on time, by city?", {"metrics": ["sla_rate"], "dimensions": ["city"], "filters": []}),
    ("How many orders were refunded in Dammam?", {"metrics": ["refund_rate"], "dimensions": [], "filters": [["city", "in", ["Dammam"]]]}),
]

def normalize(q):
    return (sorted(q.get("metrics", [])), sorted(q.get("dimensions", [])),
            sorted(json.dumps(f, ensure_ascii=False) for f in q.get("filters", [])))

results = []
for question, expected in EVALS:
    try:
        got = validate(plan(question))
        ok = normalize(got) == normalize(expected)
    except Exception as e:
        got, ok = {"error": str(e)}, False
    results.append({"question": question, "ok": ok, "got": json.dumps(got, ensure_ascii=False)})
evals = pd.DataFrame(results)
print(f"planner accuracy: {evals.ok.mean():.0%}")
evals
# END SOLUTION

# %% [markdown]
# 🔍 Read the failures, not just the score. "Daily orders in Riyadh" retrieved `revenue_sar`: short questions give
# retrieval little to work with. And "how many orders were refunded" is really a **count**, while our closest metric is a
# **rate**: the eval passes, but the user may be surprised. Fix such gaps in the semantic layer (add a `refunded_orders`
# metric, sharpen descriptions) rather than piling rules into the prompt, then re-run the evals.
#
# ## Checkpoint
#
# 1. Why route AI questions through a semantic layer instead of letting the model write SQL on raw tables?
# 2. What has to happen in your pipeline when a product's description changes? (Think: embeddings.)
# 3. Name two checks a guardrail must do before running a model-generated query.
# 4. Your planner scores 80% on the eval set. What would you do before giving it to 500 store managers?
#
# ## 🏁 Capstone kickoff
#
# Tomorrow is capstone day. Open `labs/capstone/README.md`: pick your option, copy `TEMPLATE_README.md` into your project,
# and get `capstone/pipeline.py` running end to end tonight. Slides: the last section of today's deck.
