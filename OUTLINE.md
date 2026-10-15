# Modern Data Engineering: course outline

**SDAIA Academy · Data Engineering Track**: 4 teaching days × 4 sessions + 1 capstone day.
Each day runs Session 1 → 10' break → Session 2 → 20' break → Session 3 → 10' break → Session 4 (4 × 50 min).
Every session is roughly 20–25 minutes of concepts and demos followed by a 25-minute lab; the lab builds one piece of the same platform.

> **From raw sources to a governed, AI-ready lakehouse.** Ingest, model, test, scale, stream, orchestrate and govern data
> with the open tools data teams use today: DuckDB, Polars, Parquet, dlt, dbt, Apache Iceberg, Spark, Dagster.

## Who it is for

Python- and SQL-proficient developers, analysts and data scientists moving into data engineering.
No prior experience with Spark, dbt, orchestration or streaming is needed.

## Why this outline (research summary, October 2026)

| Signal | What it means for the course |
|---|---|
| Warehouses still lead (≈44%) but lakehouses (≈27%) and hybrids (≈12%) are growing and engines are converging on **open table formats**; Iceberg v3 (deletion vectors, VARIANT, row lineage) is GA on the major platforms, DuckLake reached 1.0 (April 2026). | A full session on table formats and catalogs; the platform we build lands in **Iceberg**, queried by several engines. |
| **Single-node engines** (DuckDB, Polars, DataFusion) handle most workloads that used to need a cluster; Spark remains the standard at scale. | Teach DuckDB/Polars first (Day 1–2) and Spark as the scale-out tool (Day 3), with a "when do you need a cluster?" decision rule. |
| **dbt** is the default for SQL transformation; dbt v2 (the Rust "Fusion" engine) went GA in September 2026 after the Fivetran + dbt Labs merger. | dbt project as the transformation layer; slides explain v1 (Python) vs. v2 (Rust) and the licensing split. Labs run dbt v1 + `dbt-duckdb`, which works offline. |
| **Data modeling is the top pain point** (59% cite pressure to move fast, only 11% say modeling goes well, ≈5% use semantic models). | A dedicated dimensional-modeling session and a semantic/metrics layer in the AI session. |
| **Orchestration**: Airflow (3.x, now with asset-aware scheduling) dominates; Dagster is the asset-centric alternative; 20% of teams have no orchestrator at all. | Orchestration session framed around assets, partitions, backfills and idempotency; slides show Airflow 3, lab runs Dagster in-process (no server). |
| **Streaming**: Kafka 4 runs on KRaft only (no ZooKeeper); topic-to-table products (e.g. Tableflow) land streams directly in Iceberg/Delta. | Two streaming sessions: semantics (offsets, event time, windows, watermarks) and streaming into the lakehouse (small files, compaction, CDC). |
| **Trust and contracts**: trust in data rose from 66% to 83% in importance (dbt 2026 report); the Open Data Contract Standard (ODCS v3.1, LF AI & Data) gives a YAML contract format. | Data quality + testing session (Day 2) and observability + contracts session (Day 4). |
| **AI is table stakes**: 82% of data engineers use AI tools daily; ingestion pipelines are increasingly written by agents. Data teams are asked to make data "agent-ready". | Final session on data engineering *for* AI: semantic layer, embeddings pipelines, vector search, text-to-SQL over governed metrics, and using AI assistants safely. |
| **KSA context**: the Personal Data Protection Law (PDPL) and NDMO / SDAIA Data Management and Personal Data Protection Standards require impact-based data classification, a classified-asset register, minimization, retention and cross-border controls. | A governance and privacy session with PII discovery, classification (Top Secret / Secret / Restricted / Public), pseudonymization, role-based masking and erasure requests on a lakehouse table. |

Sources: Joe Reis, *2026 State of Data Engineering* and *Where data engineering is heading in 2026*; dbt Labs, *2026 State of Analytics Engineering*; Fivetran + dbt Labs announcements (June and September 2026); DuckLake 1.0 announcement (April 2026) and DuckDB-Iceberg v1.5.3 notes; Apache Iceberg spec v3; Confluent, *Introducing Apache Kafka 4.0* and Tableflow GA; Astronomer and Dagster comparisons; Bitol ODCS v3.1.0 release; NDMO / SDAIA standards summaries. Full links are in the [README](README.md#sources).

## One case study, all week: **Qafila**

Qafila is a fictional quick-commerce grocery platform in Riyadh, Jeddah and Dammam. Its data arrives the way real data does:

| Source | Shape | Deliberate problems |
|---|---|---|
| Operational database (SQLite) | customers, stores, products (Arabic + English names), orders, order items | soft deletes, `updated_at` cursors, PII |
| Payments API (local mock REST service) | paginated JSON, cursor by `updated_at` | rate limits, retries, a field added mid-week |
| Customer CDC feed | Debezium-style change events (`c`/`u`/`d`, `before`/`after`) | out-of-order and duplicate events |
| Delivery event stream | JSON events with event time and device time | late, duplicate and out-of-order events; time zones |
| Supplier price files | weekly CSV drops | decimal commas, Arabic digits, renamed column, schema drift |
| Product catalog text and reviews | Arabic + English free text | for embeddings and AI use cases |

All data is synthetic and generated deterministically by `labs/prepare_data.py`, so the labs run offline and nobody handles real personal data.

## Day 1 · Data Platforms, Storage & Ingestion

| # | Session | Concepts | Lab (25') |
|---|---|---|---|
| 1 | **The modern data platform** | Data engineering lifecycle; OLTP vs. OLAP; warehouse, lake, lakehouse; ETL → ELT; batch vs. streaming; medallion layers; the 2026 tool landscape | **1.1 Platform tour with DuckDB**: query CSV, JSON, Parquet and SQLite in place; profile every Qafila source; log the quality issues you find |
| 2 | **Files & columnar storage** | Row vs. column layout; Parquet internals (row groups, pages, statistics, dictionary/RLE encoding, compression); partitioning and the small-files problem; Arrow in memory | **1.2 Parquet in practice**: CSV vs. Parquet size and speed; read file metadata and row-group stats; Hive partitioning; predicate and projection pushdown |
| 3 | **Batch ingestion** | Source types (DB, API, files); full vs. incremental extraction; cursors and high-water marks; pagination, rate limits, retries; idempotency; schema inference and evolution; dlt | **1.3 Ingest with dlt**: paginated REST API + SQL database into DuckDB; incremental cursor; re-run safely; handle a new field (schema evolution) |
| 4 | **Change data capture & load strategies** | Snapshot vs. log-based CDC (Debezium); append, overwrite, merge/upsert; deletes and tombstones; SCD Type 1 and Type 2; late and out-of-order changes | **1.4 CDC to SCD2**: apply a Debezium-style feed with `MERGE`; dedupe and order events; build an SCD Type 2 customer dimension; answer "as of" questions |

## Day 2 · Transformation, Modeling & Quality

| # | Session | Concepts | Lab (25') |
|---|---|---|---|
| 1 | **Processing engines: DuckDB & Polars** | Vectorized, columnar execution; lazy evaluation and query plans; predicate pushdown; joins and window functions; out-of-core processing; when single-node is enough | **2.1 Bronze → silver**: clean orders and deliveries with Polars and DuckDB (types, dedupe, time zones, Arabic text normalization); read the query plans |
| 2 | **Dimensional modeling** | Grain; facts and dimensions; star vs. snowflake vs. one big table; surrogate keys; conformed dimensions; date dimension; Data Vault in one slide | **2.2 Model Qafila**: design the star schema for order lines; build `fact_order_items` and its dimensions; check grain and row counts; answer business questions |
| 3 | **dbt: transformation as software** | Sources, refs and the DAG; staging → intermediate → marts; materializations (view, table, incremental); Jinja and macros; docs and lineage; dbt v1 vs. v2 (Fusion) | **2.3 A dbt project**: run the Qafila project on DuckDB; write a staging model and an incremental mart; generate lineage |
| 4 | **Testing & data quality** | Quality dimensions; generic, singular and unit tests in dbt; model contracts; dataframe validation with Pandera; quarantine vs. fail; testing in CI | **2.4 Make it trustworthy**: add tests and a model contract; write a dbt unit test; validate supplier files with Pandera and quarantine bad rows |

## Day 3 · Lakehouse, Scale & Streaming

| # | Session | Concepts | Lab (25') |
|---|---|---|---|
| 1 | **Open table formats & the lakehouse** | Why files alone are not tables; Iceberg metadata tree (catalog → metadata → manifest lists → manifests → data files); snapshots, time travel, schema and partition evolution; Iceberg v3; Delta Lake, Hudi, DuckLake; catalogs (REST, Polaris, Unity, Glue) | **3.1 Iceberg with PyIceberg**: create tables in a local catalog; append, upsert, delete; time travel; evolve schema and partitioning; query the same table from DuckDB |
| 2 | **Distributed processing with Spark** | Driver/executors; partitions, stages, shuffles; narrow vs. wide transformations; broadcast vs. sort-merge joins; skew and salting; AQE; Spark 4 and Spark Connect; when you need a cluster | **3.2 Spark** (Colab or Java 17+): DataFrame API on Qafila orders; read physical plans; broadcast join; detect and fix a skewed key; write partitioned output |
| 3 | **Streaming fundamentals** | Events and logs; Kafka model (topics, partitions, offsets, consumer groups; KRaft in Kafka 4); delivery guarantees; event time vs. processing time; tumbling/sliding/session windows; watermarks; state | **3.3 Stream processing from first principles**: a partitioned log with consumer offsets; windowed delivery KPIs on event time; watermarks and late events; replay after a crash |
| 4 | **Real-time pipelines into the lakehouse** | Flink and Spark Structured Streaming; Kafka Connect and Debezium; topic-to-table (e.g. Tableflow); exactly-once sinks; small files and compaction; lambda vs. kappa; when *not* to stream | **3.4 Stream to Iceberg**: micro-batch the delivery stream into an Iceberg table idempotently; compact small files; serve a near-real-time SLA query |

## Day 4 · Production: Orchestration, Reliability, Governance & AI

| # | Session | Concepts | Lab (25') |
|---|---|---|---|
| 1 | **Orchestration** | DAGs; tasks vs. data assets; schedules, sensors and events; partitions and backfills; retries and idempotency; Airflow 3, Dagster, Prefect; CI/CD and environments | **4.1 Orchestrate Qafila with Dagster**: ingestion, dbt and Iceberg as software-defined assets; daily partitions; a backfill; asset checks — all run in the notebook |
| 2 | **Observability & data contracts** | Freshness, volume, schema, distribution and lineage; SLAs and SLOs; OpenLineage; the Open Data Contract Standard; breaking vs. non-breaking changes; FinOps basics | **4.2 Contracts and monitors**: write an ODCS contract; validate data against it; catch a breaking schema change; freshness and volume anomaly monitors |
| 3 | **Governance, security & privacy** | PDPL principles; NDMO classification levels and the asset register; PII discovery; masking, pseudonymization, tokenization; RBAC and row-level security; retention and erasure; catalogs and lineage | **4.3 Govern Qafila**: detect Saudi PII (national ID/Iqama, mobile, IBAN, email); classify columns; HMAC pseudonymization; role-based masked views; serve an erasure request on an Iceberg table |
| 4 | **Data engineering for AI + capstone kickoff** | Semantic/metrics layers; features and embeddings pipelines; unstructured data; vector search in DuckDB; text-to-SQL and agents over governed data (MCP); using AI assistants safely in data work | **4.4 AI-ready data**: define metrics in YAML and compile them to SQL; embed the bilingual product catalog; semantic search in DuckDB; optional LLM text-to-SQL restricted to the metrics layer |

## Day 5 · Capstone presentations

Teams of 2–3 present a working data platform: 10 minutes talk + live demo, 5 minutes technical Q&A, peer feedback (1 strength · 1 question · 1 suggestion), instructor review and closing.

### Capstone options

1. **Qafila Lakehouse**: batch + streaming platform for Qafila, from raw sources to governed marts and a near-real-time operations view.
2. **Open Data Platform**: ingest Saudi open datasets (e.g. the national open data portal or GASTAT releases) into a lakehouse with tests, a semantic layer and a small dashboard.
3. **Real-time Operations Monitor**: streaming pipeline with event-time KPIs, SLA alerts, an Iceberg history table and replay.

### Minimum requirements

1. An end-to-end pipeline from at least two source types to analytics-ready tables, runnable with one command.
2. A layered model (raw/bronze → staging/silver → marts/gold) with a documented grain for every fact table.
3. Incremental loading that is safe to re-run (idempotent), demonstrated live.
4. At least 8 data tests or contract checks, one of which catches a real problem in the data.
5. Orchestration (Dagster, Airflow or Prefect) or a justified alternative, plus a short governance note: classification of every column that holds personal data and how it is protected.

### Rubric

| Criterion | Weight |
|---|---|
| Functional end-to-end pipeline | 30% |
| Architecture & data modeling decisions | 25% |
| Data quality, testing & observability | 20% |
| Orchestration, governance & operability | 15% |
| Presentation & reproducibility | 10% |

## Lab stack

Python 3.10–3.13 · DuckDB · Polars · PyArrow / Parquet · dlt · dbt (v1) + dbt-duckdb · PyIceberg (SQL catalog) · PySpark 4 (Lab 3.2) · Pandera · Dagster + dagster-dbt · model2vec (static multilingual embeddings) · optional OpenAI-compatible LLM endpoint.
Everything runs on a laptop CPU or Google Colab; no Docker, cloud account or JVM needed except Java 17+ for the Spark lab (on Colab the lab installs it).
