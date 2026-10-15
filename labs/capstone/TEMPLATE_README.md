# <Project name>

**Team:** <names> · **Option:** <1 / 2 / 3> · **Run it:** `uv run python pipeline.py`

## Problem
<Who needs this data, which decisions it supports, in 2–3 sentences.>

**Our platform takes** <sources> **and produces** <tables / dashboard / alerts>, **refreshed** <how often>, **trusted because** <tests, contracts, monitors>.

## Sources
| Source | Type | Volume | Refresh | Load strategy | Known issues |
|---|---|---|---|---|---|
| | DB / API / files / events | | | full / incremental / CDC / stream | |
| | | | | | |

## Architecture
<Diagram: sources → raw → staging → marts → serving. Name the tools at each step and why you chose them.>

Storage: <DuckDB file / Parquet / Iceberg> because <...>. Orchestrator: <...> because <...>.

## Data model
| Table | Layer | Grain (one row per...) | Keys | Materialization |
|---|---|---|---|---|
| | gold | | | table / incremental / view |
| | | | | |

## Quality and observability
| Check | Where | Severity | What it caught |
|---|---|---|---|
| | dbt test / contract / asset check / monitor | error / warn / quarantine | |
| | | | |

At least one real problem found: <what, how many rows, what you did about it>.

## Incremental loads and re-runs
<How each incremental load works (key, cursor, look-back), and the evidence that re-running gives the same result.>

## Governance note
| Column | Classification (NDMO level) | Protection | Who can see it raw |
|---|---|---|---|
| | | pseudonymized / masked / dropped / encrypted | |

Erasure: <how a PDPL deletion request would be handled end to end, including snapshots and derived tables>.

## Reproduce
```bash
uv sync
uv run python pipeline.py             # one command, end to end
uv run dagster dev -f pipeline.py     # UI (optional)
```
Data: <where it comes from, how to download it>. Tool versions: <see uv.lock>.

## Limitations and next steps
<What would break first at 100× the volume? What would you do with two more weeks?>
