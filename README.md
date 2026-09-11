# Bing Ads Performance Warehouse (MVP)

Airflow-orchestrated pipeline that parses `BING_MultiDays.csv` and loads a MySQL star schema designed for analyst queries and downstream Insights Agents.

**Full walkthrough (design, schema, and run steps):** [DOCUMENTATION.md](DOCUMENTATION.md)


This is an MVP: one source file, one warehouse, idempotent reload, enough modeling and task boundaries to extend later.

## Why this schema

The source is a denormalized Bing Ads report. The **fact grain** is one row per:

`date + ad + device (type/OS) + placement (network, auction slot, match types, language, currency)`

That grain is unique for all 12,497 source rows, so we do not aggregate on load.

| Table | Role |
| --- | --- |
| `dim_date` | Time intelligence (year/month/weekend) for agents that slice by calendar |
| `dim_account` | Customer + Bing account. `account_nk` is `Account number` |
| `dim_campaign` | Campaigns are unique per `(account_nk, campaign_name)` because names like `Brand` repeat |
| `dim_ad_group` | Bing ad group id (brackets stripped) plus parent campaign/account labels |
| `dim_ad` | Creative and landing-page attributes. Titles are often empty on expanded text ads |
| `dim_device` | Device type and OS |
| `dim_placement` | Auction/network context that is not an “entity” but drives performance |
| `fact_ad_performance` | Additive measures only |

`Avg. position` is not additive. We store `position_weighted_sum = avg_position * impressions` so any rollup can recompute:

`SUM(position_weighted_sum) / SUM(impressions)`

`vw_ad_performance` exposes CTR, CPC, CPA, and recomputed average position so an Insights Agent can query one object instead of joining seven tables.

Dimensions use surrogate keys; natural keys are unique. Reloads upsert dimensions and replace facts, so the DAG is safe to rerun.

## Pipeline tasks

```
extract -> validate -> transform -> apply_schema -> load
```

1. **extract** — Read the CSV as strings, keep the known column contract, pickle to `data/staging`.
2. **validate** — Required keys present, dates parse as `%m/%d/%Y`, metrics numeric and non-negative. Clicks > impressions is a warning, not a hard fail. Writes `validation_report.json`.
3. **transform** — Strip `[id]` wrappers, build dimension frames and the fact with natural keys.
4. **apply_schema** — Create tables/views if needed (`sql/schema.sql`).
5. **load** — Upsert dimensions, stage facts, join to surrogate keys, fail if any fact row cannot resolve a dimension.

The same functions run from Airflow (`dags/bing_ads_etl_dag.py`) or from `scripts/run_pipeline.py`.

## Run locally

Needs Docker Desktop (MySQL 8 + optional Airflow).

### Warehouse only (fastest demo)

```bash
docker compose up -d mysql
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
PYTHONPATH=src python scripts/run_pipeline.py
```

Then:

```bash
docker compose exec mysql mysql -uads -pads bing_ads_dw -e "SELECT COUNT(*) FROM fact_ad_performance; SELECT account_name, SUM(spend) spend FROM vw_ad_performance GROUP BY account_name ORDER BY spend DESC;"
```

MySQL is published on **host port 3307** so it does not collide with a local MySQL on 3306. Inside Docker the warehouse still listens on 3306.

More starter questions are in `sql/insights_queries.sql`.

### Full Airflow

```bash
docker compose up --build
```

UI: [http://localhost:8080](http://localhost:8080) — user `airflow` / password `airflow`.

Trigger DAG `bing_ads_performance_etl`. The scheduler and webserver share `./src`, `./dags`, `./data`, and `./sql`.

## Tests

```bash
PYTHONPATH=src pytest -q
```

## Layout

```
dags/bing_ads_etl_dag.py     Airflow DAG
src/bing_ads_etl/            extract, validate, transform, load
sql/schema.sql               warehouse DDL + analytics view
sql/insights_queries.sql     sample agent/analyst SQL
data/raw/BING_MultiDays.csv  source file
scripts/run_pipeline.py      run without the Airflow UI
```

## What we would add after the MVP

- Incremental loads by report date instead of full fact replace
- Type-2 history on campaign/ad status
- Separate keyword/query grain if a keyword report is added
- dbt tests on the warehouse in addition to the pre-load validator
