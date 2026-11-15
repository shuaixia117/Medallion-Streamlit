# Retail Demand Intelligence — Medallion Lakehouse + Streamlit

An end-to-end lakehouse on **Databricks (PySpark + Delta Lake)** that turns ~988K raw omnichannel retail
transaction lines (60 stores · 600 SKUs · 12 categories · 3 channels · 2024–2025) into business-ready
Gold tables, and an interactive **Streamlit** dashboard over them.

```
raw parquet ─► Bronze (raw Delta + provenance) ─► Silver (clean, deduped, returns integrated)
            ─► Gold (wide fact + aggregates) ─► Q1–Q9 + CSV exports ─► Streamlit app
```

Design decisions, data-quality counts and expected results: **[docs/architecture.md](docs/architecture.md)**.

## Repository layout

```
notebooks/
  01_bronze_ingestion.py     raw → retail_bronze.*  (metadata cols, idempotent append, year/month partitions)
  02_silver_transform.py     quality rules, dim dedupe, returns aggregation (no fan-out), audit log
  03_gold_aggregate.py       retail_analytics (13 derived cols), store_performance, category_trends
  04_analysis_queries.py     Q1–Q9 + CSV exports for the app
app/
  app.py                     Streamlit dashboard (7 charts + reorder table + YoY)
  data/*.csv                 Gold exports the app reads (no live DB connection)
  requirements.txt
local/
  run_pipeline.py            run the same notebooks locally with PySpark + Delta
docs/architecture.md
```

## Run on Databricks (Free Edition or any Unity Catalog workspace)

1. Create the schema + volume and upload the data (brief §3.1):
   ```sql
   CREATE SCHEMA IF NOT EXISTS workspace.retail;
   CREATE VOLUME IF NOT EXISTS workspace.retail.raw;
   ```
   Upload to `/Volumes/workspace/retail/raw/final_project_retail/` keeping the `transactions/` sub-folder.
2. Import the notebooks: **Workspace → Import → File** (each `.py` opens as a notebook), or link this repo
   via **Workspace → Repos / Git folder**.
3. Attach serverless compute and run `01` → `02` → `03` → `04` in order. Every notebook ends with
   assertions; a failing count stops the run.
4. Download `/Volumes/workspace/retail/raw/app_exports/*.csv` into `app/data/` (or use
   `databricks fs cp -r dbfs:/Volumes/workspace/retail/raw/app_exports ./app/data --overwrite`).

Tables are created with two-level names (`retail_bronze.transactions`, …) and resolve in the default
`workspace` catalog.

## Run locally (no Databricks needed)

Requires Java 17+ and Python 3.10+.

```bash
python -m venv .venv && source .venv/bin/activate
pip install -r local/requirements.txt -r app/requirements.txt
python local/run_pipeline.py --src /path/to/final_project_retail --full-refresh   # all 4 notebooks, ~2 min
streamlit run app/app.py                                                          # http://localhost:8501
```

`run_pipeline.py` provides the two Databricks globals the notebooks use (`spark`, `display`), points them
at the local data, and keeps tables in a local Derby metastore (`local/_metastore`, git-ignored), so you
can also run a single layer: `python local/run_pipeline.py --src ... 02 03`.

## Deploy the app (Streamlit Community Cloud)

share.streamlit.io → **Create app** → this repo, branch `main`, main file `app/app.py`.
The app reads only `app/data/*.csv`, so it needs no secrets.
