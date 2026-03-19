# Pricing Analytics Pipeline

A production-style analytics workflow that ingests, models, and scores price-related data, delivering data quality controls and an operational dashboard for commercial pricing decisions.

Built with Python, SQL, DuckDB, dbt, and Streamlit for a complete analytics engineering lifecycle.

## Architecture

1. Raw transaction and competitor CSV sources in `data/raw/`.
2. Ingestion pipeline (`scripts/ingestion.py`) creates cleaned staging CSVs in `data/processed/`.
3. DuckDB transformation executes `sql/analytics.sql` (`scripts/transform.py`) to generate analytics insights.
4. dbt project (`pricing_dbt/`) defines staging models (`stg_*`) and fact pricing mart (`fct_pricing`).
5. Pricing logic (`scripts/pricing_model.py`) calculates recommended price and writes `pricing_recommendations.csv`.
6. Streamlit dashboard (`streamlit_app.py`) exposes business metrics and refresh workflow.
7. CI pipeline (`.github/workflows/ci.yml`) validates all layers.

## Key Features

- End-to-end ETL + analytics pipeline (ingest → transform → model → score → visualize)
- dbt staging models: `stg_transactions`, `stg_products`, `stg_competitors`
- dbt fact model: `fct_pricing` (materialized table)
- dbt tests: `not_null`, `unique` for core columns
- Pricing recommendation using demand signal and competitor price benchmark
- Streamlit dashboard: revenue, demand, competitor comparison, suggested price
- In-dashboard pipeline refresh (dbt + pricing model)
- CI workflow coverage (build/test/pipeline)

## Tech Stack

- Python (pipeline orchestration, pricing logic, dashboard)
- SQL + DuckDB (analytical engine, local table storage)
- dbt (modeled transformations, schema tests)
- Streamlit (UI for metrics and controls)
- GitHub Actions (CI automation)

## Data Pipeline Explanation

- Ingestion: `scripts/ingestion.py` processes raw CSV input.
- Transformation: `scripts/transform.py` runs SQL from `sql/analytics.sql` in DuckDB.
- Modeling: dbt builds `stg_*` and `fct_pricing`; data tests run within dbt.
- Pricing logic: `scripts/pricing_model.py` computes demand-adjusted and competitor-based suggestions.
- Dashboard: `streamlit_app.py` surfaces model output and recommended price with filters.

## Business Use Case

- Supports data-driven pricing decisions through an integrated demand/competitor signal.
- Enables revenue and demand monitoring for each SKU/category.
- Provides recommended prices with guardrails to minimize down-side risk.
- Supports real-time analytics workflow for commercial strategy, markdown planning, and competitive responses.

## Design Decisions

- dbt for repeatable, version-controlled modeling and quality assertions.
- DuckDB for fast local analytics, built-in CSV compatibility, and slicing in pipeline and dashboard.
- SQL-first data transformation ensures visibility into business logic and auditability.
- Python orchestration separates historical model scoring and operational recommendation logic.
- CI integration guarantees data quality and build reproducibility.

## How to Run

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

# Build models and tests
cd pricing_dbt
dbt run
dbt test

# Run pipeline and generate recommendations
cd ..
python scripts/run_pipeline.py

# Launch dashboard (repo root)
streamlit run streamlit_app.py
```

## Future Improvements

- Add time-based price elasticity and trend modeling
- Add automated drift detection / alerting in CI
- Add product-level scenario simulations in the dashboard
- Containerize pipeline for deployment (Docker + managed dbt)
