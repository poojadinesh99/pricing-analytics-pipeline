# pricing_dbt

dbt project for the [Pricing Analytics Pipeline](../README.md). Transforms staged insurance data (customer quotes, competitor quotes, risk factors, claims, historical premiums, market benchmarks, products) into `fct_pricing`, the fact table consumed by the pricing model and Streamlit dashboard.

## Models

- `stg_*` — one staging model per raw source, under `models/`.
- `fct_pricing` — joins staging models on `product_id` into per-product pricing, risk, and loss metrics.

## Commands

Run from the repo root via `python scripts/run_pipeline.py`, or directly:

```bash
dbt build --profiles-dir ../.dbt
```

See the [repo root README](../README.md) for full pipeline and dashboard docs.
