"""Insurance pricing recommendation engine."""

from __future__ import annotations

import os

import duckdb
import pandas as pd


def load_analytics() -> pd.DataFrame:
    """Load fct_pricing from DuckDB, or fall back to analytics CSV."""

    duckdb_path = "data/processed/pricing_dbt.duckdb"
    if os.path.exists(duckdb_path):
        con = duckdb.connect(duckdb_path)
        df = con.execute("SELECT * FROM fct_pricing").df()
        con.close()
        return df

    analytics_path = "data/processed/analytics_output.csv"
    if os.path.exists(analytics_path):
        return pd.read_csv(analytics_path)

    raise FileNotFoundError(
        "No analytics data found. Run ingestion and dbt first: python scripts/run_pipeline.py"
    )


def recommend_price(df: pd.DataFrame) -> pd.DataFrame:
    """Compute suggested premium using risk, claims, competitor, and benchmark signals."""

    out = df.copy()

    out["benchmark_premium"] = out["market_benchmark_premium"].fillna(out["avg_quoted_premium"])
    out["competitor_premium"] = out["competitor_avg_premium"].fillna(out["benchmark_premium"])
    out["risk_score"] = out["avg_risk_score"].fillna(0.5)
    out["loss_ratio"] = out["loss_ratio"].fillna(0.0)

    # Base: blend competitor (40%), benchmark (30%), historical (30%)
    out["historical_premium"] = out["latest_historical_premium"].fillna(out["avg_quoted_premium"])
    out["base_premium"] = (
        out["competitor_premium"] * 0.40
        + out["benchmark_premium"] * 0.30
        + out["historical_premium"] * 0.30
    )

    # Risk adjustment: +/- 15% based on risk score (0.5 = neutral)
    out["risk_adjustment"] = 1.0 + 0.15 * (out["risk_score"] - 0.5)

    # Loss ratio adjustment: increase premium if loss ratio exceeds 0.6
    out["loss_adjustment"] = out["loss_ratio"].apply(
        lambda lr: 1.0 + max(0.0, (lr - 0.6) * 0.5) if pd.notna(lr) else 1.0
    )

    out["suggested_premium"] = (
        out["base_premium"] * out["risk_adjustment"] * out["loss_adjustment"]
    )

    # Guardrails: stay within 90–115% of current avg quoted premium
    out["min_premium"] = out["avg_quoted_premium"] * 0.90
    out["max_premium"] = out["avg_quoted_premium"] * 1.15
    out["suggested_premium"] = out["suggested_premium"].clip(
        lower=out["min_premium"], upper=out["max_premium"]
    )

    return out[
        [
            "product_id",
            "product_name",
            "category",
            "avg_quoted_premium",
            "competitor_avg_premium",
            "market_benchmark_premium",
            "avg_risk_score",
            "loss_ratio",
            "base_premium",
            "risk_adjustment",
            "loss_adjustment",
            "suggested_premium",
        ]
    ]


if __name__ == "__main__":
    analytics = load_analytics()
    recommendations = recommend_price(analytics)
    recommendations.to_csv("data/processed/pricing_recommendations.csv", index=False)
    print(recommendations.to_string(index=False))
