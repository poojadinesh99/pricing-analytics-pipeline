import os

import duckdb
import pandas as pd


def load_analytics():
    """Load analytics output if present, otherwise build it from processed CSVs."""

    # Prefer dbt output (fct_pricing table) when available.
    duckdb_path = "data/processed/pricing_dbt.duckdb"
    if os.path.exists(duckdb_path):
        con = duckdb.connect(duckdb_path)
        df = con.execute("SELECT * FROM fct_pricing").df()
        con.close()
        return df

    analytics_path = "data/processed/analytics_output.csv"
    if os.path.exists(analytics_path):
        df = pd.read_csv(analytics_path)

        # The analytics output can include multiple competitor prices per product.
        # Collapse to a single row per product by averaging competitor prices.
        if "competitor_avg_price" not in df.columns and "competitor_price" in df.columns:
            df = (
                df.groupby("product_id", as_index=False)
                .agg(
                    category=("category", "first"),
                    total_demand=("total_demand", "first"),
                    avg_price=("avg_price", "first"),
                    revenue=("revenue", "first"),
                    competitor_avg_price=("competitor_price", "mean"),
                )
            )

        return df

    # Fallback: derive analytics metrics from processed CSVs.
    tx = pd.read_csv("data/processed/transactions.csv")
    prod = pd.read_csv("data/processed/products.csv")
    comp = pd.read_csv("data/processed/competitors.csv")

    agg = (
        tx.groupby("product_id")
        .agg(total_demand=("quantity", "sum"), avg_price=("price", "mean"))
        .reset_index()
    )

    # revenue = sum(price * quantity)
    revenue = (
        tx.assign(revenue=tx["price"] * tx["quantity"])
        .groupby("product_id")["revenue"]
        .sum()
        .reset_index()
    )
    agg = agg.merge(revenue, on="product_id", how="left")

    agg = agg.merge(prod[["product_id", "category", "list_price"]], on="product_id", how="left")

    comp_avg = comp.groupby("product_id")["price"].mean().reset_index(name="competitor_avg_price")
    agg = agg.merge(comp_avg, on="product_id", how="left")

    return agg


def recommend_price(df: pd.DataFrame) -> pd.DataFrame:
    """Add price recommendations based on demand and competitor benchmarking."""

    # Normalize column naming between different upstream outputs
    if "competitor_avg_price" not in df.columns and "competitor_price" in df.columns:
        df = df.rename(columns={"competitor_price": "competitor_avg_price"})

    # Category-level demand baseline
    df["category_avg_demand"] = df.groupby("category")["total_demand"].transform("mean")
    df["demand_factor"] = df["total_demand"] / df["category_avg_demand"].replace(0, 1)

    # Use competitor price where available, otherwise fall back to our avg price
    df["benchmark_price"] = df["competitor_avg_price"].fillna(df["avg_price"])

    # Simple blended suggestion (tunable weights)
    df["suggested_base"] = df["benchmark_price"] * 0.6 + df["avg_price"] * 0.4

    # Adjust by demand (small bump/dip around the category mean)
    df["suggested_price"] = df["suggested_base"] * (1 + 0.1 * (df["demand_factor"] - 1))

    # Guardrails: don't stray too far from known prices
    df["min_price"] = df["avg_price"] * 0.9
    df["max_price"] = df["benchmark_price"].fillna(df["avg_price"]) * 1.1
    df["suggested_price"] = df["suggested_price"].clip(lower=df["min_price"], upper=df["max_price"])

    return df[
        [
            "product_id",
            "category",
            "total_demand",
            "avg_price",
            "competitor_avg_price",
            "demand_factor",
            "suggested_price",
        ]
    ]


if __name__ == "__main__":
    analytics = load_analytics()
    recommendations = recommend_price(analytics)
    recommendations.to_csv("data/processed/pricing_recommendations.csv", index=False)
    print(recommendations)