"""Ingest raw insurance pricing CSVs and write cleaned staging files."""

from __future__ import annotations

from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
RAW = ROOT / "data" / "raw"
PROCESSED = ROOT / "data" / "processed"


def _load(name: str) -> pd.DataFrame:
    return pd.read_csv(RAW / name)


def _clean_products(df: pd.DataFrame) -> pd.DataFrame:
    df = df.drop_duplicates(subset=["product_id"])
    df["list_price"] = df["list_price"].astype(float)
    return df


def _clean_customer_quotes(df: pd.DataFrame) -> pd.DataFrame:
    df = df.drop_duplicates(subset=["quote_id"])
    df["premium_amount"] = df["premium_amount"].astype(float)
    df["coverage_limit"] = df["coverage_limit"].astype(float)
    df["quote_date"] = pd.to_datetime(df["quote_date"])
    return df


def _clean_competitor_quotes(df: pd.DataFrame) -> pd.DataFrame:
    df = df.drop_duplicates(subset=["quote_id"])
    df["premium_amount"] = df["premium_amount"].astype(float)
    df["quote_date"] = pd.to_datetime(df["quote_date"])
    return df


def _clean_risk_factors(df: pd.DataFrame) -> pd.DataFrame:
    df = df.drop_duplicates(subset=["product_id", "customer_id"])
    df["risk_score"] = df["risk_score"].clip(0, 1)
    df["claims_history_count"] = df["claims_history_count"].astype(int)
    return df


def _clean_claims(df: pd.DataFrame) -> pd.DataFrame:
    df = df.drop_duplicates(subset=["claim_id"])
    df["claim_amount"] = df["claim_amount"].astype(float)
    df["claim_date"] = pd.to_datetime(df["claim_date"])
    return df


def _clean_historical_premiums(df: pd.DataFrame) -> pd.DataFrame:
    df = df.drop_duplicates(subset=["product_id", "premium_year"])
    df["avg_premium"] = df["avg_premium"].astype(float)
    df["policy_count"] = df["policy_count"].astype(int)
    df["renewal_rate"] = df["renewal_rate"].astype(float)
    return df


def _clean_market_benchmarks(df: pd.DataFrame) -> pd.DataFrame:
    df = df.drop_duplicates(subset=["product_id", "benchmark_year"])
    df["industry_avg_premium"] = df["industry_avg_premium"].astype(float)
    df["market_share_pct"] = df["market_share_pct"].astype(float)
    df["inflation_rate"] = df["inflation_rate"].astype(float)
    return df


def run_ingestion() -> None:
    PROCESSED.mkdir(parents=True, exist_ok=True)

    cleaners = {
        "products": _clean_products,
        "customer_quotes": _clean_customer_quotes,
        "competitor_quotes": _clean_competitor_quotes,
        "risk_factors": _clean_risk_factors,
        "claims": _clean_claims,
        "historical_premiums": _clean_historical_premiums,
        "market_benchmarks": _clean_market_benchmarks,
    }

    for stem, cleaner in cleaners.items():
        raw_path = RAW / f"{stem}.csv"
        if not raw_path.exists():
            raise FileNotFoundError(f"Missing raw file: {raw_path}")
        cleaned = cleaner(_load(f"{stem}.csv"))
        cleaned.to_csv(PROCESSED / f"{stem}_clean.csv", index=False)
        cleaned.to_csv(PROCESSED / f"{stem}.csv", index=False)
        print(f"  {stem}: {len(cleaned)} rows")

    print("Data ingestion completed successfully.")


if __name__ == "__main__":
    run_ingestion()
