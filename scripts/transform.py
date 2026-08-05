"""Run DuckDB SQL analytics against processed CSVs (pre-dbt fallback)."""

from __future__ import annotations

from pathlib import Path

import duckdb

ROOT = Path(__file__).resolve().parents[1]
PROCESSED = ROOT / "data" / "processed"


def run_transform() -> None:
    con = duckdb.connect()

    tables = [
        "products",
        "customer_quotes",
        "competitor_quotes",
        "risk_factors",
        "claims",
        "historical_premiums",
        "market_benchmarks",
    ]
    for table in tables:
        path = PROCESSED / f"{table}.csv"
        con.execute(f"CREATE OR REPLACE TABLE {table} AS SELECT * FROM '{path}'")

    query = (ROOT / "sql" / "analytics.sql").read_text()
    result = con.execute(query).df()
    result.to_csv(PROCESSED / "analytics_output.csv", index=False)
    print(result.to_string(index=False))


if __name__ == "__main__":
    run_transform()
