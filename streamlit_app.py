import os
import subprocess
import sys

import duckdb
import pandas as pd
import streamlit as st


if not os.path.exists("data/processed/pricing_dbt.duckdb"):
    subprocess.run([sys.executable, "scripts/run_pipeline.py"], check=True)
from scripts.portfolio_metrics import calculate_irr, simulate_scenario


@st.cache_data
def load_fct_pricing():
    con = duckdb.connect("data/processed/pricing_dbt.duckdb")
    df = con.execute("SELECT * FROM fct_pricing").df()
    con.close()
    return df


def load_pricing_recommendations():
    path = "data/processed/pricing_recommendations.csv"
    if not os.path.exists(path):
        return None
    return pd.read_csv(path)


def main():
    st.set_page_config(page_title="Insurance Pricing Analytics", layout="wide")
    st.title("Insurance Pricing Analytics Dashboard")

    st.sidebar.header("Actions")
    if st.sidebar.button("Refresh pipeline (ingest + dbt + pricing)"):
        with st.spinner("Rebuilding pipeline…"):
            subprocess.run([sys.executable, "scripts/run_pipeline.py"], check=True)
            load_fct_pricing.clear()
            st.rerun()

    st.sidebar.markdown("---")
    st.sidebar.markdown("*Run the pipeline first if the dashboard is empty.*")

    if not os.path.exists("data/processed/pricing_dbt.duckdb"):
        st.warning("No data found. Click **Refresh pipeline** or run `python scripts/run_pipeline.py`.")
        return

    df = load_fct_pricing()
    rec = load_pricing_recommendations()
    if rec is not None:
        df = df.merge(rec[["product_id", "suggested_premium"]], on="product_id", how="left")

    st.sidebar.header("Filters")
    category_choices = sorted(df["category"].dropna().unique())
    categories = st.sidebar.multiselect("Category", options=category_choices, default=category_choices)

    filtered_df = df[df["category"].isin(categories)] if categories else df.copy()
    if filtered_df.empty:
        st.warning("No products match the current filters.")
        return

    # KPI row
    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Products", len(filtered_df))
    col2.metric("Avg Quoted Premium", f"${filtered_df['avg_quoted_premium'].mean():.2f}")
    col3.metric("Avg Loss Ratio", f"{filtered_df['loss_ratio'].mean():.1%}" if filtered_df["loss_ratio"].notna().any() else "N/A")
    col4.metric("Avg Risk Score", f"{filtered_df['avg_risk_score'].mean():.2f}" if filtered_df["avg_risk_score"].notna().any() else "N/A")

    st.markdown("---")
    st.markdown("## Pricing recommendations")

    product = st.selectbox(
        "Select product",
        options=sorted(filtered_df["product_id"].unique()),
        format_func=lambda pid: f"{pid} — {filtered_df.loc[filtered_df['product_id'] == pid, 'product_name'].iloc[0]}",
    )
    selected = filtered_df[filtered_df["product_id"] == product].iloc[0]

    tab_quotes, tab_risk, tab_claims, tab_benchmarks = st.tabs(
        ["Customer & Competitor Quotes", "Risk Factors", "Claims", "Market Benchmarks"]
    )

    with tab_quotes:
        c1, c2 = st.columns(2)
        c1.metric("Avg Quoted Premium", f"${selected['avg_quoted_premium']:.2f}")
        c2.metric("Competitor Avg Premium", f"${selected['competitor_avg_premium']:.2f}" if pd.notna(selected.get("competitor_avg_premium")) else "N/A")
        c1.metric("Quote Count", int(selected.get("quote_count", 0)))
        c2.metric("Accepted Quotes", int(selected.get("accepted_quotes", 0)))

    with tab_risk:
        st.metric("Avg Risk Score", f"{selected['avg_risk_score']:.2f}" if pd.notna(selected.get("avg_risk_score")) else "N/A")
        st.metric("Avg Prior Claims (history)", f"{selected['avg_prior_claims']:.1f}" if pd.notna(selected.get("avg_prior_claims")) else "N/A")

    with tab_claims:
        st.metric("Claim Count", int(selected.get("claim_count", 0)))
        st.metric("Total Claim Amount", f"${selected['total_claim_amount']:,.2f}")
        lr = selected.get("loss_ratio")
        st.metric("Loss Ratio", f"{lr:.1%}" if pd.notna(lr) else "N/A")

    with tab_benchmarks:
        st.metric("Market Benchmark Premium", f"${selected['market_benchmark_premium']:.2f}" if pd.notna(selected.get("market_benchmark_premium")) else "N/A")
        st.metric("Latest Historical Premium", f"${selected['latest_historical_premium']:.2f}" if pd.notna(selected.get("latest_historical_premium")) else "N/A")
        st.metric("Market Share", f"{selected['market_share_pct']:.1%}" if pd.notna(selected.get("market_share_pct")) else "N/A")

    st.markdown("### Suggested premium")
    suggested = selected.get("suggested_premium")
    if pd.notna(suggested):
        delta = suggested - selected["avg_quoted_premium"]
        st.metric("Suggested Premium", f"${suggested:.2f}", delta=f"${delta:+.2f}")
    else:
        st.info("Run the pricing model to generate suggested premiums.")

    st.markdown("### Portfolio Return (IRR)")
    base_revenue = selected.get("total_premium_revenue", 0) or selected["avg_quoted_premium"]
    cashflows = [-1000.0] + [base_revenue * (0.8 + i * 0.05) for i in range(5)]
    try:
        irr_value = calculate_irr(cashflows)
        st.metric("IRR", f"{irr_value * 100:.2f}%")
    except Exception as e:
        st.warning(f"IRR calculation not available: {e}")

    st.markdown("### Scenario Simulation")
    scenario_multiplier = st.slider("Premium shock multiplier", 0.5, 1.5, 1.0, 0.05)
    scenario_cashflows = simulate_scenario(cashflows, scenario_multiplier)
    st.dataframe(
        pd.DataFrame({"period": range(len(scenario_cashflows)), "cashflow": scenario_cashflows}),
        width="stretch",
    )

    st.markdown("---")
    st.markdown("### Full pricing table")
    display_cols = [
        "product_id", "product_name", "category", "avg_quoted_premium",
        "competitor_avg_premium", "market_benchmark_premium", "avg_risk_score",
        "loss_ratio", "suggested_premium",
    ]
    display_cols = [c for c in display_cols if c in filtered_df.columns]
    st.dataframe(filtered_df[display_cols], width="stretch")

    st.markdown("### Avg Quoted Premium vs Market Benchmark")
    chart_df = filtered_df[["product_id", "avg_quoted_premium", "market_benchmark_premium"]].set_index("product_id")
    st.bar_chart(chart_df)


if __name__ == "__main__":
    main()
