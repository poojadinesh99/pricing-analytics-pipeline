import os
import duckdb
import pandas as pd
import streamlit as st
import subprocess
import sys


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
    st.set_page_config(page_title="Pricing Analytics Dashboard", layout="wide")
    st.title("Pricing Analytics Dashboard")

    st.sidebar.header("Actions")

    if st.sidebar.button("Refresh pipeline (dbt + pricing)"):
        with st.spinner("Rebuilding pipeline (this may take a moment)…"):
            subprocess.run([sys.executable, "scripts/run_pipeline.py"], check=True)
            # Clear cached data so the refreshed data is loaded
            load_fct_pricing.clear()
            st.experimental_rerun()

    st.sidebar.markdown("---")
    st.sidebar.markdown("*Tip: run the pipeline first if the dashboard is empty.*")

    df = load_fct_pricing()
    rec = load_pricing_recommendations()
    if rec is not None:
        df = df.merge(rec[["product_id", "suggested_price"]], on="product_id", how="left")

    # Sidebar filters
    st.sidebar.header("Filters")
    category_choices = sorted(df["category"].dropna().unique())
    categories = st.sidebar.multiselect("Category", options=category_choices, default=category_choices)
    min_demand, max_demand = st.sidebar.slider(
        "Total demand range",
        float(df["total_demand"].min()),
        float(df["total_demand"].max()),
        (float(df["total_demand"].min()), float(df["total_demand"].max())),
    )

    filtered_df = df.copy()
    if categories:
        filtered_df = filtered_df[filtered_df["category"].isin(categories)]
    filtered_df = filtered_df[
        (filtered_df["total_demand"] >= min_demand) & (filtered_df["total_demand"] <= max_demand)
    ]

    if filtered_df.empty:
        st.warning("No products match the current filters.")
        return

    df = filtered_df

    if df.empty:
        st.warning("No pricing data found. Run `dbt run` first to build the fct_pricing model.")
        return

    st.markdown("## Pricing recommendations")
    st.write(
        "This dashboard uses the `fct_pricing` dbt model as the source of truth."
    )

    product = st.selectbox("Select product", options=sorted(df["product_id"].unique()))
    selected = df[df["product_id"] == product].copy()

    st.markdown("### Product summary")
    st.table(
        selected[
            [
                "product_id",
                "category",
                "total_demand",
                "avg_price",
                "competitor_price",
                "revenue",
            ]
        ]
    )

    st.markdown("### Suggested price")
    suggested = selected["suggested_price"].iloc[0] if "suggested_price" in selected.columns else None
    if suggested is not None:
        st.metric("Suggested price", f"${suggested:.2f}")
    else:
        st.info("No suggested price column available (run the pricing model pipeline).")

    st.markdown("---")
    st.markdown("### Pricing table (all products)")
    st.dataframe(df, use_container_width=True)

    st.markdown("---")
    st.markdown("### Demand vs Average Price")
    chart = (
        df.sort_values("total_demand", ascending=False)
        .head(20)
        .reset_index(drop=True)
        .loc[:, ["product_id", "total_demand", "avg_price"]]
    )
    st.bar_chart(chart.set_index("product_id"))


if __name__ == "__main__":
    main()
