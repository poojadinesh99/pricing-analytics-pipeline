{{ config(materialized='table') }}

WITH customer_quotes AS (
    SELECT
        product_id,
        COUNT(*) AS quote_count,
        AVG(premium_amount) AS avg_quoted_premium,
        SUM(CASE WHEN status = 'accepted' THEN premium_amount ELSE 0 END) AS total_premium_revenue,
        SUM(CASE WHEN status = 'accepted' THEN 1 ELSE 0 END) AS accepted_quotes
    FROM {{ ref('stg_customer_quotes') }}
    GROUP BY product_id
),

competitor_quotes AS (
    SELECT
        product_id,
        AVG(premium_amount) AS competitor_avg_premium
    FROM {{ ref('stg_competitor_quotes') }}
    GROUP BY product_id
),

risk AS (
    SELECT
        product_id,
        AVG(risk_score) AS avg_risk_score,
        AVG(claims_history_count) AS avg_prior_claims
    FROM {{ ref('stg_risk_factors') }}
    GROUP BY product_id
),

claims AS (
    SELECT
        product_id,
        COUNT(*) AS claim_count,
        SUM(claim_amount) AS total_claim_amount
    FROM {{ ref('stg_claims') }}
    GROUP BY product_id
),

historical AS (
    SELECT
        product_id,
        AVG(avg_premium) AS avg_historical_premium,
        MAX(CASE WHEN premium_year = (SELECT MAX(premium_year) FROM {{ ref('stg_historical_premiums') }}) THEN avg_premium END) AS latest_historical_premium,
        AVG(renewal_rate) AS avg_renewal_rate
    FROM {{ ref('stg_historical_premiums') }}
    GROUP BY product_id
),

benchmarks AS (
    SELECT
        product_id,
        AVG(industry_avg_premium) AS market_benchmark_premium,
        AVG(market_share_pct) AS market_share_pct,
        AVG(inflation_rate) AS inflation_rate
    FROM {{ ref('stg_market_benchmarks') }}
    GROUP BY product_id
)

SELECT
    p.product_id,
    p.product_name,
    p.category,
    p.list_price,
    COALESCE(cq.quote_count, 0) AS quote_count,
    COALESCE(cq.avg_quoted_premium, p.list_price) AS avg_quoted_premium,
    COALESCE(cq.total_premium_revenue, 0) AS total_premium_revenue,
    COALESCE(cq.accepted_quotes, 0) AS accepted_quotes,
    comp.competitor_avg_premium,
    r.avg_risk_score,
    r.avg_prior_claims,
    COALESCE(cl.claim_count, 0) AS claim_count,
    COALESCE(cl.total_claim_amount, 0) AS total_claim_amount,
    CASE
        WHEN COALESCE(cq.total_premium_revenue, 0) > 0
        THEN COALESCE(cl.total_claim_amount, 0) / cq.total_premium_revenue
        ELSE NULL
    END AS loss_ratio,
    h.avg_historical_premium,
    h.latest_historical_premium,
    h.avg_renewal_rate,
    b.market_benchmark_premium,
    b.market_share_pct,
    b.inflation_rate
FROM {{ ref('stg_products') }} p
LEFT JOIN customer_quotes cq ON p.product_id = cq.product_id
LEFT JOIN competitor_quotes comp ON p.product_id = comp.product_id
LEFT JOIN risk r ON p.product_id = r.product_id
LEFT JOIN claims cl ON p.product_id = cl.product_id
LEFT JOIN historical h ON p.product_id = h.product_id
LEFT JOIN benchmarks b ON p.product_id = b.product_id
