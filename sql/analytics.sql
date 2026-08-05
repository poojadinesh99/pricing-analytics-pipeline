SELECT
    p.product_id,
    p.product_name,
    p.category,
    p.list_price,
    COALESCE(cq.quote_count, 0) AS quote_count,
    COALESCE(cq.avg_quoted_premium, p.list_price) AS avg_quoted_premium,
    COALESCE(cq.total_premium_revenue, 0) AS total_premium_revenue,
    comp.competitor_avg_premium,
    r.avg_risk_score,
    COALESCE(cl.claim_count, 0) AS claim_count,
    COALESCE(cl.total_claim_amount, 0) AS total_claim_amount,
    CASE
        WHEN COALESCE(cq.total_premium_revenue, 0) > 0
        THEN COALESCE(cl.total_claim_amount, 0) / cq.total_premium_revenue
        ELSE NULL
    END AS loss_ratio,
    h.latest_historical_premium,
    b.market_benchmark_premium
FROM products p
LEFT JOIN (
    SELECT
        product_id,
        COUNT(*) AS quote_count,
        AVG(premium_amount) AS avg_quoted_premium,
        SUM(CASE WHEN status = 'accepted' THEN premium_amount ELSE 0 END) AS total_premium_revenue
    FROM customer_quotes
    GROUP BY product_id
) cq ON p.product_id = cq.product_id
LEFT JOIN (
    SELECT product_id, AVG(premium_amount) AS competitor_avg_premium
    FROM competitor_quotes
    GROUP BY product_id
) comp ON p.product_id = comp.product_id
LEFT JOIN (
    SELECT product_id, AVG(risk_score) AS avg_risk_score
    FROM risk_factors
    GROUP BY product_id
) r ON p.product_id = r.product_id
LEFT JOIN (
    SELECT product_id, COUNT(*) AS claim_count, SUM(claim_amount) AS total_claim_amount
    FROM claims
    GROUP BY product_id
) cl ON p.product_id = cl.product_id
LEFT JOIN (
    SELECT product_id, MAX(avg_premium) AS latest_historical_premium
    FROM historical_premiums
    GROUP BY product_id
) h ON p.product_id = h.product_id
LEFT JOIN (
    SELECT product_id, AVG(industry_avg_premium) AS market_benchmark_premium
    FROM market_benchmarks
    GROUP BY product_id
) b ON p.product_id = b.product_id
