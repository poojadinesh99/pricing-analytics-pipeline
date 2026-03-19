SELECT 
    t.product_id,
    p.category,
    SUM(t.quantity) AS total_demand,
    AVG(t.price) AS avg_price,
    c.price AS competitor_price,
    SUM(t.price * t.quantity) AS revenue
FROM transactions t
JOIN products p ON t.product_id = p.product_id
LEFT JOIN competitors c ON t.product_id = c.product_id
GROUP BY t.product_id, p.category, c.price
