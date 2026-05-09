-- GMMCO Dashboard — Panel 4: Customer Health Portfolio
SELECT
    customer_health, customer_tier, industry_segment, region,
    COUNT(*) as customer_count,
    ROUND(SUM(revenue_in_lakhs), 2) as total_revenue_lakhs,
    ROUND(AVG(total_service_calls), 1) as avg_service_calls
FROM workspace.gmmco_poc.gold_customer_portfolio
GROUP BY customer_health, customer_tier, industry_segment, region
ORDER BY total_revenue_lakhs DESC;