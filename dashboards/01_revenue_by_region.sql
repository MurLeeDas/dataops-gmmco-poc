-- GMMCO Dashboard — Panel 1: Revenue by Region and Segment
SELECT
    region, industry_segment, sale_year, sale_quarter,
    total_deals, revenue_in_lakhs, avg_deal_value_inr, unique_customers
FROM workspace.gmmco_poc.gold_sales_performance
ORDER BY revenue_in_lakhs DESC;