-- GMMCO Dashboard — Panel 3: Parts Spend Analysis
SELECT
    part_description, urgency_level, warehouse_location,
    total_orders, total_quantity_ordered, spend_in_lakhs,
    unique_customers_ordering
FROM workspace.gmmco_poc.gold_parts_analysis
ORDER BY spend_in_lakhs DESC LIMIT 20;