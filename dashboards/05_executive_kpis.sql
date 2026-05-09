-- GMMCO Dashboard — Panel 5: Executive KPI Summary
SELECT kpi_name, kpi_value, _gold_computed_at
FROM workspace.gmmco_poc.gold_executive_kpis
ORDER BY kpi_name;