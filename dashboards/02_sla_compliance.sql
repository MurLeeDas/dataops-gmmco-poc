-- GMMCO Dashboard — Panel 2: SLA Compliance by Branch
SELECT
    branch, region, fault_category, total_workorders,
    sla_met_count, sla_breached_count, sla_compliance_pct,
    avg_overdue_hours, performance_flag
FROM workspace.gmmco_poc.gold_sla_compliance
ORDER BY sla_compliance_pct ASC;