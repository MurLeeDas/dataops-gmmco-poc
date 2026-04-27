# Databricks notebook source
# ── CONFIGURATION ─────────────────────────────────────────────
CATALOG   = "workspace"
SCHEMA    = "gmmco_poc"
SILVER_DB = f"{CATALOG}.{SCHEMA}"
GOLD_DB   = f"{CATALOG}.{SCHEMA}"

print("Gold aggregation configuration loaded.")
print(f"Reading from : {SILVER_DB}")
print(f"Writing to   : {GOLD_DB}")

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.functions import (
    col, sum as spark_sum, count, avg, round as spark_round,
    max as spark_max, min as spark_min, countDistinct,
    current_timestamp, lit, when, rank
)
from pyspark.sql.window import Window
from datetime import datetime

print("Libraries loaded.")

# COMMAND ----------

# ── GOLD 1: SALES PERFORMANCE ─────────────────────────────────
# Business question: Which region and industry segment are
# driving the most revenue? How is each performing by quarter?
#
# Who uses this: Sales Director, Regional Managers, CFO
# Replaces: Manual Excel pivot tables done weekly

print("Building: gold_sales_performance\n")

df_sales = spark.table(f"{SILVER_DB}.silver_equipment_sales")
df_customers = spark.table(f"{SILVER_DB}.silver_customers")

# Join sales with customer master to get tier information
df_joined = df_sales.join(
    df_customers.select("customer_id", "customer_tier"),
    on="customer_id",
    how="left"
)

# Aggregate by region, segment, quarter, year
df_gold = (df_joined
    .groupBy("region", "industry_segment",
             "sale_year", "sale_quarter", "customer_tier")
    .agg(
        count("sale_id")
            .alias("total_deals"),
        spark_sum("sale_amount_inr")
            .alias("total_revenue_inr"),
        spark_round(avg("sale_amount_inr"), 0)
            .alias("avg_deal_value_inr"),
        spark_max("sale_amount_inr")
            .alias("largest_deal_inr"),
        countDistinct("customer_id")
            .alias("unique_customers"),
        countDistinct("salesperson_id")
            .alias("active_salespeople")
    )
    .withColumn("revenue_in_lakhs",
                spark_round(col("total_revenue_inr") / 100000, 2))
    .withColumn("_gold_computed_at", current_timestamp())
    .orderBy("sale_year", "sale_quarter",
             col("total_revenue_inr").desc())
)

(df_gold.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{GOLD_DB}.gold_sales_performance"))

print(f"  Rows written: {df_gold.count():,}")
print("  gold_sales_performance ✓\n")

# Preview
spark.table(f"{GOLD_DB}.gold_sales_performance") \
    .select("region", "industry_segment", "sale_year",
            "sale_quarter", "total_deals",
            "revenue_in_lakhs", "unique_customers") \
    .show(10, truncate=False)

# COMMAND ----------

# ── GOLD 2: SLA COMPLIANCE ────────────────────────────────────
# Business question: Which branches are meeting their SLA
# commitments? Which fault types are causing the most breaches?
#
# Who uses this: Service Director, Branch Managers, Operations
# Replaces: Weekly manual reports compiled from spreadsheets
# GMMCO impact: SLA compliance directly affects contract renewals

print("Building: gold_sla_compliance\n")

df_wo = spark.table(f"{SILVER_DB}.silver_service_workorders")

df_gold = (df_wo
    .groupBy("branch", "region",
             "fault_category", "sla_hours_promised")
    .agg(
        count("workorder_id")
            .alias("total_workorders"),
        spark_sum(when(col("sla_breach") == False, 1).otherwise(0))
            .alias("sla_met_count"),
        spark_sum(when(col("sla_breach") == True, 1).otherwise(0))
            .alias("sla_breached_count"),
        spark_round(avg("resolution_days"), 1)
            .alias("avg_resolution_days"),
        spark_round(avg("overdue_hours"), 1)
            .alias("avg_overdue_hours"),
        spark_round(avg("labour_hours"), 1)
            .alias("avg_labour_hours"),
        spark_round(avg("parts_cost_inr"), 0)
            .alias("avg_parts_cost_inr"),
        spark_max("overdue_hours")
            .alias("worst_breach_hours")
    )
    .withColumn("sla_compliance_pct",
                spark_round(
                    col("sla_met_count") /
                    col("total_workorders") * 100, 1))
    .withColumn("performance_flag",
                when(col("sla_compliance_pct") >= 90, lit("GREEN"))
                .when(col("sla_compliance_pct") >= 75, lit("AMBER"))
                .otherwise(lit("RED")))
    .withColumn("_gold_computed_at", current_timestamp())
    .orderBy(col("sla_compliance_pct").asc())
)

(df_gold.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{GOLD_DB}.gold_sla_compliance"))

print(f"  Rows written: {df_gold.count():,}")
print("  gold_sla_compliance ✓\n")

# Show branches in RED performance
print("  Branches needing attention (RED flag):\n")
spark.table(f"{GOLD_DB}.gold_sla_compliance") \
    .filter(col("performance_flag") == "RED") \
    .select("branch", "fault_category",
            "total_workorders", "sla_compliance_pct",
            "avg_overdue_hours", "performance_flag") \
    .show(10, truncate=False)

# COMMAND ----------

# ── GOLD 3: PARTS INVENTORY ───────────────────────────────────
# Business question: Which parts are ordered most frequently?
# Which are critical urgency? Where is most of the parts
# spend concentrated?
#
# Who uses this: Parts Manager, Procurement, Finance
# GMMCO impact: Optimise stock levels, reduce emergency orders

print("Building: gold_parts_analysis\n")

df_parts = spark.table(f"{SILVER_DB}.silver_parts_orders")

df_gold = (df_parts
    .groupBy("part_number", "part_description",
             "warehouse_location", "urgency_level")
    .agg(
        count("order_id")
            .alias("total_orders"),
        spark_sum("quantity_ordered")
            .alias("total_quantity_ordered"),
        spark_sum("total_value_inr")
            .alias("total_spend_inr"),
        spark_round(avg("unit_price_inr"), 0)
            .alias("avg_unit_price_inr"),
        spark_max("total_value_inr")
            .alias("largest_single_order_inr"),
        countDistinct("customer_id")
            .alias("unique_customers_ordering")
    )
    .withColumn("spend_in_lakhs",
                spark_round(col("total_spend_inr") / 100000, 2))
    .withColumn("reorder_priority",
                when(col("urgency_level") == "CRITICAL", lit(1))
                .when(col("urgency_level") == "URGENT",   lit(2))
                .otherwise(lit(3)))
    .withColumn("_gold_computed_at", current_timestamp())
    .orderBy("reorder_priority",
             col("total_orders").desc())
)

(df_gold.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{GOLD_DB}.gold_parts_analysis"))

print(f"  Rows written: {df_gold.count():,}")
print("  gold_parts_analysis ✓\n")

# Top 5 parts by spend
print("  Top 5 parts by total spend:\n")
spark.table(f"{GOLD_DB}.gold_parts_analysis") \
    .select("part_description", "warehouse_location",
            "total_orders", "total_quantity_ordered",
            "spend_in_lakhs", "urgency_level") \
    .orderBy(col("spend_in_lakhs").desc()) \
    .show(5, truncate=False)

# COMMAND ----------

# ── GOLD 4: CUSTOMER PORTFOLIO ────────────────────────────────
# Business question: Who are our most valuable customers?
# What is the revenue and service footprint per customer?
#
# Who uses this: Account Managers, Sales Director, CEO
# GMMCO impact: Identify at-risk customers, prioritise renewals

print("Building: gold_customer_portfolio\n")

df_customers = spark.table(f"{SILVER_DB}.silver_customers")
df_sales     = spark.table(f"{SILVER_DB}.silver_equipment_sales")
df_wo        = spark.table(f"{SILVER_DB}.silver_service_workorders")

# Sales summary per customer
df_sales_summary = (df_sales
    .groupBy("customer_id")
    .agg(
        count("sale_id").alias("total_purchases"),
        spark_sum("sale_amount_inr").alias("total_revenue_inr"),
        spark_max("date").alias("last_purchase_date")
    )
)

# Service summary per customer
df_service_summary = (df_wo
    .groupBy("customer_id")
    .agg(
        count("workorder_id").alias("total_service_calls"),
        spark_sum(when(col("sla_breach") == True, 1).otherwise(0))
            .alias("sla_breaches_experienced"),
        spark_round(avg("resolution_days"), 1)
            .alias("avg_resolution_days")
    )
)

# Join everything together
df_gold = (df_customers
    .join(df_sales_summary,   on="customer_id", how="left")
    .join(df_service_summary, on="customer_id", how="left")
    .withColumn("total_revenue_inr",
                F.coalesce(col("total_revenue_inr"), lit(0)))
    .withColumn("total_purchases",
                F.coalesce(col("total_purchases"),   lit(0)))
    .withColumn("total_service_calls",
                F.coalesce(col("total_service_calls"), lit(0)))
    .withColumn("revenue_in_lakhs",
                spark_round(col("total_revenue_inr") / 100000, 2))
    .withColumn("customer_health",
                when(
                    (col("sla_breaches_experienced") > 3) |
                    (col("total_purchases") == 0),
                    lit("AT RISK"))
                .when(
                    col("total_revenue_inr") > 5000000,
                    lit("STRATEGIC"))
                .otherwise(lit("HEALTHY")))
    .withColumn("_gold_computed_at", current_timestamp())
    .orderBy(col("total_revenue_inr").desc())
)

(df_gold.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{GOLD_DB}.gold_customer_portfolio"))

print(f"  Rows written: {df_gold.count():,}")
print("  gold_customer_portfolio ✓\n")

# Customer health distribution
print("  Customer health distribution:\n")
spark.table(f"{GOLD_DB}.gold_customer_portfolio") \
    .groupBy("customer_health") \
    .count() \
    .orderBy("customer_health") \
    .show()

# COMMAND ----------

# ── GOLD 5: EXECUTIVE KPI SUMMARY ────────────────────────────
# Business question: Give me the headline numbers — one table
# that tells me how the entire business is performing.
#
# Who uses this: CEO, CFO, COO — this is the board dashboard
# GMMCO impact: Replaces the weekly manually compiled PDF report

print("Building: gold_executive_kpis\n")

df_sales     = spark.table(f"{SILVER_DB}.silver_equipment_sales")
df_parts     = spark.table(f"{SILVER_DB}.silver_parts_orders")
df_wo        = spark.table(f"{SILVER_DB}.silver_service_workorders")
df_customers = spark.table(f"{SILVER_DB}.silver_customers")

# Collect all KPIs into a single summary row
kpis = {
    "total_equipment_revenue_inr":
        df_sales.agg(spark_sum("sale_amount_inr")).collect()[0][0],

    "total_deals_closed":
        df_sales.count(),

    "avg_deal_value_inr":
        round(df_sales.agg(avg("sale_amount_inr")).collect()[0][0], 0),

    "total_parts_spend_inr":
        df_parts.agg(spark_sum("total_value_inr")).collect()[0][0],

    "total_service_calls":
        df_wo.count(),

    "overall_sla_compliance_pct":
        round(df_wo.filter(col("sla_breach") == False).count()
              / df_wo.count() * 100, 1),

    "critical_sla_breaches":
        df_wo.filter(
            (col("sla_breach") == True) &
            (col("overdue_hours") > 24)).count(),

    "total_active_customers":
        df_customers.count(),

    "enterprise_customers":
        df_customers.filter(
            col("customer_tier") == "Enterprise").count(),

    "at_risk_customers":
        spark.table(f"{GOLD_DB}.gold_customer_portfolio")
        .filter(col("customer_health") == "AT RISK").count(),

    "total_revenue_in_lakhs":
        round(df_sales.agg(
            spark_sum("sale_amount_inr")).collect()[0][0] / 100000, 2),
}

# Print the executive dashboard
print("  ╔══════════════════════════════════════════════════╗")
print("  ║       GMMCO EXECUTIVE KPI DASHBOARD             ║")
print("  ╠══════════════════════════════════════════════════╣")
print(f"  ║  Total Revenue          ₹{kpis['total_revenue_in_lakhs']:>10.2f} Lakhs      ║")
print(f"  ║  Total Deals Closed     {kpis['total_deals_closed']:>15,}      ║")
print(f"  ║  Avg Deal Value         ₹{kpis['avg_deal_value_inr']:>14,.0f}      ║")
print(f"  ║  Total Parts Spend      ₹{kpis['total_parts_spend_inr']/100000:>10.2f} Lakhs      ║")
print(f"  ║  Service Calls          {kpis['total_service_calls']:>15,}      ║")
print(f"  ║  SLA Compliance         {kpis['overall_sla_compliance_pct']:>14.1f}%      ║")
print(f"  ║  Critical SLA Breaches  {kpis['critical_sla_breaches']:>15,}      ║")
print(f"  ║  Active Customers       {kpis['total_active_customers']:>15,}      ║")
print(f"  ║  Enterprise Customers   {kpis['enterprise_customers']:>15,}      ║")
print(f"  ║  At-Risk Customers      {kpis['at_risk_customers']:>15,}      ║")
print("  ╚══════════════════════════════════════════════════╝")

# Save KPI summary as a Delta table
kpi_rows = [(k, str(round(v, 2))) for k, v in kpis.items()]
df_kpis = spark.createDataFrame(kpi_rows, ["kpi_name", "kpi_value"]) \
    .withColumn("_gold_computed_at", current_timestamp())

(df_kpis.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{GOLD_DB}.gold_executive_kpis"))

print(f"\n  gold_executive_kpis ✓")

# COMMAND ----------

# ── GOLD SUMMARY ──────────────────────────────────────────────
print("=" * 55)
print("GOLD LAYER COMPLETE — GMMCO Data Platform")
print("=" * 55)

gold_tables = [
    "gold_sales_performance",
    "gold_sla_compliance",
    "gold_parts_analysis",
    "gold_customer_portfolio",
    "gold_executive_kpis"
]

for t in gold_tables:
    count = spark.table(f"{GOLD_DB}.{t}").count()
    print(f"  {t:<38} {count:>6,} rows")

print("=" * 55)
print("\nAll Gold tables registered in Unity Catalog.")
print("Ready for SQL Warehouse connection and dashboards.")