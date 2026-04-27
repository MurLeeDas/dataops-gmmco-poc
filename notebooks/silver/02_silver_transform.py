# Databricks notebook source
# ── CONFIGURATION ─────────────────────────────────────────────
CATALOG     = "workspace"
SCHEMA      = "gmmco_poc"
BRONZE_DB   = f"{CATALOG}.{SCHEMA}"
SILVER_DB   = f"{CATALOG}.{SCHEMA}"

# Silver table names (input → output mapping)
SILVER_TABLES = {
    "equipment_sales":    ("bronze_equipment_sales",    "silver_equipment_sales"),
    "parts_orders":       ("bronze_parts_orders",       "silver_parts_orders"),
    "service_workorders": ("bronze_service_workorders", "silver_service_workorders"),
    "customers":          ("bronze_customers",          "silver_customers"),
}

print("Silver transformation configuration loaded.")
print(f"Reading from : {BRONZE_DB}")
print(f"Writing to   : {SILVER_DB}")

# COMMAND ----------

# ── IMPORTS ───────────────────────────────────────────────────
from pyspark.sql import functions as F
from pyspark.sql.functions import (
    col, trim, upper, lower, to_date, when, lit,
    current_timestamp, datediff, abs as spark_abs,
    regexp_replace, coalesce, round as spark_round
)
from datetime import datetime

print("Libraries loaded.")

# COMMAND ----------

# ── SILVER: EQUIPMENT SALES ───────────────────────────────────
# Business rules applied:
#   1. Reject records with null sale_id, customer_id, or sale_amount
#   2. Reject records where sale_amount <= 0
#   3. Standardise date format to yyyy-MM-dd
#   4. Standardise region and branch to Title Case
#   5. Add sale_year and sale_month for easy aggregation in Gold

print("Processing: equipment_sales")
df = spark.table(f"{BRONZE_DB}.bronze_equipment_sales")
print(f"  Bronze rows: {df.count():,}")

# Step 1: Flag bad records with a rejection reason
df_flagged = df.withColumn("_rejection_reason",
    when(col("sale_id").isNull(),          lit("NULL sale_id"))
    .when(col("customer_id").isNull(),     lit("NULL customer_id"))
    .when(col("sale_amount_inr").isNull(), lit("NULL sale_amount"))
    .when(col("sale_amount_inr") <= 0,    lit("Invalid sale_amount <= 0"))
    .when(col("date").isNull(),            lit("NULL date"))
    .otherwise(lit(None))
)

# Step 2: Separate good and bad records
df_good     = df_flagged.filter(col("_rejection_reason").isNull())
df_rejected = df_flagged.filter(col("_rejection_reason").isNotNull())

print(f"  Good rows  : {df_good.count():,}")
print(f"  Rejected   : {df_rejected.count():,}")

# Step 3: Apply transformations to good records
df_silver = (df_good
    .withColumn("date",
                to_date(col("date"), "yyyy-MM-dd"))
    .withColumn("region",
                F.initcap(trim(col("region"))))
    .withColumn("branch",
                F.initcap(trim(col("branch"))))
    .withColumn("industry_segment",
                trim(col("industry_segment")))
    .withColumn("finance_type",
                trim(col("finance_type")))
    .withColumn("sale_year",
                F.year(col("date")))
    .withColumn("sale_month",
                F.month(col("date")))
    .withColumn("sale_quarter",
                F.quarter(col("date")))
    .withColumn("_silver_processed_at",
                current_timestamp())
    .drop("_rejection_reason")
)

# Step 4: Write silver table
(df_silver.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{SILVER_DB}.silver_equipment_sales"))

# Step 5: Write rejected records for investigation
if df_rejected.count() > 0:
    (df_rejected.write
        .format("delta")
        .mode("overwrite")
        .saveAsTable(f"{SILVER_DB}.silver_equipment_sales_rejected"))
    print(f"  Rejected table written.")

print(f"  Silver rows written: {df_silver.count():,}")
print("  equipment_sales ✓\n")

# COMMAND ----------

# ── SILVER: PARTS ORDERS ──────────────────────────────────────
# Business rules:
#   1. Reject nulls on order_id, part_number, quantity_ordered
#   2. Reject quantity <= 0 or unit_price <= 0
#   3. Standardise urgency_level to uppercase
#   4. Add total_value_inr calculated field (quantity × unit_price)
#   5. Add urgency_rank (1=Critical, 2=Urgent, 3=Standard)

print("Processing: parts_orders")
df = spark.table(f"{BRONZE_DB}.bronze_parts_orders")
print(f"  Bronze rows: {df.count():,}")

df_flagged = df.withColumn("_rejection_reason",
    when(col("order_id").isNull(),           lit("NULL order_id"))
    .when(col("part_number").isNull(),       lit("NULL part_number"))
    .when(col("quantity_ordered").isNull(),  lit("NULL quantity"))
    .when(col("quantity_ordered") <= 0,      lit("Invalid quantity <= 0"))
    .when(col("unit_price_inr").isNull(),    lit("NULL unit_price"))
    .when(col("unit_price_inr") <= 0,        lit("Invalid unit_price <= 0"))
    .otherwise(lit(None))
)

df_good     = df_flagged.filter(col("_rejection_reason").isNull())
df_rejected = df_flagged.filter(col("_rejection_reason").isNotNull())

print(f"  Good rows  : {df_good.count():,}")
print(f"  Rejected   : {df_rejected.count():,}")

df_silver = (df_good
    .withColumn("date",
                to_date(col("date"), "yyyy-MM-dd"))
    .withColumn("urgency_level",
                upper(trim(col("urgency_level"))))
    .withColumn("urgency_rank",
                when(col("urgency_level") == "CRITICAL", lit(1))
                .when(col("urgency_level") == "URGENT",   lit(2))
                .otherwise(lit(3)))
    .withColumn("total_value_inr",
                spark_round(
                    col("quantity_ordered") * col("unit_price_inr"), 2))
    .withColumn("_silver_processed_at", current_timestamp())
    .drop("_rejection_reason")
)

(df_silver.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{SILVER_DB}.silver_parts_orders"))

print(f"  Silver rows written: {df_silver.count():,}")
print("  parts_orders ✓\n")

# COMMAND ----------

# ── SILVER: SERVICE WORKORDERS ────────────────────────────────
# Business rules:
#   1. Reject nulls on workorder_id, machine_serial
#   2. Standardise fault_category to Title Case
#   3. Add resolution_days (date_resolved - date_raised)
#   4. Add sla_breach flag (boolean — easier for BI than Yes/No string)
#   5. Add overdue_hours (how many hours past SLA was resolved)

print("Processing: service_workorders")
df = spark.table(f"{BRONZE_DB}.bronze_service_workorders")
print(f"  Bronze rows: {df.count():,}")

df_flagged = df.withColumn("_rejection_reason",
    when(col("workorder_id").isNull(),    lit("NULL workorder_id"))
    .when(col("machine_serial").isNull(), lit("NULL machine_serial"))
    .when(col("date_raised").isNull(),    lit("NULL date_raised"))
    .otherwise(lit(None))
)

df_good     = df_flagged.filter(col("_rejection_reason").isNull())
df_rejected = df_flagged.filter(col("_rejection_reason").isNotNull())

print(f"  Good rows  : {df_good.count():,}")
print(f"  Rejected   : {df_rejected.count():,}")

df_silver = (df_good
    .withColumn("date_raised",
                to_date(col("date_raised"),   "yyyy-MM-dd"))
    .withColumn("date_resolved",
                to_date(col("date_resolved"), "yyyy-MM-dd"))
    .withColumn("fault_category",
                F.initcap(trim(col("fault_category"))))
    .withColumn("branch",
                F.initcap(trim(col("branch"))))
    .withColumn("region",
                F.initcap(trim(col("region"))))
    .withColumn("resolution_days",
                datediff(col("date_resolved"), col("date_raised")))
    .withColumn("sla_breach",
                when(col("sla_met") == "No", lit(True))
                .otherwise(lit(False)))
    .withColumn("overdue_hours",
                when(col("sla_breach") == True,
                     spark_round(
                         col("labour_hours") - col("sla_hours_promised"), 1))
                .otherwise(lit(0.0)))
    .withColumn("_silver_processed_at", current_timestamp())
    .drop("_rejection_reason")
)

(df_silver.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{SILVER_DB}.silver_service_workorders"))

print(f"  Silver rows written: {df_silver.count():,}")
print("  service_workorders ✓\n")

# COMMAND ----------

# ── SILVER: CUSTOMERS ─────────────────────────────────────────
# Business rules:
#   1. Reject nulls on customer_id
#   2. Standardise industry_segment and region to Title Case
#   3. Standardise state names to Title Case
#   4. Add customer_tier based on fleet_size
#      (Enterprise: >50, Mid-Market: 10-50, SME: <10)
#   5. Add has_contract boolean flag

print("Processing: customers")
df = spark.table(f"{BRONZE_DB}.bronze_customers")
print(f"  Bronze rows: {df.count():,}")

df_flagged = df.withColumn("_rejection_reason",
    when(col("customer_id").isNull(),   lit("NULL customer_id"))
    .when(col("fleet_size").isNull(),   lit("NULL fleet_size"))
    .when(col("fleet_size") < 0,        lit("Invalid fleet_size < 0"))
    .otherwise(lit(None))
)

df_good     = df_flagged.filter(col("_rejection_reason").isNull())
df_rejected = df_flagged.filter(col("_rejection_reason").isNotNull())

print(f"  Good rows  : {df_good.count():,}")
print(f"  Rejected   : {df_rejected.count():,}")

df_silver = (df_good
    .withColumn("industry_segment",
                F.initcap(trim(col("industry_segment"))))
    .withColumn("region",
                F.initcap(trim(col("region"))))
    .withColumn("state",
                F.initcap(trim(col("state"))))
    .withColumn("customer_tier",
                when(col("fleet_size") > 50,  lit("Enterprise"))
                .when(col("fleet_size") >= 10, lit("Mid-Market"))
                .otherwise(lit("SME")))
    .withColumn("has_contract",
                when(col("contract_type") == "None", lit(False))
                .otherwise(lit(True)))
    .withColumn("_silver_processed_at", current_timestamp())
    .drop("_rejection_reason")
)

(df_silver.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{SILVER_DB}.silver_customers"))

print(f"  Silver rows written: {df_silver.count():,}")
print("  customers ✓\n")

# COMMAND ----------

# ── SILVER SUMMARY REPORT ─────────────────────────────────────
print("=" * 55)
print("SILVER TRANSFORMATION COMPLETE — GMMCO Data Platform")
print("=" * 55)

silver_tables = [
    "silver_equipment_sales",
    "silver_parts_orders",
    "silver_service_workorders",
    "silver_customers"
]

total = 0
for t in silver_tables:
    count = spark.table(f"{SILVER_DB}.{t}").count()
    total += count
    print(f"  {t:<35} {count:>8,}")

print("-" * 55)
print(f"  {'TOTAL':<35} {total:>8,}")
print("=" * 55)

# COMMAND ----------

# ── SPOT CHECK ────────────────────────────────────────────────
# Verify that the enriched columns we added are correct.

print("Spot check — silver_equipment_sales (new columns):\n")
spark.table(f"{SILVER_DB}.silver_equipment_sales") \
    .select("sale_id", "date", "region", "sale_amount_inr",
            "sale_year", "sale_month", "sale_quarter",
            "_silver_processed_at") \
    .show(5, truncate=False)

print("\nSpot check — silver_service_workorders (SLA columns):\n")
spark.table(f"{SILVER_DB}.silver_service_workorders") \
    .select("workorder_id", "date_raised", "date_resolved",
            "sla_hours_promised", "labour_hours",
            "resolution_days", "sla_breach", "overdue_hours") \
    .show(5, truncate=False)

print("\nCustomer tiers distribution:\n")
spark.table(f"{SILVER_DB}.silver_customers") \
    .groupBy("customer_tier") \
    .count() \
    .orderBy("customer_tier") \
    .show()

print("\nSLA breach rate:\n")
total_wo = spark.table(f"{SILVER_DB}.silver_service_workorders").count()
breached = spark.table(f"{SILVER_DB}.silver_service_workorders") \
    .filter(col("sla_breach") == True).count()
breach_rate = round(breached / total_wo * 100, 1)
print(f"  Total workorders : {total_wo:,}")
print(f"  SLA breached     : {breached:,}")
print(f"  Breach rate      : {breach_rate}%")
print("\n  (This is the headline metric GMMCO's service director cares about most)")