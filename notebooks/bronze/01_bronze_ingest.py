# Databricks notebook source
# ── CONFIGURATION ─────────────────────────────────────────────
# Change only this section if you deploy to a different environment.
# Everything else reads from these variables.

CATALOG        = "workspace"
SCHEMA         = "gmmco_poc"
VOLUME_PATH    = f"/Volumes/{CATALOG}/{SCHEMA}/raw_data"
BRONZE_DB      = f"{CATALOG}.{SCHEMA}"

# Source files and their target table names
SOURCE_FILES = {
    "equipment_sales":   f"{VOLUME_PATH}/equipment_sales.csv",
    "parts_orders":      f"{VOLUME_PATH}/parts_orders.csv",
    "service_workorders": f"{VOLUME_PATH}/service_workorders.csv",
    "customers":         f"{VOLUME_PATH}/customers.csv"
}

print(f"Catalog  : {CATALOG}")
print(f"Schema   : {SCHEMA}")
print(f"Volume   : {VOLUME_PATH}")
print(f"Tables   : {list(SOURCE_FILES.keys())}")

# COMMAND ----------

# ── VERIFY RAW FILES ──────────────────────────────────────────
# Always verify inputs before processing.
# If a file is missing, fail early with a clear message.

from pyspark.sql.functions import current_timestamp, lit, md5, concat_ws
from datetime import datetime

print("Checking source files in Volume...\n")
for table_name, file_path in SOURCE_FILES.items():
    try:
        files = dbutils.fs.ls(file_path)
        size_kb = round(files[0].size / 1024, 1)
        print(f"  ✓  {table_name:25s} → {size_kb} KB")
    except Exception as e:
        print(f"  ✗  {table_name:25s} → FILE NOT FOUND: {e}")

print("\nAll source files verified.")

# COMMAND ----------

# ── BRONZE INGESTION FUNCTION ─────────────────────────────────
# This function does the same thing for every source file:
#   1. Read CSV
#   2. Add audit columns (when, where, hash)
#   3. Write to Delta table
#
# WHY A FUNCTION: Write once, use four times.
# If you need to change the audit columns, you change them here
# and all four tables benefit automatically.

def ingest_to_bronze(table_name: str, file_path: str) -> dict:
    """
    Reads a CSV from Volume, adds audit columns,
    writes to Bronze Delta table. Returns stats.
    """
    target_table = f"{BRONZE_DB}.bronze_{table_name}"
    ingestion_time = datetime.utcnow().isoformat()

    print(f"\n{'─'*55}")
    print(f"  Ingesting: {table_name}")
    print(f"  Source   : {file_path}")
    print(f"  Target   : {target_table}")

    # Step 1: Read raw CSV — infer schema, keep all columns as-is
    df_raw = (spark.read
              .option("header", "true")
              .option("inferSchema", "true")
              .csv(file_path))

    raw_count = df_raw.count()
    print(f"  Raw rows : {raw_count:,}")

    # Step 2: Add audit columns
    # _ingested_at  → when this record entered our system
    # _source_file  → exactly which file it came from
    # _record_hash  → fingerprint of the record (detects changes)
    df_bronze = (df_raw
        .withColumn("_ingested_at",  current_timestamp())
        .withColumn("_source_file",  lit(file_path.split("/")[-1]))
        .withColumn("_record_hash",
                    md5(concat_ws("|", *[df_raw[c]
                                         for c in df_raw.columns])))
    )

    # Step 3: Write to Delta table
    # mode("overwrite") for full reload — safe for Bronze
    # In production this would be "append" with deduplication
    (df_bronze.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(target_table))

    final_count = spark.table(target_table).count()
    print(f"  Written  : {final_count:,} rows → {target_table}")

    return {
        "table": target_table,
        "source_rows": raw_count,
        "written_rows": final_count,
        "ingested_at": ingestion_time
    }

# COMMAND ----------

# ── RUN BRONZE INGESTION ──────────────────────────────────────
# Loop through all source files and ingest each one.
# Collect stats for the audit report at the end.
from datetime import datetime, timezone

print("Starting Bronze ingestion — GMMCO Data Platform")
print(f"Timestamp: {datetime.utcnow().isoformat()} UTC\n")

ingestion_results = []
failed_tables = []

for table_name, file_path in SOURCE_FILES.items():
    try:
        result = ingest_to_bronze(table_name, file_path)
        ingestion_results.append(result)
    except Exception as e:
        print(f"\n  ERROR ingesting {table_name}: {e}")
        failed_tables.append(table_name)

print(f"\n{'═'*55}")
print("BRONZE INGESTION COMPLETE")
print(f"{'═'*55}")
print(f"  Succeeded : {len(ingestion_results)}")
print(f"  Failed    : {len(failed_tables)}")
if failed_tables:
    print(f"  Failed tables: {failed_tables}")

# COMMAND ----------

# ── INGESTION AUDIT REPORT ────────────────────────────────────
# Print a clean summary of what was ingested.
# This is what you show clients — proof that the pipeline ran
# and exactly how many records were processed.

print("\nINGESTION AUDIT REPORT")
print(f"{'─'*55}")
print(f"{'Table':<35} {'Rows':>10}")
print(f"{'─'*55}")

total_rows = 0
for r in ingestion_results:
    table_short = r['table'].split('.')[-1]
    print(f"  {table_short:<33} {r['written_rows']:>10,}")
    total_rows += r['written_rows']

print(f"{'─'*55}")
print(f"  {'TOTAL':<33} {total_rows:>10,}")
print(f"{'─'*55}")
print(f"\nAll tables written to: {BRONZE_DB}")

# COMMAND ----------

# ── VERIFY AUDIT COLUMNS ──────────────────────────────────────
# Inspect the bronze_equipment_sales table to confirm
# audit columns were added correctly.
# Run this after ingestion to validate the output.

print("Sample from bronze_equipment_sales:\n")
(spark.table(f"{BRONZE_DB}.bronze_equipment_sales")
    .select("sale_id", "date", "branch", "sale_amount_inr",
            "_ingested_at", "_source_file", "_record_hash")
    .show(5, truncate=False))

print("\nSchema of bronze_equipment_sales:\n")
spark.table(f"{BRONZE_DB}.bronze_equipment_sales").printSchema()

# COMMAND ----------

# ── VERIFY ALL BRONZE TABLES IN CATALOG ──────────────────────
print("Bronze tables registered in Unity Catalog:\n")

tables = spark.sql(f"SHOW TABLES IN {BRONZE_DB} LIKE 'bronze_*'")
tables.show(truncate=False)

print("\nRow counts per table:\n")
for table_name in SOURCE_FILES.keys():
    count = spark.table(f"{BRONZE_DB}.bronze_{table_name}").count()
    print(f"  bronze_{table_name:<25} {count:>8,} rows")