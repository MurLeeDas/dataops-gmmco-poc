"""
GMMCO DataOps POC — Schema Validation Gate
Checks incoming CSVs against expected schemas.
Runs BEFORE data quality checks in the pipeline.
Blocks pipeline if any column is added, removed, or renamed.
"""

import pandas as pd
import sys
from datetime import datetime, timezone

# ── Expected schemas — locked at project start ─────────────
# These are your contracts with the source system.
# Any deviation is caught here before it reaches Bronze.

EXPECTED_SCHEMAS = {
    "sample_data/equipment_sales.csv": {
        "required_columns": [
            "sale_id", "date", "branch", "region",
            "customer_id", "customer_name", "industry_segment",
            "product_category", "model_code", "serial_number",
            "sale_amount_inr", "finance_type", "salesperson_id"
        ],
        "expected_dtypes": {
            "sale_amount_inr": "numeric",
            "date":            "string"
        }
    },
    "sample_data/parts_orders.csv": {
        "required_columns": [
            "order_id", "date", "part_number", "part_description",
            "quantity_ordered", "unit_price_inr", "total_value_inr",
            "warehouse_location", "customer_id", "machine_serial",
            "urgency_level"
        ],
        "expected_dtypes": {
            "quantity_ordered": "numeric",
            "unit_price_inr":   "numeric"
        }
    },
    "sample_data/service_workorders.csv": {
        "required_columns": [
            "workorder_id", "date_raised", "date_resolved",
            "machine_serial", "fault_category", "fault_code",
            "technician_id", "labour_hours", "parts_cost_inr",
            "sla_hours_promised", "sla_met", "branch",
            "region", "customer_id"
        ],
        "expected_dtypes": {
            "labour_hours":      "numeric",
            "sla_hours_promised":"numeric"
        }
    },
    "sample_data/customers.csv": {
        "required_columns": [
            "customer_id", "customer_name", "industry_segment",
            "region", "state", "fleet_size",
            "contract_type", "contract_value_inr", "account_manager"
        ],
        "expected_dtypes": {
            "fleet_size":          "numeric",
            "contract_value_inr":  "numeric"
        }
    }
}


def validate_schema(csv_path: str, schema: dict) -> tuple:
    """
    Validates a CSV file against its expected schema.
    Returns (passed: bool, issues: list of strings)
    """
    issues = []

    try:
        df = pd.read_csv(csv_path)
    except FileNotFoundError:
        return False, [f"FILE NOT FOUND: {csv_path}"]

    actual_cols   = set(df.columns.tolist())
    expected_cols = set(schema["required_columns"])

    # Check 1: Missing columns — most critical
    missing = expected_cols - actual_cols
    if missing:
        issues.append(
            f"MISSING COLUMNS: {sorted(missing)} — "
            f"Source system may have removed or renamed these"
        )

    # Check 2: Unexpected new columns
    unexpected = actual_cols - expected_cols
    if unexpected:
        issues.append(
            f"NEW COLUMNS DETECTED: {sorted(unexpected)} — "
            f"Schema expanded without notification. Review before ingesting."
        )

    # Check 3: Data type validation
    for col, expected_type in schema.get("expected_dtypes", {}).items():
        if col not in df.columns:
            continue
        if expected_type == "numeric":
            non_numeric = pd.to_numeric(
                df[col], errors="coerce").isna().sum()
            if non_numeric > 0:
                issues.append(
                    f"TYPE MISMATCH: '{col}' expected numeric "
                    f"but has {non_numeric} non-numeric values"
                )

    # Check 4: Empty file
    if len(df) == 0:
        issues.append("EMPTY FILE: No rows in dataset")

    passed = len(issues) == 0
    return passed, issues


# ── Main ───────────────────────────────────────────────────
if __name__ == "__main__":

    print("=" * 60)
    print("GMMCO SCHEMA VALIDATION GATE")
    print(f"Started: {datetime.now(timezone.utc).isoformat()} UTC")
    print("=" * 60)

    all_passed    = True
    results       = []

    for csv_path, schema in EXPECTED_SCHEMAS.items():
        passed, issues = validate_schema(csv_path, schema)
        results.append((csv_path, passed, issues))

        dataset = csv_path.split("/")[-1]
        status  = "✅ PASSED" if passed else "❌ FAILED"
        print(f"\n  {dataset}")
        print(f"  {status}")

        if issues:
            all_passed = False
            for issue in issues:
                print(f"    → {issue}")

    print("\n" + "=" * 60)
    print("SCHEMA VALIDATION SUMMARY")
    print("=" * 60)
    print(f"  {'Dataset':<35} {'Status':>10}")
    print(f"  {'─'*47}")
    for csv_path, passed, _ in results:
        dataset = csv_path.split("/")[-1]
        status  = "✅ PASSED" if passed else "❌ FAILED"
        print(f"  {dataset:<35} {status:>10}")
    print(f"  {'─'*47}")

    if all_passed:
        print("\n✅ ALL SCHEMA CHECKS PASSED")
        print("   Structure matches expected contract.")
        print("   Proceed to data quality validation.\n")
        sys.exit(0)
    else:
        print("\n❌ SCHEMA VALIDATION FAILED")
        print("   Source system schema has changed.")
        print("   Do NOT ingest until schema contract is reviewed.\n")
        sys.exit(1)