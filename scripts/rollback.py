"""
GMMCO DataOps POC — Delta Lake Rollback Script
================================================
Rolls back any Gold, Silver, or Bronze table to a
previous version using Delta Lake Time Travel.

Usage:
  python scripts/rollback.py --layer gold --steps 1
  python scripts/rollback.py --layer silver --steps 2
  python scripts/rollback.py --layer all --steps 1
  python scripts/rollback.py --list gold
  
"""

import argparse
import requests
import json
import sys
import time
from datetime import datetime, timezone


# ── Configuration ──────────────────────────────────────────────
DATABRICKS_HOST  = "https://dbc-8e912ed6-1406.cloud.databricks.com"
CATALOG          = "workspace"
SCHEMA           = "gmmco_poc"

# Tables per layer — order matters for rollback
LAYER_TABLES = {
    "bronze": [
        "bronze_equipment_sales",
        "bronze_parts_orders",
        "bronze_service_workorders",
        "bronze_customers"
    ],
    "silver": [
        "silver_equipment_sales",
        "silver_parts_orders",
        "silver_service_workorders",
        "silver_customers"
    ],
    "gold": [
        "gold_sales_performance",
        "gold_sla_compliance",
        "gold_parts_analysis",
        "gold_customer_portfolio",
        "gold_executive_kpis"
    ]
}


# ── Databricks SQL Statement API ───────────────────────────────
def run_sql(token: str, sql: str,
            warehouse_id: str, timeout: int = 50) -> dict:
    """
    Executes SQL on Databricks SQL Warehouse via REST API.
    Returns the result as a dict.
    """
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }

    # Submit statement
    response = requests.post(
        f"{DATABRICKS_HOST}/api/2.0/sql/statements",
        headers=headers,
        json={
            "statement":    sql,
            "warehouse_id": warehouse_id,
            "wait_timeout": "50s",
            "on_wait_timeout": "CONTINUE",
            "catalog":      "workspace",     
            "schema":       "gmmco_poc"       
        }
    )
    
    response.raise_for_status()
    result = response.json()
    statement_id = result.get("statement_id")

    # Poll until complete
    for _ in range(30):
        poll = requests.get(
            f"{DATABRICKS_HOST}/api/2.0/sql/statements/{statement_id}",
            headers=headers
        )
        poll.raise_for_status()
        data   = poll.json()
        status = data.get("status", {}).get("state")

        if status == "SUCCEEDED":
            return data
        elif status in ("FAILED", "CANCELLED", "CLOSED"):
            error = data.get("status", {}).get("error", {})
            raise Exception(f"SQL failed: {error.get('message', 'Unknown error')}")

        time.sleep(3)

    raise Exception("SQL statement timed out")


def get_table_history(token: str, warehouse_id: str,
                      table: str) -> list:
    """Returns version history for a Delta table."""
    full_name = f"{CATALOG}.{SCHEMA}.{table}"
    result = run_sql(
        token,
        f"DESCRIBE HISTORY {full_name} LIMIT 10",
        warehouse_id
    )

    rows    = result.get("result", {}).get("data_array", [])
    columns = [
        col["name"]
        for col in result.get("manifest", {})
                          .get("schema", {})
                          .get("columns", [])
    ]

    history = []
    for row in rows:
        record = dict(zip(columns, row))
        history.append({
            "version":   record.get("version"),
            "timestamp": record.get("timestamp"),
            "operation": record.get("operation"),
            "user":      record.get("userName", "system")
        })

    return history


def rollback_table(token: str, warehouse_id: str,
                   table: str, target_version: int) -> bool:
    """
    Rolls back a Delta table to a specific version.
    Uses RESTORE TABLE — official Delta Lake command.
    """
    full_name = f"{CATALOG}.{SCHEMA}.{table}"
    print(f"\n    Rolling back {table} → version {target_version}")

    try:
        run_sql(
            token,
            f"RESTORE TABLE {full_name} TO VERSION AS OF {target_version}",
            warehouse_id,
            timeout=50
        )

        # Verify the restore worked
        result = run_sql(
            token,
            f"DESCRIBE HISTORY {full_name} LIMIT 1",
            warehouse_id
        )
        rows = result.get("result", {}).get("data_array", [])
        if rows:
            print(f"    ✅ Restored successfully")
            return True
        return False

    except Exception as e:
        print(f"    ❌ Rollback failed: {e}")
        return False


def get_warehouse_id(token: str) -> str:
    """Gets the first available SQL Warehouse ID."""
    headers = {"Authorization": f"Bearer {token}"}
    response = requests.get(
        f"{DATABRICKS_HOST}/api/2.0/sql/warehouses",
        headers=headers
    )
    response.raise_for_status()
    warehouses = response.json().get("warehouses", [])
    if not warehouses:
        raise Exception("No SQL warehouses found")
    return warehouses[0]["id"]


# ── CLI Interface ──────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser(
        description="GMMCO DataOps — Delta Lake Rollback Tool",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  List Gold table versions:
    python scripts/rollback.py --list gold --token YOUR_TOKEN

  Roll back Gold by 1 version (undo last pipeline run):
    python scripts/rollback.py --layer gold --steps 1 --token YOUR_TOKEN

  Roll back Silver by 2 versions:
    python scripts/rollback.py --layer silver --steps 2 --token YOUR_TOKEN

  Roll back everything by 1 version:
    python scripts/rollback.py --layer all --steps 1 --token YOUR_TOKEN
        """
    )

    parser.add_argument("--token",  required=True,
                        help="Databricks personal access token")
    parser.add_argument("--layer",
                        choices=["bronze", "silver", "gold", "all"],
                        help="Which layer to roll back")
    parser.add_argument("--steps", type=int, default=1,
                        help="How many versions to roll back (default: 1)")
    parser.add_argument("--list",
                        choices=["bronze", "silver", "gold"],
                        help="List version history for a layer")

    args = parser.parse_args()

    print("=" * 60)
    print("GMMCO DATAOPS — DELTA LAKE ROLLBACK TOOL")
    print(f"Started: {datetime.now(timezone.utc).isoformat()} UTC")
    print("=" * 60)

    token = args.token

    try:
        warehouse_id = get_warehouse_id(token)
        print(f"\nConnected to SQL Warehouse: {warehouse_id}")
    except Exception as e:
        print(f"❌ Cannot connect to Databricks: {e}")
        sys.exit(1)

    # ── LIST mode ──────────────────────────────────────────────
    if args.list:
        layer  = args.list
        tables = LAYER_TABLES[layer]
        print(f"\nVersion history — {layer.upper()} layer:\n")

        for table in tables:
            print(f"  Table: {table}")
            print(f"  {'─'*50}")
            try:
                history = get_table_history(token, warehouse_id, table)
                for h in history:
                    print(f"    v{h['version']}  "
                          f"{h['timestamp']}  "
                          f"{h['operation']}")
            except Exception as e:
                print(f"    ❌ Error: {e}")
            print()
        return

    # ── ROLLBACK mode ──────────────────────────────────────────
    if not args.layer:
        parser.print_help()
        sys.exit(1)

    layers = (["bronze", "silver", "gold"]
              if args.layer == "all"
              else [args.layer])

    print(f"\nRollback plan:")
    print(f"  Layer(s) : {', '.join(layers).upper()}")
    print(f"  Steps    : {args.steps} version(s) back")
    print(f"\n  ⚠️  This will restore tables to a previous state.")
    print(f"      Data written after that version will be removed")
    print(f"      from the active table (but remains in Delta log).")
    print(f"\nProceed? (yes/no): ", end="")

    confirm = input().strip().lower()
    if confirm != "yes":
        print("Rollback cancelled.")
        sys.exit(0)

    # Execute rollback for each layer and table
    total_success = 0
    total_failed  = 0

    for layer in layers:
        print(f"\n{'─'*60}")
        print(f"  Rolling back: {layer.upper()} layer")
        print(f"{'─'*60}")

        tables = LAYER_TABLES[layer]

        for table in tables:
            # Get current version first
            try:
                history = get_table_history(token, warehouse_id, table)
                if not history:
                    print(f"    ⚠️  {table}: No history found")
                    continue

                current_version = int(history[0]["version"])
                target_version  = max(0, current_version - args.steps)

                print(f"\n    {table}")
                print(f"      Current version : {current_version}")
                print(f"      Target version  : {target_version}")

                if current_version == 0:
                    print(f"      ⚠️  Already at version 0 — cannot roll back further")
                    continue

                success = rollback_table(
                    token, warehouse_id, table, target_version)

                if success:
                    total_success += 1
                else:
                    total_failed += 1

            except Exception as e:
                print(f"    ❌ Error with {table}: {e}")
                total_failed += 1

    # Final summary
    print(f"\n{'='*60}")
    print("ROLLBACK COMPLETE")
    print(f"{'='*60}")
    print(f"  Succeeded : {total_success} tables")
    print(f"  Failed    : {total_failed} tables")

    if total_failed == 0:
        print(f"\n✅ All tables rolled back successfully")
        print(f"   Dashboard will reflect previous state on next refresh")
    else:
        print(f"\n⚠️  Some tables failed — check errors above")
        sys.exit(1)


if __name__ == "__main__":
    main()