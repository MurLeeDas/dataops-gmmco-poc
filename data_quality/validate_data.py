"""
GMMCO DataOps POC — Data Quality Validation
Pure Python + Pandas approach — no GX context complexity.
Runs all quality checks against incoming CSV files.
Called by GitHub Actions before Bronze ingestion.
Pipeline stops with exit code 1 if ANY check fails.
"""

import pandas as pd
import sys
from datetime import datetime


# ══════════════════════════════════════════════════════════════
# VALIDATION ENGINE
# ══════════════════════════════════════════════════════════════

class DataQualityValidator:
    """
    Runs a list of checks against a DataFrame.
    Records pass/fail for each check.
    Returns overall pass/fail status.
    """

    def __init__(self, name: str, df: pd.DataFrame):
        self.name    = name
        self.df      = df
        self.results = []
        self.passed  = 0
        self.failed  = 0

    def check(self, description: str,
              condition: bool, detail: str = "") -> None:
        """Run a single check and record the result."""
        if condition:
            self.results.append(("✅", description, detail))
            self.passed += 1
        else:
            self.results.append(("❌", description, detail))
            self.failed += 1

    def not_null(self, column: str) -> None:
        null_count = self.df[column].isna().sum()
        self.check(
            f"Column '{column}' has no nulls",
            null_count == 0,
            f"{null_count} null values found" if null_count else ""
        )

    def is_unique(self, column: str) -> None:
        dup_count = self.df[column].duplicated().sum()
        self.check(
            f"Column '{column}' has no duplicates",
            dup_count == 0,
            f"{dup_count} duplicate values found" if dup_count else ""
        )

    def min_rows(self, count: int) -> None:
        actual = len(self.df)
        self.check(
            f"Table has at least {count} rows",
            actual >= count,
            f"Only {actual} rows found" if actual < count else ""
        )

    def column_exists(self, column: str) -> None:
        exists = column in self.df.columns
        self.check(
            f"Column '{column}' exists",
            exists,
            "Column missing from dataset" if not exists else ""
        )

    def in_set(self, column: str, valid_values: set) -> None:
        if column not in self.df.columns:
            self.check(f"Column '{column}' values in valid set",
                       False, "Column missing")
            return
        invalid = self.df[~self.df[column].isin(valid_values)][column]
        invalid_unique = set(invalid.dropna().unique())
        self.check(
            f"Column '{column}' only contains valid values",
            len(invalid_unique) == 0,
            f"Invalid values: {invalid_unique}" if invalid_unique else ""
        )

    def between(self, column: str,
                min_val: float, max_val: float) -> None:
        if column not in self.df.columns:
            self.check(f"Column '{column}' values in range",
                       False, "Column missing")
            return
        series = pd.to_numeric(self.df[column], errors="coerce")
        out_of_range = series[(series < min_val) |
                               (series > max_val)].count()
        self.check(
            f"Column '{column}' values between "
            f"{min_val:,} and {max_val:,}",
            out_of_range == 0,
            f"{out_of_range} values out of range" if out_of_range else ""
        )

    def positive(self, column: str) -> None:
        self.between(column, 0.01, float("inf"))

    def print_results(self) -> None:
        print(f"\n  Dataset : {self.name}")
        print(f"  Rows    : {len(self.df):,}")
        print(f"  {'─'*50}")
        for icon, desc, detail in self.results:
            print(f"  {icon}  {desc}")
            if detail:
                print(f"       → {detail}")
        print(f"  {'─'*50}")
        status = "PASSED" if self.failed == 0 else "FAILED"
        print(f"  Result  : {status} "
              f"({self.passed}/{self.passed + self.failed} checks)")

    @property
    def success(self) -> bool:
        return self.failed == 0


# ══════════════════════════════════════════════════════════════
# EXPECTATION SUITES — One function per dataset
# ══════════════════════════════════════════════════════════════

def validate_equipment_sales(df: pd.DataFrame) -> DataQualityValidator:
    v = DataQualityValidator("equipment_sales", df)

    # Required columns
    for col in ["sale_id", "date", "branch", "region",
                "customer_id", "industry_segment",
                "sale_amount_inr", "finance_type"]:
        v.column_exists(col)

    # Row count
    v.min_rows(1)

    # Nulls
    v.not_null("sale_id")
    v.not_null("customer_id")
    v.not_null("sale_amount_inr")
    v.not_null("date")

    # Uniqueness
    v.is_unique("sale_id")

    # Value ranges
    v.between("sale_amount_inr", 1, 100_000_000)

    # Valid sets
    v.in_set("region",
             {"North", "South", "East", "West", "Central"})
    v.in_set("industry_segment",
             {"Mining", "Construction", "Power & Energy",
              "Oil & Gas", "Infrastructure"})
    v.in_set("finance_type",
             {"Cash", "Cat Finance", "Bank Loan"})

    return v


def validate_parts_orders(df: pd.DataFrame) -> DataQualityValidator:
    v = DataQualityValidator("parts_orders", df)

    for col in ["order_id", "date", "part_number",
                "quantity_ordered", "unit_price_inr",
                "urgency_level"]:
        v.column_exists(col)

    v.min_rows(1)
    v.not_null("order_id")
    v.not_null("part_number")
    v.not_null("quantity_ordered")
    v.is_unique("order_id")
    v.between("quantity_ordered", 1, 10_000)
    v.between("unit_price_inr", 1, 1_000_000)
    v.in_set("urgency_level", {"Standard", "Urgent", "Critical"})

    return v


def validate_service_workorders(df: pd.DataFrame) -> DataQualityValidator:
    v = DataQualityValidator("service_workorders", df)

    for col in ["workorder_id", "date_raised", "date_resolved",
                "machine_serial", "fault_category",
                "labour_hours", "sla_hours_promised", "sla_met"]:
        v.column_exists(col)

    v.min_rows(1)
    v.not_null("workorder_id")
    v.not_null("machine_serial")
    v.not_null("date_raised")
    v.is_unique("workorder_id")
    v.between("labour_hours", 0.5, 720)
    v.in_set("sla_hours_promised", {4, 8, 24, 48})
    v.in_set("sla_met", {"Yes", "No"})
    v.in_set("fault_category",
             {"Engine", "Hydraulics", "Transmission",
              "Electrical", "Undercarriage"})

    return v


def validate_customers(df: pd.DataFrame) -> DataQualityValidator:
    v = DataQualityValidator("customers", df)

    for col in ["customer_id", "customer_name",
                "industry_segment", "region",
                "fleet_size", "contract_type"]:
        v.column_exists(col)

    v.min_rows(1)
    v.not_null("customer_id")
    v.not_null("fleet_size")
    v.is_unique("customer_id")
    v.between("fleet_size", 1, 500)
    v.in_set("contract_type", {"CVA", "Ad-hoc", "Rental", "None"})
    v.in_set("region",
             {"North", "South", "East", "West", "Central"})

    return v


# ══════════════════════════════════════════════════════════════
# MAIN — Run all validations
# ══════════════════════════════════════════════════════════════

if __name__ == "__main__":

    print("=" * 60)
    print("GMMCO DATA QUALITY VALIDATION")
    from datetime import datetime, timezone
    print(f"Started: {datetime.now(timezone.utc).isoformat()} UTC")
    print("=" * 60)

    # Map: CSV path → validator function
    validations = [
        ("sample_data/equipment_sales.csv",
         validate_equipment_sales),
        ("sample_data/parts_orders.csv",
         validate_parts_orders),
        ("sample_data/service_workorders.csv",
         validate_service_workorders),
        ("sample_data/customers.csv",
         validate_customers),
    ]

    validators = []

    for csv_path, validator_fn in validations:
        try:
            df = pd.read_csv(csv_path)
            v  = validator_fn(df)
            v.print_results()
            validators.append(v)
        except FileNotFoundError:
            print(f"\n  ❌ FILE NOT FOUND: {csv_path}")
            sys.exit(1)

    # ── Final summary ──────────────────────────────────────────
    total_checks = sum(v.passed + v.failed for v in validators)
    total_passed = sum(v.passed for v in validators)
    total_failed = sum(v.failed for v in validators)
    all_passed   = all(v.success for v in validators)

    print("\n" + "=" * 60)
    print("VALIDATION SUMMARY")
    print("=" * 60)
    print(f"  {'Dataset':<35} {'Status':>10}")
    print(f"  {'─'*47}")
    for v in validators:
        status = "✅ PASSED" if v.success else "❌ FAILED"
        print(f"  {v.name:<35} {status:>10}")
    print(f"  {'─'*47}")
    print(f"  Total checks : {total_checks}")
    print(f"  Passed       : {total_passed}")
    print(f"  Failed       : {total_failed}")
    print("=" * 60)

    if all_passed:
        print("\n✅ ALL QUALITY CHECKS PASSED")
        print("   Pipeline may proceed to Bronze ingestion.\n")
        sys.exit(0)
    else:
        print("\n❌ QUALITY CHECKS FAILED")
        print("   Pipeline BLOCKED. Fix source data before retrying.\n")
        sys.exit(1)
