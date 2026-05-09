# DataOps Pipeline — Medallion Architecture on Databricks

**Built by [Murali Doss](https://www.linkedin.com/in/dossops/) | DevSecOps & DataOps Consultant**

A production-grade DataOps pipeline built on Databricks Free Edition, demonstrating the complete Medallion Architecture with automated quality gates, CI/CD deployment, and Delta Lake rollback capability.

Built as a reference implementation for a leading heavy equipment dealer and authorised Caterpillar distributor operating across India — covering equipment sales, parts management, service operations, and fleet analytics across 10 branches and 5 regions.

---

## The Problem This Solves

Data engineers in many organisations modify production notebooks directly. There is no version control, no quality gate, no automated deployment, and no rollback when something goes wrong.

The result: silent data corruption reaches dashboards before anyone knows. Recovery takes days. Business decisions are made on wrong numbers.

This pipeline solves all of that.

---

## Architecture

```
Every pipeline run follows this automated sequence:

git push origin main
        │
        ▼
┌─────────────────────────────────────────────────────────┐
│                  GITHUB ACTIONS CI/CD                    │
│                                                          │
│  Job 1: Quality Gates                                    │
│    ├── Schema validation (structure check)               │
│    └── Data quality checks (62 rules across 4 datasets) │
│                        │ all pass                        │
│  Job 2: Deploy Notebooks                                 │
│    ├── Bronze notebook → /gmmco-poc/bronze/              │
│    ├── Silver notebook → /gmmco-poc/silver/              │
│    └── Gold notebook   → /gmmco-poc/gold/                │
│                        │ deployed                        │
│  Job 3: Trigger Pipeline                                 │
│    └── Databricks Job API call                           │
└─────────────────────────────────────────────────────────┘
        │
        ▼
┌─────────────────────────────────────────────────────────┐
│              DATABRICKS — MEDALLION ARCHITECTURE         │
│                                                          │
│  Task 1: Bronze Ingest                                   │
│    Raw CSVs → Delta tables + audit columns               │
│    (_ingested_at, _source_file, _record_hash)            │
│                        │                                 │
│  Task 2: Silver Transform                                 │
│    Business rules applied, bad records quarantined       │
│    Enriched columns: SLA breach flag, customer tier      │
│    Resolution days, sale quarter, urgency rank           │
│                        │                                 │
│  Task 3: Gold Aggregate                                  │
│    5 business metric tables ready for BI consumption     │
│    Auto-refreshed executive dashboard                    │
└─────────────────────────────────────────────────────────┘
        │
        ▼
  Executive Dashboard — 5 live panels
  Email alert on success or failure
```

---

## What Makes This Production-Grade

### Three Sequential Quality Gates

Every pipeline run passes through three mandatory gates before data reaches Bronze. If any gate fails, the pipeline stops. Yesterday's clean data remains untouched.

| Gate | Tool | What It Checks |
|---|---|---|
| Schema validation | Custom Python | Column presence, data types, no structural drift |
| Data quality | Custom validator | 62 rules — nulls, ranges, valid sets, uniqueness |
| Notebook linting | GitHub Actions | Code committed and reviewed before deployment |

### Medallion Architecture — Three Layers

**Bronze — Raw data landing zone**
Every record arrives exactly as it came from the source system. Three audit columns added to every row: when it arrived, which file it came from, and a hash fingerprint of its content. Nothing is modified. Nothing is deleted. This is your audit trail and recovery point.

**Silver — Trusted data layer**
Business rules applied. Records failing validation are quarantined in a separate rejected table with a reason — not silently deleted. Calculated fields added that the business needs: SLA breach flag, resolution days, customer tier, sale quarter, urgency rank.

**Gold — Business-ready aggregations**
Five tables answering five business questions directly. These are what dashboards read. Rebuilt completely on every pipeline run from Silver — always consistent, always current.

### Delta Lake Time Travel and Rollback

Every write to a Delta table creates a new version. If a bad transformation reaches Gold, recovery is one command:

```bash
python scripts/rollback.py --layer gold --steps 1 --token YOUR_TOKEN
```

Rolls back all Gold tables to the previous version. Under 2 minutes. No data loss. The old version remains in Delta log for audit purposes.

---

## Data Model

Four source datasets simulating a heavy equipment dealer's operational data:

| Dataset | Records | Description |
|---|---|---|
| equipment_sales | 1,000 | Sales transactions by branch, region, segment, product |
| parts_orders | 2,000 | Parts procurement with urgency classification |
| service_workorders | 1,500 | Field service jobs with SLA tracking |
| customers | 201 | Customer master with fleet size and contract type |

Five Gold tables serving business consumers:

| Gold Table | Business Question |
|---|---|
| gold_sales_performance | Revenue by region, segment, quarter — for Sales Director |
| gold_sla_compliance | SLA met/breached by branch with RED/AMBER/GREEN flag — for Service Director |
| gold_parts_analysis | Parts spend and urgency distribution — for Procurement |
| gold_customer_portfolio | Customer health with AT RISK flagging — for Account Management |
| gold_executive_kpis | Headline KPIs in one table — for CEO and CFO |

---

## Repository Structure

```
dataops-gmmco-poc/
├── .github/
│   └── workflows/
│       └── dataops-pipeline.yml    ← 3-job CI/CD pipeline
├── notebooks/
│   ├── bronze/
│   │   └── 01_bronze_ingest.py     ← Raw ingest with audit columns
│   ├── silver/
│   │   └── 02_silver_transform.py  ← Business rules and enrichment
│   └── gold/
│       └── 03_gold_aggregate.py    ← Business metric aggregations
├── data_quality/
│   ├── validate_schema.py          ← Gate 1: structural validation
│   └── validate_data.py            ← Gate 2: content validation (62 checks)
├── scripts/
│   └── rollback.py                 ← Delta Lake time travel rollback tool
├── dashboards/
│   ├── 01_revenue_by_region.sql
│   ├── 02_sla_compliance.sql
│   ├── 03_parts_analysis.sql
│   ├── 04_customer_portfolio.sql
│   └── 05_executive_kpis.sql
├── sample_data/
│   └── generate_data.py            ← Realistic synthetic data generator
├── .gitignore
└── README.md
```

---

## Running Locally

**Prerequisites:** Python 3.11+, Databricks account, Databricks CLI v2

```bash
# Clone the repository
git clone git@github.com:MurLeeDas/dataops-gmmco-poc.git
cd dataops-gmmco-poc

# Install dependencies
pip install pandas requests

# Generate sample data
python sample_data/generate_data.py

# Run schema validation
python data_quality/validate_schema.py

# Run data quality checks
python data_quality/validate_data.py

# Configure Databricks CLI
databricks configure
# Host: your-workspace.cloud.databricks.com
# Token: your-personal-access-token

# View Delta table version history
python scripts/rollback.py --list gold --token YOUR_TOKEN

# Roll back Gold layer by 1 version
python scripts/rollback.py --layer gold --steps 1 --token YOUR_TOKEN
```

---

## GitHub Actions Setup

Add these secrets to your repository:

| Secret | Description |
|---|---|
| `DATABRICKS_TOKEN` | Personal access token from Databricks settings |
| `DATABRICKS_JOB_ID` | Job ID of the Databricks pipeline job |

The pipeline triggers automatically on push to `main` when files in `notebooks/`, `data_quality/`, `sample_data/`, or `.github/workflows/` change. Manual trigger is also available via the Actions tab.

---

## Dashboard

Five panels built on the Gold layer in Databricks Dashboards, auto-refreshed daily at 06:00 AM:

- Equipment Sales Revenue by Region — stacked bar by industry segment
- SLA Compliance by Branch — table with RED/AMBER/GREEN performance flag
- Top Parts by Spend — horizontal bar by urgency level
- Customer Health Distribution — donut chart with AT RISK flagging
- Executive KPI Summary — all headline metrics in one table

---

## Key Design Decisions

**Why pure Python for quality validation instead of Great Expectations?**
Every check is readable, maintainable, and explainable in a meeting without opening documentation. The team who inherits this pipeline can add a new rule in 30 seconds. No framework version dependencies to manage.

**Why quarantine rejected records instead of dropping them?**
Silently dropping bad records hides source system problems. Quarantining them in a rejected table makes the problem visible. The data team can trace every rejection back to its cause and fix the upstream system.

**Why git SHA image tagging for notebooks?**
Every notebook version in Databricks is traceable to the exact commit that deployed it. If a production issue appears, the debugging path is: check deployment logs, find the commit SHA, review the diff. No ambiguity about what changed.

**Why Delta Lake over Parquet?**
ACID transactions, time travel, and schema enforcement. If a write fails halfway, the table is not corrupted. If a bad transformation is deployed, roll back to the previous version in one command. Parquet offers none of this.

---

## Consultant Contact

**Murali Doss**
DevSecOps and DataOps Consultant
Specialised in data platform modernisation for manufacturing, telecom, and fintech organisations.

[LinkedIn](https://www.linkedin.com/in/dossops/) · [GitHub](https://github.com/MurLeeDas)

Available for DataOps maturity assessments, Medallion Architecture implementations, and data pipeline modernisation engagements.