# SpotterPrep — Enterprise Data Pipeline Prototype

**8.6M rows · 5 business domains · ~19 GB · 10 Snowflake tables · 1,880 columns**

A production-grade data pipeline built to demonstrate the power of ThoughtSpot's Spotter AI on enterprise-scale, realistic data. Covers the full lifecycle: synthetic data generation → profiling → quality scoring → cleaning → Snowflake loading → engineering handoff.

---

## What This Project Does

Most demos use toy datasets. This one doesn't.

SpotterPrep generates 5 enterprise-grade synthetic datasets across completely different business domains, each with **intentionally introduced real-world data quality issues** — nulls, duplicates, format inconsistencies, impossible sequences, range violations. Every dataset is then profiled, scored, cleaned, and loaded into Snowflake as a side-by-side raw/cleaned pair.

The result: a realistic data environment where Spotter can demonstrate the difference between querying dirty data vs. production-ready data.

---

## Architecture

```
CSV Generation ──▶ Profiling ──▶ Quality Scoring ──▶ Cleaning ──▶ Snowflake Load
      │                │               │                 │              │
  5 domains       5 dimensions     Grade B→A         54K rows      8.6M rows
   ~19 GB raw      per dataset     +6.2 pts avg       removed      10 tables
```

See [`docs/architecture.md`](docs/architecture.md) for the full system design.

---

## Dataset Overview

| # | Dataset | Domain | Raw Rows | Cols | Size | Clean Rows | Rows Removed | Quality Lift |
|---|---------|--------|----------|------|------|------------|-------------|-------------|
| 1 | CUSTOMER_ORDERS | E-commerce / B2B SaaS | 100,000 | 200 | 234 MB | 99,655 | 345 | 88.5 → 95.7 **(B→A)** |
| 2 | IOT_TELEMETRY | Industrial IoT | 500,000 | 300 | 1.9 GB | 499,244 | 756 | 94.0 → 99.1 **(B→A)** |
| 3 | HR_WORKFORCE | Enterprise HR | 800,000 | 400 | 4.4 GB | 790,809 | 9,191 | 89.7 → 95.1 **(B→A)** |
| 4 | FINANCIAL_LEDGER | General Ledger / Finance | 1,000,000 | 480 | 3.6 GB | 996,405 | 3,595 | 93.1 → 99.6 **(B→A)** |
| 5 | PRODUCT_CATALOG | Global E-commerce | 2,000,000 | 500 | 9.0 GB | 1,959,606 | 40,394 | 91.6 → 98.4 **(B→A)** |
| **∑** | | | **4,400,000** | **1,880** | **~19 GB** | **4,345,719** | **54,281** | **91.4 → 97.6 avg** |

---

## Key Metrics

```
✦ 8,645,719 rows loaded into Snowflake across 10 tables
✦ 54,281 dirty rows identified and removed (1.23% of total)
✦ 400+ columns incorrectly typed — caught by schema validation
✦ 100% of datasets improved from Grade B → Grade A after cleaning
✦ Average quality improvement: +6.2 percentage points across 5 dimensions
✦ 10 Snowflake tables, production-ready, zero manual SQL written
✦ 952-line engineering handoff document
```

---

## Quality Scoring Framework

Every dataset is scored across 5 dimensions before and after cleaning:

| Dimension | Weight | What It Measures |
|-----------|--------|-----------------|
| Completeness | 25% | % of rows where critical fields are non-null |
| Validity | 25% | % of rows within expected type, range, format |
| Uniqueness | 20% | % of rows not duplicating another (by PK) |
| Consistency | 20% | % of rows conforming to domain vocab & business rules |
| Accuracy | 10% | % of rows where logical/temporal relationships hold |

**Grade thresholds:** A = 95–100 · B = 85–94 · C = 70–84

See [`docs/quality_framework.md`](docs/quality_framework.md) for the full methodology.

---

## Quality Results — Raw vs Cleaned

| Dataset | Raw Score | Clean Score | Delta | Grade Change |
|---------|-----------|------------|-------|-------------|
| CUSTOMER_ORDERS | 88.5 | 95.7 | +7.2 | B → **A** |
| IOT_TELEMETRY | 94.0 | 99.1 | +5.1 | B → **A** |
| HR_WORKFORCE | 89.7 | 95.1 | +5.4 | B → **A** |
| FINANCIAL_LEDGER | 93.1 | 99.6 | +6.5 | B → **A** |
| PRODUCT_CATALOG | 91.6 | 98.4 | +6.8 | B → **A** |
| **Average** | **91.4** | **97.6** | **+6.2** | **All B → A** |

---

## The Bug That Almost Broke Everything

During development, a single-line substring check `"_at" in column_name` caused **400+ numeric columns** to be mislabeled as `TIMESTAMP_NTZ` in Snowflake. Every row was rejected. 8 of 10 tables loaded with 0 rows.

The fix was 2 lines of regex. The lesson: always use word-boundary checks when pattern-matching column names.

Full incident report: [`docs/bug_postmortem.md`](docs/bug_postmortem.md)

---

## Tech Stack

| Component | Technology |
|-----------|-----------|
| Language | Python 3.9 |
| Data manipulation | pandas, numpy |
| Cloud data warehouse | Snowflake |
| Snowflake client | snowflake-connector-python |
| Load strategy | PUT (staged upload) + COPY INTO (server-side parallel parse) |
| Schema inference | Custom pandas → Snowflake type mapper with regex |
| Credentials | python-dotenv (.env file, gitignored) |
| Orchestration | Bash shell script |

---

## Snowflake Schema

```
SPOTTERPREP_TEST
├── RAW
│   ├── CUSTOMER_ORDERS_RAW      (100,000 rows)
│   ├── IOT_TELEMETRY_RAW        (500,000 rows)
│   ├── HR_WORKFORCE_RAW         (800,000 rows)
│   ├── FINANCIAL_LEDGER_RAW   (1,000,000 rows)
│   └── PRODUCT_CATALOG_RAW    (2,000,000 rows)
└── CLEANED
    ├── CUSTOMER_ORDERS_CLEANED   (99,655 rows)
    ├── IOT_TELEMETRY_CLEANED    (499,244 rows)
    ├── HR_WORKFORCE_CLEANED     (790,809 rows)
    ├── FINANCIAL_LEDGER_CLEANED (996,405 rows)
    └── PRODUCT_CATALOG_CLEANED (1,959,606 rows)
```

---

## How to Run

### Prerequisites

```bash
pip install pandas numpy snowflake-connector-python python-dotenv boto3
```

### 1. Set credentials

```bash
cp .env.example .env
# Edit .env with your Snowflake account details
```

### 2. Generate datasets

```bash
python3 scripts/generate_all.py
```

### 3. Load to Snowflake

```bash
# Load all 10 tables
python3 scripts/load_to_snowflake.py

# Load a single dataset
python3 scripts/load_to_snowflake.py --dataset 1

# Dry run — print DDL without connecting
python3 scripts/load_to_snowflake.py --dry-run
```

---

## Snowflake Quickstart

```sql
USE DATABASE SPOTTERPREP_TEST;

-- Verify all 10 tables
SELECT table_schema, table_name, row_count
FROM information_schema.tables
WHERE table_schema IN ('RAW', 'CLEANED')
ORDER BY table_schema, table_name;

-- Raw vs cleaned comparison
SELECT * FROM RAW.CUSTOMER_ORDERS_RAW     LIMIT 5;
SELECT * FROM CLEANED.CUSTOMER_ORDERS_CLEANED LIMIT 5;
```

See [`sql/quickstart_queries.sql`](sql/quickstart_queries.sql) for full query library.

---

## Project Structure

```
spotterprep-data-pipeline/
├── README.md
├── SPOTTERPREP_ENGINEERING_HANDOFF.md   # Full 952-line technical handoff
├── .env.example                          # Credential template
├── .gitignore
├── scripts/
│   ├── load_to_snowflake.py              # Core loader
│   ├── run_remaining_loads.sh            # Orchestration
│   └── generate_all.py                   # Data generation
├── docs/
│   ├── architecture.md                   # System design
│   ├── quality_framework.md              # 5-dimension scoring methodology
│   └── bug_postmortem.md                 # Production bug incident report
└── sql/
    ├── quickstart_queries.sql            # Ready-to-run Snowflake queries
    └── quality_checks.sql               # Per-dimension validation queries
```

---

## Engineering Handoff

A full [`SPOTTERPREP_ENGINEERING_HANDOFF.md`](SPOTTERPREP_ENGINEERING_HANDOFF.md) is included covering:
- Per-dataset data profiles with dimension-level quality breakdowns
- Cleaning decisions with before/after comparisons
- Snowflake DDL and loading strategy
- SQL quickstart queries
- Pre/post comparative analysis across all 10 tables

---

## Built By

Product-led, AI-assisted. Built as a SpotterPrep demo environment to showcase ThoughtSpot Spotter on realistic enterprise data.
