# Project Context: Snowflake + S3 Parquet ELT with Dagster & dbt

## 1. Project Overview & Architecture

This project demonstrates an end-to-end modern ELT data pipeline orchestrated locally with Dagster (running via `dagster dev` on `localhost:3000`). It ingests Parquet source files stored in AWS S3 into Snowflake, builds a dimensional Kimball star schema using dbt Core (`dbt-snowflake`), and enforces automated data testing and lineage.

### High-Level Data Flow

```text
AWS S3 (Parquet files)
        │
        │  Snowflake External Stage via Storage Integration
        ▼
Snowflake RAW layer (staging database / schema)
        │
        │  Orchestrated by Dagster via dagster-dbt
        ▼
dbt transformation layers
  ├── Staging:      schema flattening, type casting, basic renames
  ├── Intermediate: deduplication, business logic, surrogate keys
  └── Marts:        dimensional models (conformed dims + fact tables)
```

---

## 2. Technology Stack & Environment

- **Orchestration:** Dagster Open Source (`dagster`, `dagster-webserver`, `dagster-dbt`)
- **Transformation:** dbt Core with `dbt-snowflake`
- **Cloud Storage:** AWS S3 (housing source Parquet datasets)
- **Data Warehouse:** Snowflake (Standard compute warehouse, Role-Based Access Control)
- **Language / Runtime:** Python 3.12+ (isolated virtual environment `.venv`)

---

## 3. Directory Structure Standards

```text
.
├── GEMINI.md                      # Antigravity project context & instructions
├── pyproject.toml / requirements.txt
├── .env.example                   # Template for environment variables (never commit .env)
├── dagster_pipeline/              # Dagster orchestration project
│   ├── __init__.py
│   ├── assets/
│   │   ├── ingestion.py           # S3 -> Snowflake raw ingest assets (COPY INTO)
│   │   └── dbt_assets.py          # dbt manifest parsing via @dbt_assets
│   ├── resources.py               # Snowflake & dbt CLI resources
│   └── definitions.py             # Dagster Definitions (assets, schedules, resources)
└── dbt_project/                   # dbt transformation project
    ├── dbt_project.yml
    ├── profiles.yml               # dbt profile targeting Snowflake via env vars
    ├── models/
    │   ├── staging/               # stg_<source>__<entity>.sql
    │   ├── intermediate/          # int_<entity>__<verb/grain>.sql
    │   └── marts/                 # dim_<entity>.sql, fct_<process>.sql
    └── tests/                     # Custom and generic dbt tests
```

---

## 4. Key Architectural Constraints & Conventions

### Security & Credentials

- **No hardcoded secrets:** Never hardcode AWS keys or Snowflake passwords.
- Snowflake authentication must use environment variables: `SNOWFLAKE_ACCOUNT`, `SNOWFLAKE_USER`, `SNOWFLAKE_ROLE`, `SNOWFLAKE_WAREHOUSE`, `SNOWFLAKE_DATABASE`, and password or private key.
- In Snowflake, S3 access is managed via a configured Snowflake **Storage Integration** and **External Stage** referencing IAM roles.

### Ingestion Logic

- Dagster ingestion assets must trigger deterministic, idempotent loads using Snowflake `COPY INTO` syntax targeting Parquet stages:
  - Match schema case-insensitively (`MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE`).
  - Raw landing tables should handle semi-structured attributes or map Parquet columns cleanly.

### dbt Modeling Standards

- **Staging (`models/staging`):** 1:1 views over raw tables. Cast data types, clean column names, extract nested fields.
- **Intermediate (`models/intermediate`):** Ephemeral or transient tables applying business logic, deduplication, and joins.
- **Marts (`models/marts`):** Persistent tables / incremental models implementing a Kimball star schema.
  - Dimensions must be prefixed with `dim_` and include surrogate keys (e.g., `dbt_utils.generate_surrogate_key`).
  - Facts must be prefixed with `fct_`, with a clean grain and foreign keys pointing to dimension surrogate keys.
- **Testing:** Every mart model must include `not_null` and `unique` tests on primary keys in YAML schema files.

### Dagster Integration Standards

- Use the modern `dagster-dbt` integration (`DbtCliResource`, `@dbt_assets`).
- Ensure the dbt manifest (`manifest.json`) is read or built cleanly in development without breaking Dagster asset resolution.
- Dagster assets should declare upstream dependencies accurately so the entire graph, from S3 Parquet landing to final `fct_` tables, renders contiguously in the Dagster UI.

---

## 5. Development Workflow Commands

```bash
# Activate virtual environment
source .venv/bin/activate

# Run Dagster UI locally (http://localhost:3000)
dagster dev

# Compile / test dbt independently
dbt compile --project-dir dbt_project --profiles-dir dbt_project
dbt test --project-dir dbt_project --profiles-dir dbt_project
```
