# Pipeline Tour & Order of Execution: AWS S3 → Snowflake → dbt → Dagster

This document provides a complete walkthrough of the **AWS S3 → Snowflake → dbt → Dagster** ELT pipeline and its exact order of execution, broken down into **One-Time Infrastructure Setup**, **Dagster Startup (Code Load Time)**, and **Runtime Execution (Materialization Order)**.

---

## High-Level Architecture & Execution Flow

```text
[0. One-Time Setup]
  Snowflake RBAC + Storage Integration + @S3_PARQUET_STAGE + RAW_* Tables
        │
        ▼
[1. Dagster Startup (`dagster dev`)]
  Loads SnowflakeResource & DbtCliResource -> Parses dbt manifest.json -> Stitches Lineage Graph
        │
        ▼
[2. Layer 1: Raw S3 Ingestion (Dagster Python Assets)]
  Executes `COPY INTO RAW_<TABLE> FROM @S3_PARQUET_STAGE/<file>.parquet`
        │
        ▼
[3. Layer 2: dbt Staging (`models/staging/` — Views)]
  1:1 views over RAW tables (`source('raw_data', 'raw_*')`) — type casting & column renaming
        │
        ▼
[4. Layer 3: dbt Intermediate (`models/intermediate/` — Views)]
  Joins line items & orders (`int_order_items__joined`) and deduplicates events (`int_events__deduped`)
        │
        ▼
[5. Layer 4: dbt Marts (`models/marts/` — Kimball Star Schema Tables)]
  Step 5a: Conformed Dimensions (`dim_customers`, `dim_suppliers`, `dim_parts`, `dim_users`)
  Step 5b: Fact Tables (`fct_orders`, `fct_order_items`, `fct_events`) + Schema Tests
```

---

## Phase 0: One-Time Snowflake & S3 Infrastructure Setup

Before Dagster runs any assets, two SQL bootstrap scripts configure Snowflake and link it to AWS S3:

1. **Database & RBAC Setup (`snowflake_dagster/snowflake_db_setup/database_setup.sql`):**
   - Creates database `ANALYTICS_PROD` and schema `CORE` (dropping `PUBLIC`).
   - Sets up role hierarchy: `ANALYTICS_READ_ROLE` → `ANALYTICS_WRITE_ROLE` → `ANALYTICS_ADMIN_ROLE` → `SYSADMIN`, granting privileges on `ETL_WH` and `ANALYTICS_PROD.CORE`.
2. **S3 Storage Integration & Landing Tables (`snowflake_dagster/snowflake_db_setup/s3_storage_integration_setup.sql`):**
   - Creates `STORAGE INTEGRATION S3_NAT_DATA` pointing to `s3://nat-data-source-351578878554-us-east-1-an/` via AWS IAM role `arn:aws:iam::351578878554:role/snowflake-s3-role`.
   - Creates `PARQUET_FORMAT` and external stage `@S3_PARQUET_STAGE`.
   - Creates the 8 TPC-H `RAW_*` landing tables (`RAW_CUSTOMER`, `RAW_LINEITEM`, `RAW_NATION`, `RAW_ORDERS`, `RAW_PART`, `RAW_PARTSUPP`, `RAW_REGION`, `RAW_SUPPLIER`) using `INFER_SCHEMA` on the staged Parquet files, plus the template `RAW_EVENTS` table.

---

## Phase 1: Dagster Startup & Graph Resolution (`dagster dev`)

When you start Dagster, it evaluates `defs` in `snowflake_dagster/definitions.py`:

1. **Resources Initialized (`snowflake_dagster/resources.py`):**
   - Loads `.env` credentials and configures `snowflake_resource` (`SnowflakeResource`, supporting RSA key-pair or password auth) and `dbt_resource` (`DbtCliResource` pointing at `dbt_project/`).
2. **dbt Manifest Preparation (`snowflake_dagster/assets/dbt_assets.py`):**
   - `dbt_project.prepare_if_dev()` automatically parses the dbt project and generates `dbt_project/target/manifest.json`.
3. **Lineage Stitching (`Key` Matching):**
   - Each S3 ingestion asset in `snowflake_dagster/assets/ingestion.py` declares a two-part Dagster asset key: `key=["raw_data", "raw_<table_name>"]` (e.g., `["raw_data", "raw_orders"]`).
   - In dbt's `dbt_project/models/staging/sources.yml`, the source is named `raw_data` and its tables are named `raw_<table_name>`.
   - Because those keys match `["raw_data", "raw_<table_name>"]`, `@dbt_assets` in `snowflake_dbt_assets` automatically links the Python S3 ingestion assets as upstream parents of the dbt staging models.

---

## Phase 2: Runtime Execution Order (When You Materialize All Assets)

### Step 1: S3 → Snowflake Raw Ingestion (`raw_ingestion` group)

Dagster executes the 9 Python assets in `snowflake_dagster/assets/ingestion.py` (which have no upstream dependencies and can run in parallel):

- **TPC-H Assets:** `raw_customer`, `raw_lineitem`, `raw_nation`, `raw_orders`, `raw_part`, `raw_partsupp`, `raw_region`, `raw_supplier`, plus `raw_s3_parquet_ingest`.
- **What happens under the hood:** Each calls `_ingest_parquet_table()`, which runs an idempotent Snowflake `COPY INTO RAW_<TABLE> FROM @S3_PARQUET_STAGE/<file>.parquet` using `MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE`. Snowflake tracks loaded files so already-ingested Parquet files are skipped unless modified, and Dagster logs metadata (`query_id`, `files_processed`, `rows_loaded`, `total_rows`).

---

### Step 2: dbt Build Triggered (`snowflake_dbt_assets`)

Once the upstream `RAW_*` tables finish loading, Dagster invokes `dbt build` via `snowflake_dbt_assets` (configured in `dbt_project/dbt_project.yml` and `dbt_project/profiles.yml` with `threads: 4`). `dbt build` runs models and tests in topological DAG order across three layers:

#### 2A. Staging Layer (`dbt_project/models/staging/` — Materialized as Snowflake `VIEW`s)

Reads directly from `{{ source('raw_data', 'raw_*') }}`, casts types, and renames TPC-H columns (`o_orderkey` → `order_id`, etc.):

- `stg_tpch__customer.sql` ← `RAW_CUSTOMER`
- `stg_tpch__orders.sql` ← `RAW_ORDERS`
- `stg_tpch__lineitem.sql` ← `RAW_LINEITEM`
- `stg_tpch__part.sql` ← `RAW_PART`
- `stg_tpch__partsupp.sql` ← `RAW_PARTSUPP`
- `stg_tpch__supplier.sql` ← `RAW_SUPPLIER`
- `stg_tpch__nation.sql` ← `RAW_NATION`
- `stg_tpch__region.sql` ← `RAW_REGION`
- `stg_events.sql` ← `RAW_EVENTS`

*(As each staging view finishes, `dbt build` immediately runs the staging schema tests defined in `dbt_project/models/staging/schema.yml`.)*

#### 2B. Intermediate Layer & Independent Conformed Dimensions (Run in Parallel)

Once their upstream staging views pass tests, dbt builds both the intermediate views and the dimension tables that only depend on staging views:

- **Intermediate Views (`dbt_project/models/intermediate/` — Materialized as `VIEW`s):**
  - `int_order_items__joined.sql`: Joins `stg_tpch__lineitem` + `stg_tpch__orders` and computes financial metrics (`item_discount_amount`, `discounted_price`, `item_tax_amount`, `net_amount`).
  - `int_events__deduped.sql`: Deduplicates `stg_events` by `event_id`.
- **Conformed Dimensions (`dbt_project/models/marts/` — Materialized as `TABLE`s):**
  - `dim_customers.sql`: Joins `stg_tpch__customer` + `stg_tpch__nation` + `stg_tpch__region` and generates surrogate key `customer_sk`.
  - `dim_suppliers.sql`: Joins `stg_tpch__supplier` + `stg_tpch__nation` + `stg_tpch__region` and generates surrogate key `supplier_sk`.
  - `dim_parts.sql`: Transforms `stg_tpch__part` and generates surrogate key `part_sk`.

#### 2C. Final Marts — Fact Tables & Dependent Dimensions (`dbt_project/models/marts/` — Materialized as `TABLE`s)

Because the fact tables look up the surrogate keys (`customer_sk`, `part_sk`, `supplier_sk`) from the dimension tables and metrics from the intermediate views, they execute **last**:

- `fct_orders.sql`: Waits for `stg_tpch__orders`, `int_order_items__joined`, and `dim_customers`. Aggregates order-item measures to the order grain and attaches `order_sk` and `customer_sk`.
- `fct_order_items.sql`: Waits for `int_order_items__joined`, `dim_customers`, `dim_parts`, and `dim_suppliers`. Produces the line-item grain fact table with `order_item_sk` and foreign surrogate keys.
- `dim_users.sql` & `fct_events.sql`: Wait for `int_events__deduped`.
- **Final Mart Tests (`dbt_project/models/marts/schema.yml`):** `dbt build` runs `unique`, `not_null`, and `relationships` (referential integrity) tests on the newly built dimension and fact tables before completing the Dagster run.
