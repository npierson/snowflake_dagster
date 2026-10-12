# Snowflake + Dagster Data Pipeline

This repository contains a Dagster project integrated with Snowflake and dbt for data orchestration and transformation.

---
## Platform access
```
Username: MONOTREMES
Account Login URL: https://fsyjvmv-pzb47408.snowflakecomputing.com
Organization name FSYJVMV
Account name PZB47408
Role ACCOUNTADMIN
Account locator
HXB50797
Cloud platform
AWS
Edition
Enterprise

```


## Prerequisites

- **Python**: `>= 3.10`
- **Snowflake Account**: With appropriate roles, database, and warehouse permissions.

---

## Project Structure

```text
snowflake_dagster/
├── GEMINI.md                # Antigravity project context & instructions
├── pyproject.toml           # Project metadata and dependencies
├── .env                     # Environment variables (credentials)
├── .env.example             # Template for environment variables
├── snowflake_dagster/       # Dagster orchestration package
│   ├── assets/
│   │   ├── ingestion.py     # S3 -> Snowflake raw ingest asset (COPY INTO)
│   │   └── dbt_assets.py    # dbt manifest loading via @dbt_assets
│   ├── resources.py         # Snowflake & dbt CLI resources
│   └── definitions.py       # Dagster Definitions entry point
├── dbt_project/             # dbt transformation project
│   ├── dbt_project.yml
│   ├── profiles.yml         # dbt profile targeting Snowflake
│   ├── packages.yml         # dbt packages (dbt-utils)
│   ├── models/
│   │   ├── staging/         # 1:1 view models (stg_events.sql)
│   │   ├── intermediate/    # Transformation & deduplication (int_events__deduped.sql)
│   │   └── marts/           # Kimball star schema (dim_users.sql, fct_events.sql)
│   └── tests/
└── README.md                # Setup and development guide
```

---

## Development Setup

### 1. Clone & Navigate to Repository

```bash
git clone <repository-url>
cd snowflake_dagster
```

### 2. Create and Activate a Virtual Environment

```bash
# Create a virtual environment
python3 -m venv .venv

# Activate the virtual environment
# macOS / Linux:
source .venv/bin/activate

# Windows (Command Prompt / PowerShell):
# .venv\Scripts\activate
```

### 3. Install Dependencies

Install the project and all its required dependencies in editable mode (`-e`):

```bash
pip install -e .
```

This installs core dependencies defined in `pyproject.toml`:
- `dagster` & `dagster-webserver`
- `dagster-snowflake` & `snowflake-connector-python`
- `dagster-dbt` & `dbt-snowflake`
- `python-dotenv`

### 4. Configure Environment Variables

Create a `.env` file in the project root:

```bash
cp .env.example .env
```

Populate the `.env` file with your Snowflake credentials:

```dotenv
SNOWFLAKE_ACCOUNT="your_account_id.region"
SNOWFLAKE_USER="your_username"
SNOWFLAKE_PASSWORD="your_password"
SNOWFLAKE_ROLE="TRANSFORMER_ROLE"
SNOWFLAKE_DATABASE="DEV_ANALYTICS"
SNOWFLAKE_WAREHOUSE="COMPUTE_WH"
```

> **Important**: Never commit `.env` containing sensitive credentials to version control.

---

## Running Dagster Locally

Start the Dagster development server (launches the web UI and code server daemon):

```bash
dagster dev
```

Once running, access the Dagster UI in your web browser:

👉 **[http://localhost:3000](http://localhost:3000)**

---

## Troubleshooting

### `ModuleNotFoundError: No module named 'snowflake_dagster'`
- **Cause**: Either `definitions.py` is in the repository root rather than inside the `snowflake_dagster/` package directory, or the package was not installed into the virtual environment.
- **Fix**: Ensure your code lives inside `snowflake_dagster/` and run `pip install -e .` with your virtual environment activated.

### Port 3000 Already in Use
- Run on a custom port:
  ```bash
  dagster dev -p 3001
  ```

