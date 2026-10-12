import os
import sys
from pathlib import Path
import shutil
from dotenv import load_dotenv
from dagster import EnvVar
from dagster_dbt import DbtCliResource
from dagster_snowflake import SnowflakeResource

# Load environment variables from .env if present
load_dotenv()

DBT_PROJECT_DIR = Path(__file__).joinpath("..", "..", "dbt_project").resolve()
dbt_executable = shutil.which("dbt") or os.fspath(Path(sys.executable).parent / "dbt")

dbt_resource = DbtCliResource(
    project_dir=os.fspath(DBT_PROJECT_DIR),
    profiles_dir=os.fspath(DBT_PROJECT_DIR),
    dbt_executable=dbt_executable,
)

snowflake_resource = SnowflakeResource(
    account=EnvVar("SNOWFLAKE_ACCOUNT"),
    user=EnvVar("SNOWFLAKE_USER"),
    password=EnvVar("SNOWFLAKE_PASSWORD"),
    role=EnvVar("SNOWFLAKE_ROLE"),
    database=EnvVar("SNOWFLAKE_DATABASE"),
    warehouse=EnvVar("SNOWFLAKE_WAREHOUSE"),
    schema=os.getenv("SNOWFLAKE_SCHEMA", "PUBLIC"),
)
