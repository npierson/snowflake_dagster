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

snowflake_config = {
    "account": EnvVar("SNOWFLAKE_ACCOUNT"),
    "user": EnvVar("SNOWFLAKE_USER"),
    "role": EnvVar("SNOWFLAKE_ROLE"),
    "database": EnvVar("SNOWFLAKE_DATABASE"),
    "warehouse": EnvVar("SNOWFLAKE_WAREHOUSE"),
    "schema_": os.getenv("SNOWFLAKE_SCHEMA", "CORE"),
}

# Support both Key-Pair and Password authentication
if os.getenv("SNOWFLAKE_PRIVATE_KEY_PATH"):
    snowflake_config["private_key_path"] = os.getenv("SNOWFLAKE_PRIVATE_KEY_PATH")
    if os.getenv("SNOWFLAKE_PRIVATE_KEY_PASSPHRASE"):
        snowflake_config["private_key_password"] = os.getenv("SNOWFLAKE_PRIVATE_KEY_PASSPHRASE")
else:
    snowflake_config["password"] = os.getenv("SNOWFLAKE_PASSWORD", "")

snowflake_resource = SnowflakeResource(**snowflake_config)
