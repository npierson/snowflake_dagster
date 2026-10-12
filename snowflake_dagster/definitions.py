from dagster import Definitions, load_assets_from_modules

from snowflake_dagster.assets import dbt_assets, ingestion
from snowflake_dagster.resources import dbt_resource, snowflake_resource

all_assets = [
    *load_assets_from_modules([ingestion]),
    *load_assets_from_modules([dbt_assets]),
]

defs = Definitions(
    assets=all_assets,
    resources={
        "snowflake": snowflake_resource,
        "dbt": dbt_resource,
    },
)