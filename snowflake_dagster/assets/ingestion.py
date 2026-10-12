import os
from dagster import AssetExecutionContext, Output, asset
from dagster_snowflake import SnowflakeResource


@asset(
    key=["raw_data", "raw_s3_parquet_ingest"],
    group_name="raw_ingestion",
    compute_kind="snowflake",
    description="Loads Parquet data from AWS S3 external stage into Snowflake RAW landing table",
)
def raw_s3_parquet_ingest(
    context: AssetExecutionContext,
    snowflake: SnowflakeResource,
) -> Output[None]:
    """Idempotently ingests Parquet files from an external S3 stage into Snowflake.

    Uses Snowflake's MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE to ensure robust schema alignment.
    """
    raw_table = os.getenv("SNOWFLAKE_RAW_TABLE", "RAW_EVENTS")
    stage_name = os.getenv("SNOWFLAKE_STAGE_NAME", "@EXTERNAL_S3_STAGE")

    copy_query = f"""
    COPY INTO {raw_table}
    FROM {stage_name}
    FILE_FORMAT = (TYPE = PARQUET)
    MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
    ON_ERROR = 'CONTINUE';
    """

    context.log.info(f"Executing COPY INTO for table: {raw_table} from stage: {stage_name}")

    with snowflake.get_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute(copy_query)
            query_id = cursor.sfqid
            context.log.info(f"Successfully triggered COPY INTO. Snowflake Query ID: {query_id}")

    return Output(
        value=None,
        metadata={
            "raw_table": raw_table,
            "stage_name": stage_name,
            "match_by_column_name": "CASE_INSENSITIVE",
        },
    )
