import os
from dagster import AssetExecutionContext, MetadataValue, Output, asset
from dagster_snowflake import SnowflakeResource


def _ingest_parquet_table(
    context: AssetExecutionContext,
    snowflake: SnowflakeResource,
    table_name: str,
    file_pattern: str,
) -> Output[None]:
    """Helper function to execute an idempotent COPY INTO operation for a Parquet dataset."""
    stage_name = os.getenv("SNOWFLAKE_STAGE_NAME", "@S3_PARQUET_STAGE")
    stage_path = f"{stage_name}/{file_pattern}"
    raw_table = f"RAW_{table_name.upper()}"

    copy_query = f"""
    COPY INTO {raw_table}
    FROM {stage_path}
    FILE_FORMAT = (FORMAT_NAME = 'PARQUET_FORMAT')
    MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
    ON_ERROR = 'CONTINUE';
    """

    context.log.info(f"Executing COPY INTO target table '{raw_table}' from '{stage_path}'")

    rows_loaded = 0
    files_processed = 0
    query_id = None

    with snowflake.get_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute(copy_query)
            query_id = cursor.sfqid
            results = cursor.fetchall()
            context.log.info(f"Executed COPY INTO for {raw_table}. Query ID: {query_id}")

            if results:
                for row in results:
                    # When no files need loading, Snowflake returns a single-item tuple: ('Copy executed with 0 files processed.',)
                    if len(row) == 1 and "0 files processed" in str(row[0]):
                        context.log.info(f"Stage up to date for {raw_table}: {row[0]}")
                        continue

                    file_name = row[0] if len(row) > 0 else "unknown"
                    status = row[1] if len(row) > 1 else "UNKNOWN"
                    loaded_count = row[3] if len(row) > 3 and isinstance(row[3], int) else 0
                    rows_loaded += loaded_count
                    files_processed += 1
                    context.log.info(f"Loaded {file_name}: {status} ({loaded_count} rows)")
                context.log.info(
                    f"COPY INTO summary for {raw_table}: {files_processed} file(s) processed, {rows_loaded} rows loaded."
                )
            else:
                context.log.info(f"No new files to load for {raw_table} (stage is up to date or empty).")

            cursor.execute(f"SELECT COUNT(*) FROM {raw_table};")
            total_rows = cursor.fetchone()[0]

    return Output(
        value=None,
        metadata={
            "raw_table": MetadataValue.text(raw_table),
            "stage_path": MetadataValue.text(stage_path),
            "query_id": MetadataValue.text(query_id or "N/A"),
            "files_processed": MetadataValue.int(files_processed),
            "rows_loaded": MetadataValue.int(rows_loaded),
            "total_rows": MetadataValue.int(total_rows),
            "match_by_column_name": MetadataValue.text("CASE_INSENSITIVE"),
        },
    )


@asset(
    key=["raw_data", "raw_customer"],
    group_name="raw_ingestion",
    compute_kind="snowflake",
    description="Loads customer.parquet from S3 Parquet stage into Snowflake RAW_CUSTOMER table",
)
def raw_customer(context: AssetExecutionContext, snowflake: SnowflakeResource) -> Output[None]:
    return _ingest_parquet_table(context, snowflake, "customer", "customer.parquet")


@asset(
    key=["raw_data", "raw_lineitem"],
    group_name="raw_ingestion",
    compute_kind="snowflake",
    description="Loads lineitem.parquet from S3 Parquet stage into Snowflake RAW_LINEITEM table",
)
def raw_lineitem(context: AssetExecutionContext, snowflake: SnowflakeResource) -> Output[None]:
    return _ingest_parquet_table(context, snowflake, "lineitem", "lineitem.parquet")


@asset(
    key=["raw_data", "raw_nation"],
    group_name="raw_ingestion",
    compute_kind="snowflake",
    description="Loads nation.parquet from S3 Parquet stage into Snowflake RAW_NATION table",
)
def raw_nation(context: AssetExecutionContext, snowflake: SnowflakeResource) -> Output[None]:
    return _ingest_parquet_table(context, snowflake, "nation", "nation.parquet")


@asset(
    key=["raw_data", "raw_orders"],
    group_name="raw_ingestion",
    compute_kind="snowflake",
    description="Loads orders.parquet from S3 Parquet stage into Snowflake RAW_ORDERS table",
)
def raw_orders(context: AssetExecutionContext, snowflake: SnowflakeResource) -> Output[None]:
    return _ingest_parquet_table(context, snowflake, "orders", "orders.parquet")


@asset(
    key=["raw_data", "raw_part"],
    group_name="raw_ingestion",
    compute_kind="snowflake",
    description="Loads part.parquet from S3 Parquet stage into Snowflake RAW_PART table",
)
def raw_part(context: AssetExecutionContext, snowflake: SnowflakeResource) -> Output[None]:
    return _ingest_parquet_table(context, snowflake, "part", "part.parquet")


@asset(
    key=["raw_data", "raw_partsupp"],
    group_name="raw_ingestion",
    compute_kind="snowflake",
    description="Loads partsupp.parquet from S3 Parquet stage into Snowflake RAW_PARTSUPP table",
)
def raw_partsupp(context: AssetExecutionContext, snowflake: SnowflakeResource) -> Output[None]:
    return _ingest_parquet_table(context, snowflake, "partsupp", "partsupp.parquet")


@asset(
    key=["raw_data", "raw_region"],
    group_name="raw_ingestion",
    compute_kind="snowflake",
    description="Loads region.parquet from S3 Parquet stage into Snowflake RAW_REGION table",
)
def raw_region(context: AssetExecutionContext, snowflake: SnowflakeResource) -> Output[None]:
    return _ingest_parquet_table(context, snowflake, "region", "region.parquet")


@asset(
    key=["raw_data", "raw_supplier"],
    group_name="raw_ingestion",
    compute_kind="snowflake",
    description="Loads supplier.parquet from S3 Parquet stage into Snowflake RAW_SUPPLIER table",
)
def raw_supplier(context: AssetExecutionContext, snowflake: SnowflakeResource) -> Output[None]:
    return _ingest_parquet_table(context, snowflake, "supplier", "supplier.parquet")


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
    """Legacy/template asset for single-table RAW_EVENTS landing."""
    raw_table = os.getenv("SNOWFLAKE_RAW_TABLE", "RAW_EVENTS")
    stage_name = os.getenv("SNOWFLAKE_STAGE_NAME", "@S3_PARQUET_STAGE")

    copy_query = f"""
    COPY INTO {raw_table}
    FROM {stage_name}
    FILE_FORMAT = (FORMAT_NAME = 'PARQUET_FORMAT')
    MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
    ON_ERROR = 'CONTINUE';
    """

    context.log.info(f"Executing COPY INTO target table '{raw_table}' from stage '{stage_name}'")

    rows_loaded = 0
    files_processed = 0
    query_id = None

    with snowflake.get_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute(copy_query)
            query_id = cursor.sfqid
            results = cursor.fetchall()
            context.log.info(f"Executed COPY INTO. Snowflake Query ID: {query_id}")

            if results:
                for row in results:
                    if len(row) == 1 and "0 files processed" in str(row[0]):
                        context.log.info(f"Stage up to date for {raw_table}: {row[0]}")
                        continue
                    file_name = row[0] if len(row) > 0 else "unknown"
                    status = row[1] if len(row) > 1 else "UNKNOWN"
                    loaded_count = row[3] if len(row) > 3 and isinstance(row[3], int) else 0
                    rows_loaded += loaded_count
                    files_processed += 1
                    context.log.info(f"Loaded {file_name}: {status} ({loaded_count} rows)")
                context.log.info(f"COPY INTO summary: {files_processed} files processed, {rows_loaded} rows loaded.")
            else:
                context.log.info("No new files to load (stage is up to date or empty).")

    return Output(
        value=None,
        metadata={
            "raw_table": MetadataValue.text(raw_table),
            "stage_name": MetadataValue.text(stage_name),
            "query_id": MetadataValue.text(query_id or "N/A"),
            "files_processed": MetadataValue.int(files_processed),
            "rows_loaded": MetadataValue.int(rows_loaded),
            "match_by_column_name": MetadataValue.text("CASE_INSENSITIVE"),
        },
    )
