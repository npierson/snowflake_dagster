with source_data as (
    select * from {{ source('raw_data', 'raw_s3_parquet_ingest') }}
),

renamed as (
    select
        cast(event_id as varchar) as event_id,
        cast(user_id as varchar) as user_id,
        cast(event_type as varchar) as event_type,
        cast(event_timestamp as timestamp_ntz) as event_timestamp,
        cast(payload as variant) as raw_payload
    from source_data
)

select * from renamed

