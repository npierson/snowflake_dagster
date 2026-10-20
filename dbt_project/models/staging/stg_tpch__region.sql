with source as (
    select * from {{ source('raw_data', 'raw_region') }}
),

renamed as (
    select
        cast("r_regionkey" as number(38, 0)) as region_id,
        cast("r_name" as varchar) as region_name,
        cast("r_comment" as varchar) as comment
    from source
)

select * from renamed
