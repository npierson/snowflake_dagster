with source as (
    select * from {{ source('raw_data', 'raw_part') }}
),

renamed as (
    select
        cast("p_partkey" as number(38, 0)) as part_id,
        cast("p_name" as varchar) as part_name,
        cast("p_mfgr" as varchar) as manufacturer,
        cast("p_brand" as varchar) as brand,
        cast("p_type" as varchar) as part_type,
        cast("p_size" as number(38, 0)) as part_size,
        cast("p_container" as varchar) as container,
        cast("p_retailprice" as number(15, 2)) as retail_price,
        cast("p_comment" as varchar) as comment
    from source
)

select * from renamed
