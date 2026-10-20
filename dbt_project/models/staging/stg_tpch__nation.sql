with source as (
    select * from {{ source('raw_data', 'raw_nation') }}
),

renamed as (
    select
        cast("n_nationkey" as number(38, 0)) as nation_id,
        cast("n_name" as varchar) as nation_name,
        cast("n_regionkey" as number(38, 0)) as region_id,
        cast("n_comment" as varchar) as comment
    from source
)

select * from renamed
