with source as (
    select * from {{ source('raw_data', 'raw_partsupp') }}
),

renamed as (
    select
        cast("ps_partkey" as number(38, 0)) as part_id,
        cast("ps_suppkey" as number(38, 0)) as supplier_id,
        cast("ps_availqty" as number(38, 0)) as available_quantity,
        cast("ps_supplycost" as number(15, 2)) as supply_cost,
        cast("ps_comment" as varchar) as comment
    from source
)

select * from renamed
