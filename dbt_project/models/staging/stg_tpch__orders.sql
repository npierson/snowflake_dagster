with source as (
    select * from {{ source('raw_data', 'raw_orders') }}
),

renamed as (
    select
        cast("o_orderkey" as number(38, 0)) as order_id,
        cast("o_custkey" as number(38, 0)) as customer_id,
        cast("o_orderstatus" as varchar) as order_status,
        cast("o_totalprice" as number(15, 2)) as total_price,
        cast("o_orderdate" as date) as order_date,
        cast("o_orderpriority" as varchar) as order_priority,
        cast("o_clerk" as varchar) as clerk_name,
        cast("o_shippriority" as number(38, 0)) as ship_priority,
        cast("o_comment" as varchar) as comment
    from source
)

select * from renamed
