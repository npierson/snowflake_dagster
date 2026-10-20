with source as (
    select * from {{ source('raw_data', 'raw_lineitem') }}
),

renamed as (
    select
        cast("l_orderkey" as number(38, 0)) as order_id,
        cast("l_partkey" as number(38, 0)) as part_id,
        cast("l_suppkey" as number(38, 0)) as supplier_id,
        cast("l_linenumber" as number(38, 0)) as line_number,
        cast("l_quantity" as number(15, 2)) as quantity,
        cast("l_extendedprice" as number(15, 2)) as extended_price,
        cast("l_discount" as number(15, 2)) as discount_percentage,
        cast("l_tax" as number(15, 2)) as tax_rate,
        cast("l_returnflag" as varchar) as return_flag,
        cast("l_linestatus" as varchar) as line_status,
        cast("l_shipdate" as date) as ship_date,
        cast("l_commitdate" as date) as commit_date,
        cast("l_receiptdate" as date) as receipt_date,
        cast("l_shipinstruct" as varchar) as ship_instructions,
        cast("l_shipmode" as varchar) as ship_mode,
        cast("l_comment" as varchar) as comment
    from source
)

select * from renamed
