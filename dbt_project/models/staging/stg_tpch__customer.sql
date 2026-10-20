with source as (
    select * from {{ source('raw_data', 'raw_customer') }}
),

renamed as (
    select
        cast("c_custkey" as number(38, 0)) as customer_id,
        cast("c_name" as varchar) as customer_name,
        cast("c_address" as varchar) as customer_address,
        cast("c_nationkey" as number(38, 0)) as nation_id,
        cast("c_phone" as varchar) as phone_number,
        cast("c_acctbal" as number(15, 2)) as account_balance,
        cast("c_mktsegment" as varchar) as market_segment,
        cast("c_comment" as varchar) as comment
    from source
)

select * from renamed
