with source as (
    select * from {{ source('raw_data', 'raw_supplier') }}
),

renamed as (
    select
        cast("s_suppkey" as number(38, 0)) as supplier_id,
        cast("s_name" as varchar) as supplier_name,
        cast("s_address" as varchar) as supplier_address,
        cast("s_nationkey" as number(38, 0)) as nation_id,
        cast("s_phone" as varchar) as phone_number,
        cast("s_acctbal" as number(15, 2)) as account_balance,
        cast("s_comment" as varchar) as comment
    from source
)

select * from renamed
