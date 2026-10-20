with customer as (
    select * from {{ ref('stg_tpch__customer') }}
),

nation as (
    select * from {{ ref('stg_tpch__nation') }}
),

region as (
    select * from {{ ref('stg_tpch__region') }}
),

final as (
    select
        {{ dbt_utils.generate_surrogate_key(['customer.customer_id']) }} as customer_sk,
        customer.customer_id,
        customer.customer_name,
        customer.customer_address,
        customer.phone_number,
        customer.account_balance,
        customer.market_segment,
        customer.nation_id,
        nation.nation_name,
        nation.region_id,
        region.region_name,
        customer.comment
    from customer
    left join nation
        on customer.nation_id = nation.nation_id
    left join region
        on nation.region_id = region.region_id
)

select * from final
