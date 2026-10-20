with supplier as (
    select * from {{ ref('stg_tpch__supplier') }}
),

nation as (
    select * from {{ ref('stg_tpch__nation') }}
),

region as (
    select * from {{ ref('stg_tpch__region') }}
),

final as (
    select
        {{ dbt_utils.generate_surrogate_key(['supplier.supplier_id']) }} as supplier_sk,
        supplier.supplier_id,
        supplier.supplier_name,
        supplier.supplier_address,
        supplier.nation_id,
        nation.nation_name,
        nation.region_id,
        region.region_name,
        supplier.phone_number,
        supplier.account_balance,
        supplier.comment
    from supplier
    left join nation
        on supplier.nation_id = nation.nation_id
    left join region
        on nation.region_id = region.region_id
)

select * from final
