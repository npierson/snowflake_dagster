with order_items as (
    select * from {{ ref('int_order_items__joined') }}
),

customers as (
    select customer_id, customer_sk from {{ ref('dim_customers') }}
),

parts as (
    select part_id, part_sk from {{ ref('dim_parts') }}
),

suppliers as (
    select supplier_id, supplier_sk from {{ ref('dim_suppliers') }}
),

final as (
    select
        -- Primary Surrogate Key
        {{ dbt_utils.generate_surrogate_key(['order_items.order_id', 'order_items.line_number']) }} as order_item_sk,

        -- Foreign Surrogate Keys
        {{ dbt_utils.generate_surrogate_key(['order_items.order_id']) }} as order_sk,
        coalesce(customers.customer_sk, {{ dbt_utils.generate_surrogate_key(["'-1'"]) }}) as customer_sk,
        coalesce(parts.part_sk, {{ dbt_utils.generate_surrogate_key(["'-1'"]) }}) as part_sk,
        coalesce(suppliers.supplier_sk, {{ dbt_utils.generate_surrogate_key(["'-1'"]) }}) as supplier_sk,

        -- Natural Keys & Degenerate Dimensions
        order_items.order_id,
        order_items.line_number,
        order_items.order_status,
        order_items.order_priority,
        order_items.return_flag,
        order_items.line_status,
        order_items.ship_mode,
        order_items.ship_instructions,

        -- Dates
        order_items.order_date,
        order_items.ship_date,
        order_items.commit_date,
        order_items.receipt_date,

        -- Measures
        order_items.quantity,
        order_items.extended_price,
        order_items.discount_percentage,
        order_items.tax_rate,
        order_items.item_discount_amount,
        order_items.discounted_price,
        order_items.item_tax_amount,
        order_items.net_amount

    from order_items
    left join customers
        on order_items.customer_id = customers.customer_id
    left join parts
        on order_items.part_id = parts.part_id
    left join suppliers
        on order_items.supplier_id = suppliers.supplier_id
)

select * from final
