with orders as (
    select * from {{ ref('stg_tpch__orders') }}
),

order_items as (
    select * from {{ ref('int_order_items__joined') }}
),

customers as (
    select customer_id, customer_sk from {{ ref('dim_customers') }}
),

order_metrics as (
    select
        order_id,
        count(line_number) as line_item_count,
        sum(quantity) as total_quantity,
        sum(extended_price) as gross_item_sales_amount,
        sum(item_discount_amount) as total_discount_amount,
        sum(item_tax_amount) as total_tax_amount,
        sum(net_amount) as net_item_sales_amount
    from order_items
    group by order_id
),

final as (
    select
        -- Primary Surrogate Key
        {{ dbt_utils.generate_surrogate_key(['orders.order_id']) }} as order_sk,

        -- Foreign Surrogate Key
        coalesce(customers.customer_sk, {{ dbt_utils.generate_surrogate_key(["'-1'"]) }}) as customer_sk,

        -- Natural Keys & Degenerate Dimensions
        orders.order_id,
        orders.customer_id,
        orders.order_status,
        orders.order_priority,
        orders.clerk_name,
        orders.ship_priority,
        orders.comment,

        -- Dates
        orders.order_date,

        -- Order Level Measures
        orders.total_price as order_total_price,
        coalesce(order_metrics.line_item_count, 0) as line_item_count,
        coalesce(order_metrics.total_quantity, 0) as total_quantity,
        coalesce(order_metrics.gross_item_sales_amount, 0) as gross_item_sales_amount,
        coalesce(order_metrics.total_discount_amount, 0) as total_discount_amount,
        coalesce(order_metrics.total_tax_amount, 0) as total_tax_amount,
        coalesce(order_metrics.net_item_sales_amount, 0) as net_item_sales_amount

    from orders
    left join customers
        on orders.customer_id = customers.customer_id
    left join order_metrics
        on orders.order_id = order_metrics.order_id
)

select * from final
