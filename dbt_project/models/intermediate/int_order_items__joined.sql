with lineitem as (
    select * from {{ ref('stg_tpch__lineitem') }}
),

orders as (
    select * from {{ ref('stg_tpch__orders') }}
),

joined as (
    select
        -- Order & Item Keys
        lineitem.order_id,
        lineitem.line_number,
        orders.customer_id,
        lineitem.part_id,
        lineitem.supplier_id,

        -- Order Attributes
        orders.order_date,
        orders.order_status,
        orders.order_priority,
        orders.clerk_name,
        orders.ship_priority,

        -- Line Item Attributes
        lineitem.return_flag,
        lineitem.line_status,
        lineitem.ship_date,
        lineitem.commit_date,
        lineitem.receipt_date,
        lineitem.ship_instructions,
        lineitem.ship_mode,

        -- Quantitative Measures
        lineitem.quantity,
        lineitem.extended_price,
        lineitem.discount_percentage,
        lineitem.tax_rate,

        -- Calculated Metrics
        round(lineitem.extended_price * lineitem.discount_percentage, 2) as item_discount_amount,
        round(lineitem.extended_price * (1 - lineitem.discount_percentage), 2) as discounted_price,
        round(
            (lineitem.extended_price * (1 - lineitem.discount_percentage)) * lineitem.tax_rate,
            2
        ) as item_tax_amount,
        round(
            (lineitem.extended_price * (1 - lineitem.discount_percentage)) * (1 + lineitem.tax_rate),
            2
        ) as net_amount

    from lineitem
    inner join orders
        on lineitem.order_id = orders.order_id
)

select * from joined
