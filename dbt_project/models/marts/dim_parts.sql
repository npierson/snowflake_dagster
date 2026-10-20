with part as (
    select * from {{ ref('stg_tpch__part') }}
),

final as (
    select
        {{ dbt_utils.generate_surrogate_key(['part.part_id']) }} as part_sk,
        part.part_id,
        part.part_name,
        part.manufacturer,
        part.brand,
        part.part_type,
        part.part_size,
        part.container,
        part.retail_price,
        part.comment
    from part
)

select * from final
