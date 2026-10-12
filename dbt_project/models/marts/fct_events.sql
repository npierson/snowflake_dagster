with events as (
    select * from {{ ref('int_events__deduped') }}
),

users as (
    select * from {{ ref('dim_users') }}
),

final as (
    select
        {{ dbt_utils.generate_surrogate_key(['events.event_id']) }} as event_sk,
        events.event_id,
        coalesce(users.user_sk, {{ dbt_utils.generate_surrogate_key(["'-1'"]) }}) as user_sk,
        events.event_type,
        events.event_timestamp,
        events.raw_payload
    from events
    left join users
        on events.user_id = users.user_id
)

select * from final

