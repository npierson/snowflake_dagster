with deduped_events as (
    select * from {{ ref('int_events__deduped') }}
),

distinct_users as (
    select distinct
        user_id,
        min(event_timestamp) as first_seen_at,
        max(event_timestamp) as last_seen_at
    from deduped_events
    where user_id is not null
    group by user_id
),

final as (
    select
        {{ dbt_utils.generate_surrogate_key(['user_id']) }} as user_sk,
        user_id,
        first_seen_at,
        last_seen_at
    from distinct_users
)

select * from final

