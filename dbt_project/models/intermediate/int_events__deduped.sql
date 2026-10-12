with staging as (
    select * from {{ ref('stg_events') }}
),

deduped as (
    select
        event_id,
        user_id,
        event_type,
        event_timestamp,
        raw_payload,
        row_number() over (
            partition by event_id 
            order by event_timestamp desc
        ) as row_num
    from staging
)

select
    event_id,
    user_id,
    event_type,
    event_timestamp,
    raw_payload
from deduped
where row_num = 1

