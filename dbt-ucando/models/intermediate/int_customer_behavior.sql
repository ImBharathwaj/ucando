with behavior_agg as (
    select
        customer_id,
        count(*) as behavior_event_count,
        count(distinct session_id) as distinct_sessions,
        count(distinct device_id) as distinct_devices,
        count(distinct page) as distinct_pages,
        count(distinct product) as distinct_products,
        count(distinct channel) as distinct_channels,
        min(event_time) as first_behavior_at,
        max(event_time) as last_behavior_at
    from {{ ref('stg_behavior') }}
    group by customer_id
)

select
    customer_id,
    behavior_event_count,
    distinct_sessions,
    distinct_devices,
    distinct_pages,
    distinct_products,
    distinct_channels,
    first_behavior_at,
    last_behavior_at
from behavior_agg