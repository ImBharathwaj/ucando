with credit_events as (

    select
        customer_id,
        credit_score,
        enquiry_count,
        event_time,
        row_number() over (
            partition by customer_id
            order by event_time desc
        ) as rn
    from {{ ref('stg_credit') }}
),
latest as (
    select
        customer_id,
        credit_score as latest_credit_score,
        enquiry_count as latest_enquiry_count,
        event_time as latest_credit_event_at
    from credit_events
    where rn = 1
),
previous as (
    select
        customer_id,
        credit_score as previous_credit_score
    from credit_events
    where rn = 2
),

credit_summary as (
    select
        customer_id,
        count(*) as credit_event_count
    from {{ ref('stg_credit') }}
    group by customer_id
)

select
    l.customer_id,
    l.latest_credit_score,
    p.previous_credit_score,
    case
        when p.previous_credit_score is not null
        then l.latest_credit_score - p.previous_credit_score
        else null
    end as credit_score_change,
    l.latest_enquiry_count,
    l.latest_credit_event_at,
    coalesce(s.credit_event_count, 0) as credit_event_count
from latest l
left join previous p
    on l.customer_id = p.customer_id
left join credit_summary s
    on l.customer_id = s.customer_id