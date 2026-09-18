with customers as (
    select customer_id
    from "ucando"."dbt"."int_customer_profile"

),
activity as (
    select
        c.customer_id,
        t.last_transaction_at,
        b.last_behavior_at,
        m.last_marketing_event_at,
        cr.latest_credit_event_at,
        greatest(
            t.last_transaction_at,
            b.last_behavior_at,
            m.last_marketing_event_at,
            cr.latest_credit_event_at
        ) as last_activity_at
    from customers c
    left join "ucando"."dbt"."int_customer_transactions" t
        on c.customer_id = t.customer_id
    left join "ucando"."dbt"."int_customer_behavior" b
        on c.customer_id = b.customer_id
    left join "ucando"."dbt"."int_customer_marketing" m
        on c.customer_id = m.customer_id
    left join "ucando"."dbt"."int_customer_credit" cr
        on c.customer_id = cr.customer_id
)

select *
from activity