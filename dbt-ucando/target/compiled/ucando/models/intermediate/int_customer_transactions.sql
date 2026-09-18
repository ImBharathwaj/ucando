with rolling_calc as (
    select
    customer_id,
    count(transaction_id) over(
        partition by customer_id
        order by event_date 
        range between interval '29 days' 
        PRECEDING AND CURRENT ROW
    ) as transaction_count_30d,
    sum(amount) over(
        partition by customer_id
        order by event_date 
        range between interval '29 days' 
        PRECEDING AND CURRENT ROW
    ) as transaction_amount_30d,
    count(transaction_id) over(
        partition by customer_id
        order by event_date 
        range between interval '89 days' 
        PRECEDING AND CURRENT ROW
    ) as transaction_count_90d,
    sum(amount) over(
        partition by customer_id
        order by event_date 
        range between interval '89 days' 
        PRECEDING AND CURRENT ROW
    ) as transaction_amount_90d
    from "ucando"."dbt_stage"."stg_transactions"
)
, other_agg as (
    select 
        customer_id,
        count(1) as total_transactions,
        coalesce(sum(amount),0) as total_transaction_amount,
        coalesce(avg(amount),0) as avg_transaction_amount,
        coalesce(max(amount),0) as max_transaction_amount,
        coalesce(min(amount),0) as min_transaction_amount,
        coalesce(count(distinct merchant_id),0) as distinct_merchants,
        coalesce(count(distinct merchant_category),0) as distinct_categories,
        coalesce(count(distinct channel),0) as distinct_channels,
        min(event_time) as first_transaction_at,
        max(event_time) as last_transaction_at
    from "ucando"."dbt_stage"."stg_transactions"
    group by customer_id
)
select distinct
    a.*, 
    b.transaction_count_30d,
    b.transaction_amount_30d,
    b.transaction_count_90d,
    b.transaction_amount_90d
from other_agg a
join rolling_calc b 
on a.customer_id = b.customer_id