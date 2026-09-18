with loans as (
    select
        customer_id,
        count(*) as loan_count,
        count(*) filter (
            where status = 'ACTIVE'
        ) as active_loan_count,
        coalesce(sum(amount), 0) as total_loan_amount
    from "ucando"."dbt_stage"."stg_loans"
    group by customer_id
),

insurance as (
    select
        customer_id,
        count(*) as insurance_policy_count,
        count(*) as active_policy_count,
        coalesce(sum(premium_amount), 0) as total_premium_amount
    from "ucando"."dbt_stage"."stg_insurance"
    group by customer_id
),

customers as (
    select distinct customer_id
    from (
        select customer_id from loans
        union
        select customer_id from insurance
    ) x
)

select
    c.customer_id,
    coalesce(l.loan_count, 0) as loan_count,
    coalesce(l.active_loan_count, 0) as active_loan_count,
    coalesce(l.total_loan_amount, 0) as total_loan_amount,
    coalesce(i.insurance_policy_count, 0) as insurance_policy_count,
    coalesce(i.active_policy_count, 0) as active_policy_count,
    coalesce(i.total_premium_amount, 0) as total_premium_amount,
    case
        when coalesce(l.loan_count, 0) > 0
        then true
        else false
    end as has_loan,
    case
        when coalesce(i.insurance_policy_count, 0) > 0
        then true
        else false
    end as has_insurance,
    case
        when coalesce(l.loan_count, 0) > 0
         and coalesce(i.insurance_policy_count, 0) > 0
        then true
        else false
    end as multi_product_customer
from customers c
left join loans l
on c.customer_id = l.customer_id
left join insurance i
on c.customer_id = i.customer_id