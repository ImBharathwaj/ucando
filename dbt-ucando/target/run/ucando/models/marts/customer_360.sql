
  
    

  create  table "ucando"."dbt"."customer_360__dbt_tmp"
  
  
    as
  
  (
    select
    p.customer_id,
    p.customer_phone,
    p.profile_updated_at,
    p.source_system,
    t.total_transactions,
    t.total_transaction_amount,
    t.avg_transaction_amount,
    t.max_transaction_amount,
    t.min_transaction_amount,
    t.distinct_merchants,
    t.distinct_categories,
    t.distinct_channels,
    t.first_transaction_at,
    t.last_transaction_at,
    t.transaction_count_30d,
    t.transaction_amount_30d,
    t.transaction_count_90d,
    t.transaction_amount_90d,
    b.behavior_event_count,
    b.distinct_sessions,
    b.distinct_devices,
    b.distinct_pages,
    b.distinct_products,
    b.distinct_channels as behavior_distinct_channels,
    b.first_behavior_at,
    b.last_behavior_at,
    pr.loan_count,
    pr.active_loan_count,
    pr.total_loan_amount,
    pr.insurance_policy_count,
    pr.active_policy_count,
    pr.total_premium_amount,
    pr.has_loan,
    pr.has_insurance,
    pr.multi_product_customer,
    cr.latest_credit_score,
    cr.previous_credit_score,
    cr.credit_score_change,
    cr.latest_enquiry_count,
    cr.latest_credit_event_at,
    cr.credit_event_count,
    m.marketing_event_count,
    m.distinct_campaigns,
    m.distinct_marketing_channels,
    m.campaigns_sent,
    m.campaigns_delivered,
    m.campaigns_opened,
    m.campaigns_clicked,
    m.campaigns_converted,
    m.campaigns_bounced,
    m.campaigns_unsubscribed,
    m.leads_created,
    m.leads_accepted,
    m.leads_rejected,
    m.first_marketing_event_at,
    m.last_marketing_event_at,
    a.last_activity_at
from "ucando"."dbt"."int_customer_profile" p
left join "ucando"."dbt"."int_customer_transactions" t
    on p.customer_id = t.customer_id
left join "ucando"."dbt"."int_customer_behavior" b
    on p.customer_id = b.customer_id
left join "ucando"."dbt"."int_customer_products" pr
    on p.customer_id = pr.customer_id
left join "ucando"."dbt"."int_customer_credit" cr
    on p.customer_id = cr.customer_id
left join "ucando"."dbt"."int_customer_marketing" m
    on p.customer_id = m.customer_id
left join "ucando"."dbt"."int_customer_activity" a
    on p.customer_id = a.customer_id
  );
  