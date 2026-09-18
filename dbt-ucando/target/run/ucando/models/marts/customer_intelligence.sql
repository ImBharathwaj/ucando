
  
    

  create  table "ucando"."dbt"."customer_intelligence__dbt_tmp"
  
  
    as
  
  (
    

select
    c.customer_id,
    c.customer_phone,
    c.total_transactions,
    c.total_transaction_amount,
    c.avg_transaction_amount,
    c.transaction_count_30d,
    c.transaction_amount_30d,
    c.transaction_count_90d,
    c.transaction_amount_90d,
    c.distinct_merchants,
    c.distinct_categories,
    c.distinct_channels,
    c.first_transaction_at,
    c.last_transaction_at,
    c.behavior_event_count,
    c.distinct_sessions,
    c.distinct_devices,
    c.distinct_pages,
    c.distinct_products,
    c.behavior_distinct_channels,
    c.first_behavior_at,
    c.last_behavior_at,
    c.loan_count,
    c.active_loan_count,
    c.total_loan_amount,
    c.insurance_policy_count,
    c.active_policy_count,
    c.total_premium_amount,
    c.has_loan,
    c.has_insurance,
    c.multi_product_customer,
    c.latest_credit_score,
    c.previous_credit_score,
    c.credit_score_change,
    c.latest_enquiry_count,
    c.credit_event_count,
    c.latest_credit_event_at,
    c.marketing_event_count,
    c.distinct_campaigns,
    c.distinct_marketing_channels,
    c.campaigns_sent,
    c.campaigns_delivered,
    c.campaigns_opened,
    c.campaigns_clicked,
    c.campaigns_converted,
    c.leads_created,
    c.leads_accepted,
    c.leads_rejected,
    c.first_marketing_event_at,
    c.last_marketing_event_at,
    c.last_activity_at,
    s.segment_id,
    s.segment_name,
    s.model_name as segmentation_model,
    s.model_version as segmentation_model_version,
    lp.propensity_score as loan_propensity_score,
    lp.prediction as loan_propensity_prediction,
    lp.model_name as propensity_model,
    lp.model_version as propensity_model_version,
    lp.prediction_timestamp as propensity_prediction_at
from "ucando"."dbt"."customer_360" c
left join "ucando"."public"."customer_segments" s
on c.customer_id = s.customer_id
left join "ucando"."public"."customer_loan_propensity" lp
on c.customer_id = lp.customer_id
  );
  