
  
    

  create  table "ucando"."dbt_analytics"."analytics_customer_overview__dbt_tmp"
  
  
    as
  
  (
    

select customer_id,
    segment_id,
    segment_name,
    total_transactions,
    total_transaction_amount,
    avg_transaction_amount,
    distinct_merchants,
    distinct_categories,
    distinct_channels,
    transaction_count_30d,
    transaction_amount_30d,
    transaction_count_90d,
    transaction_amount_90d,
    behavior_event_count,
    distinct_sessions,
    distinct_devices,
    distinct_pages,
    distinct_products,
    loan_count,
    active_loan_count,
    insurance_policy_count,
    active_policy_count,
    has_loan,
    has_insurance,
    multi_product_customer,
    latest_credit_score,
    credit_score_change,
    latest_enquiry_count,
    marketing_event_count,
    campaigns_sent,
    campaigns_delivered,
    campaigns_opened,
    campaigns_clicked,
    campaigns_converted,
    leads_created,
    leads_accepted,
    loan_propensity_score_v1,
    loan_propensity_prediction_v1,
    loan_propensity_score_v2,
    loan_propensity_prediction_v2,
    graph_account_count,
    graph_transaction_count,
    graph_device_count,
    graph_insurance_count,
    graph_campaign_count,
    graph_relationship_count,
    last_transaction_at,
    last_behavior_at,
    last_marketing_event_at,
    last_activity_at
FROM "ucando"."dbt"."customer_intelligence_enriched"
  );
  