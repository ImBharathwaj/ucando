
  
    

  create  table "ucando"."dbt"."customer_intelligence_enriched__dbt_tmp"
  
  
    as
  
  (
    

select
    ci.customer_id,
    ci.customer_phone,
    ci.total_transactions,
    ci.total_transaction_amount,
    ci.avg_transaction_amount,
    ci.transaction_count_30d,
    ci.transaction_amount_30d,
    ci.transaction_count_90d,
    ci.transaction_amount_90d,
    ci.distinct_merchants,
    ci.distinct_categories,
    ci.distinct_channels,
    ci.first_transaction_at,
    ci.last_transaction_at,
    ci.behavior_event_count,
    ci.distinct_sessions,
    ci.distinct_devices,
    ci.distinct_pages,
    ci.distinct_products,
    ci.behavior_distinct_channels,
    ci.first_behavior_at,
    ci.last_behavior_at,
    ci.loan_count,
    ci.active_loan_count,
    ci.total_loan_amount,
    ci.insurance_policy_count,
    ci.active_policy_count,
    ci.total_premium_amount,
    ci.has_loan,
    ci.has_insurance,
    ci.multi_product_customer,
    ci.latest_credit_score,
    ci.previous_credit_score,
    ci.credit_score_change,
    ci.latest_enquiry_count,
    ci.credit_event_count,
    ci.latest_credit_event_at,
    ci.marketing_event_count,
    ci.distinct_campaigns,
    ci.distinct_marketing_channels,
    ci.campaigns_sent,
    ci.campaigns_delivered,
    ci.campaigns_opened,
    ci.campaigns_clicked,
    ci.campaigns_converted,
    -- ci.campaigns_bounced,
    -- ci.campaigns_unsubscribed,
    ci.leads_created,
    ci.leads_accepted,
    ci.leads_rejected,
    ci.first_marketing_event_at,
    ci.last_marketing_event_at,
    ci.last_activity_at,
    seg.segment_id,
    seg.segment_name,
    seg.model_name as segmentation_model,
    seg.model_version as segmentation_model_version,
    lp1.propensity_score as loan_propensity_score_v1,
    lp1.prediction as loan_propensity_prediction_v1,
    lp1.model_name as propensity_model_v1,
    lp1.model_version as propensity_model_version_v1,
    lp1.prediction_timestamp as propensity_prediction_at_v1,
    lp2.propensity_score as loan_propensity_score_v2,
    lp2.prediction as loan_propensity_prediction_v2,
    lp2.model_name as propensity_model_v2,
    lp2.model_version as propensity_model_version_v2,
    lp2.prediction_timestamp as propensity_prediction_at_v2,
    coalesce(gf.account_count, 0) as graph_account_count,
    coalesce(gf.graph_transaction_count, 0) as graph_transaction_count,
    coalesce(gf.connected_device_count, 0) as graph_device_count,
    coalesce(gf.graph_insurance_count, 0) as graph_insurance_count,
    coalesce(gf.connected_campaign_count, 0) as graph_campaign_count,
    coalesce(gf.graph_relationship_count, 0) as graph_relationship_count,
    coalesce(gf.connected_customer_count, 0) as graph_connected_customer_count,
    gf.graph_feature_timestamp
from "ucando"."dbt"."customer_intelligence" ci
left join "ucando"."public"."customer_segments" seg
on ci.customer_id = seg.customer_id
left join "ucando"."public"."customer_loan_propensity" lp1
on ci.customer_id = lp1.customer_id
left join "ucando"."public"."customer_loan_propensity_v2" lp2
on ci.customer_id = lp2.customer_id
left join "ucando"."public"."customer_graph_features" gf
on ci.customer_id = gf.customer_id
  );
  