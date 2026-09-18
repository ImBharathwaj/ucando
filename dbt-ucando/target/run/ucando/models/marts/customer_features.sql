
  
    

  create  table "ucando"."dbt"."customer_features__dbt_tmp"
  
  
    as
  
  (
    

select
    customer_id,
    -- TRANSACTION FEATURES
    coalesce(total_transactions, 0) as total_transactions,
    coalesce(total_transaction_amount, 0) as total_transaction_amount,
    coalesce(avg_transaction_amount, 0) as avg_transaction_amount,
    coalesce(max_transaction_amount, 0) as max_transaction_amount,
    coalesce(min_transaction_amount, 0) as min_transaction_amount,
    coalesce(distinct_merchants, 0) as distinct_merchants,
    coalesce(distinct_categories, 0) as distinct_transaction_categories,
    coalesce(distinct_channels, 0) as distinct_transaction_channels,
    coalesce(transaction_count_30d, 0) as transaction_count_30d,
    coalesce(transaction_amount_30d, 0) as transaction_amount_30d,
    coalesce(transaction_count_90d, 0) as transaction_count_90d,
    coalesce(transaction_amount_90d, 0) as transaction_amount_90d,
    -- Transaction intensity
    case
        when coalesce(transaction_count_30d, 0) > 0
        then transaction_amount_30d / transaction_count_30d
        else 0
    end as avg_transaction_amount_30d,
    case
        when coalesce(transaction_count_90d, 0) > 0
        then transaction_amount_90d / transaction_count_90d
        else 0
    end as avg_transaction_amount_90d,
    -- BEHAVIOR / ENGAGEMENT FEATURES
    coalesce(behavior_event_count, 0) as behavior_event_count,
    coalesce(distinct_sessions, 0) as distinct_sessions,
    coalesce(distinct_devices, 0) as distinct_devices,
    coalesce(distinct_pages, 0) as distinct_pages,
    coalesce(distinct_products, 0) as distinct_products,
    coalesce(behavior_distinct_channels, 0) as distinct_behavior_channels,
    -- PRODUCT FEATURES
    coalesce(loan_count, 0) as loan_count,
    coalesce(active_loan_count, 0) as active_loan_count,
    coalesce(total_loan_amount, 0) as total_loan_amount,
    coalesce(insurance_policy_count, 0) as insurance_policy_count,
    coalesce(active_policy_count, 0) as active_policy_count,
    coalesce(total_premium_amount, 0) as total_premium_amount,
    coalesce(has_loan, false) as has_loan,
    coalesce(has_insurance, false) as has_insurance,
    coalesce(multi_product_customer, false) as multi_product_customer,
    -- CREDIT FEATURES
    coalesce(latest_credit_score, 0) as latest_credit_score,
    coalesce(previous_credit_score, 0) as previous_credit_score,
    coalesce(credit_score_change, 0) as credit_score_change,
    coalesce(latest_enquiry_count, 0) as latest_enquiry_count,
    coalesce(credit_event_count, 0) as credit_event_count,
    -- MARKETING FEATURES
    coalesce(marketing_event_count, 0) as marketing_event_count,
    coalesce(distinct_campaigns, 0) as distinct_campaigns,
    coalesce(distinct_marketing_channels, 0)
        as distinct_marketing_channels,
    coalesce(campaigns_sent, 0) as campaigns_sent,
    coalesce(campaigns_delivered, 0) as campaigns_delivered,
    coalesce(campaigns_opened, 0) as campaigns_opened,
    coalesce(campaigns_clicked, 0) as campaigns_clicked,
    coalesce(campaigns_converted, 0) as campaigns_converted,
    coalesce(campaigns_bounced, 0) as campaigns_bounced,
    coalesce(campaigns_unsubscribed, 0) as campaigns_unsubscribed,
    coalesce(leads_created, 0) as leads_created,
    coalesce(leads_accepted, 0) as leads_accepted,
    coalesce(leads_rejected, 0) as leads_rejected,
    -- MARKETING RESPONSE FEATURES
    case
        when coalesce(campaigns_sent, 0) > 0
        then campaigns_opened::numeric / campaigns_sent
        else 0
    end as marketing_open_rate,
    case
        when coalesce(campaigns_sent, 0) > 0
        then campaigns_clicked::numeric / campaigns_sent
        else 0
    end as marketing_click_rate,
    case
        when coalesce(campaigns_sent, 0) > 0
        then campaigns_converted::numeric / campaigns_sent
        else 0
    end as marketing_conversion_rate,
    case
        when coalesce(leads_created, 0) > 0
        then leads_accepted::numeric / leads_created
        else 0
    end as lead_acceptance_rate,
    -- CUSTOMER ACTIVITY
    first_transaction_at,
    last_transaction_at,
    first_behavior_at,
    last_behavior_at,
    latest_credit_event_at,
    first_marketing_event_at,
    last_marketing_event_at,
    last_activity_at
from "ucando"."dbt"."customer_360"
  );
  