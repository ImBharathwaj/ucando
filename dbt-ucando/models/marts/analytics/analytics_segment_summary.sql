{{ config(
    materialized='table',
    schema='analytics'
) }}

SELECT
    segment_id,
    segment_name,
    COUNT(*) AS customer_count,
    AVG(total_transaction_amount) AS avg_transaction_amount,
    AVG(avg_transaction_amount) AS avg_transaction_value,
    AVG(transaction_count_30d) AS avg_transaction_count_30d,
    AVG(transaction_amount_90d) AS avg_transaction_amount_90d,
    AVG(behavior_event_count) AS avg_behavior_events,
    AVG(distinct_sessions) AS avg_sessions,
    AVG(distinct_devices) AS avg_devices,
    AVG(loan_count) AS avg_loan_count,
    AVG(insurance_policy_count) AS avg_insurance_policy_count,
    SUM(
        CASE
            WHEN has_loan THEN 1
            ELSE 0
        END
    ) AS customers_with_loan,
    SUM(
        CASE
            WHEN has_insurance THEN 1
            ELSE 0
        END
    ) AS customers_with_insurance,
    SUM(
        CASE
            WHEN multi_product_customer THEN 1
            ELSE 0
        END
    ) AS multi_product_customers,
    AVG(latest_credit_score) AS avg_credit_score,
    AVG(credit_score_change) AS avg_credit_score_change,
    AVG(marketing_event_count) AS avg_marketing_events,
    AVG(campaigns_sent) AS avg_campaigns_sent,
    AVG(campaigns_opened) AS avg_campaigns_opened,
    AVG(campaigns_clicked) AS avg_campaigns_clicked,
    AVG(campaigns_converted) AS avg_campaigns_converted,
    AVG(loan_propensity_score_v1) AS avg_loan_propensity_v1,
    AVG(loan_propensity_score_v2) AS avg_loan_propensity_v2,
    AVG(graph_relationship_count) AS avg_graph_relationships,
    MAX(last_activity_at) AS latest_segment_activity
FROM {{ ref('customer_intelligence_enriched') }}
GROUP BY
    segment_id,
    segment_name