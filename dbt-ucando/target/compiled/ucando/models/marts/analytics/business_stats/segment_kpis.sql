

SELECT
    segment_id,
    segment_name,
    customer_count,
    ROUND(
        avg_transaction_amount::numeric,
        2
    ) AS avg_transaction_amount,
    ROUND(
        avg_credit_score::numeric,
        2
    ) AS avg_credit_score,
    ROUND(
        avg_loan_propensity_v2::numeric,
        4
    ) AS avg_loan_propensity,
    ROUND(
        avg_behavior_events::numeric,
        2
    ) AS avg_behavior_events,
    customers_with_loan,
    customers_with_insurance,
    multi_product_customers
FROM "ucando"."dbt_analytics"."analytics_segment_summary"
ORDER BY customer_count DESC