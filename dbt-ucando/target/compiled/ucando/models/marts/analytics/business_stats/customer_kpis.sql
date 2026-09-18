

SELECT
    COUNT(*) AS total_customers,
    COUNT(*) FILTER (
        WHERE last_activity_at >= CURRENT_DATE - INTERVAL '30 days'
    ) AS active_customers_30d,
    COUNT(*) FILTER (
        WHERE last_activity_at >= CURRENT_DATE - INTERVAL '90 days'
    ) AS active_customers_90d,
    ROUND(
        AVG(total_transaction_amount)::numeric,
        2
    ) AS avg_customer_transaction_value,
    ROUND(
        AVG(transaction_count_30d)::numeric,
        2
    ) AS avg_transactions_30d,
    ROUND(
        AVG(latest_credit_score)::numeric,
        2
    ) AS avg_credit_score,
    COUNT(*) FILTER (
        WHERE multi_product_customer = TRUE
    ) AS multi_product_customers,
    ROUND(
        AVG(loan_propensity_score_v2)::numeric,
        4
    ) AS avg_loan_propensity

FROM "ucando"."dbt_analytics"."analytics_customer_overview"