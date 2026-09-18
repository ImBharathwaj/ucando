
  create view "ucando"."dbt_analytics"."product_kpis__dbt_tmp"
    
    
  as (
    

SELECT
    COUNT(*) AS total_customers,
    COUNT(*) FILTER (
        WHERE has_loan = TRUE
    ) AS customers_with_loan,
    COUNT(*) FILTER (
        WHERE has_insurance = TRUE
    ) AS customers_with_insurance,
    COUNT(*) FILTER (
        WHERE multi_product_customer = TRUE
    ) AS multi_product_customers,
    SUM(loan_count) AS total_loans,
    SUM(active_loan_count) AS active_loans,
    SUM(insurance_policy_count) AS total_insurance_policies,
    SUM(active_policy_count) AS active_insurance_policies,
    ROUND(
        AVG(loan_count)::numeric,
        2
    ) AS avg_loans_per_customer,
    ROUND(
        AVG(insurance_policy_count)::numeric,
        2
    ) AS avg_policies_per_customer
FROM "ucando"."dbt_analytics"."analytics_customer_overview"
  );