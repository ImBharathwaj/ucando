
  create view "ucando"."dbt_analytics"."marketing_kpis__dbt_tmp"
    
    
  as (
    

SELECT
    SUM(campaigns_sent) AS campaigns_sent,
    SUM(campaigns_delivered) AS campaigns_delivered,
    SUM(campaigns_opened) AS campaigns_opened,
    SUM(campaigns_clicked) AS campaigns_clicked,
    SUM(campaigns_converted) AS campaigns_converted,
    SUM(leads_created) AS leads_created,
    SUM(leads_accepted) AS leads_accepted,
    ROUND(
        (
            SUM(campaigns_opened)::numeric
            / NULLIF(SUM(campaigns_sent), 0)
        ) * 100,
        2
    ) AS cmpn_open,
    ROUND(
        (
            SUM(campaigns_clicked)::numeric
            / NULLIF(SUM(campaigns_sent), 0)
        ) * 100,
        2
    ) AS cmpn_click_rate,
    ROUND(
        (
            SUM(campaigns_converted)::numeric
            / NULLIF(SUM(campaigns_sent), 0)
        ) * 100,
        2
    ) AS cmpn_cvsn_rate,
    ROUND(
        (
            SUM(leads_accepted)::numeric
            / NULLIF(SUM(leads_created), 0)
        ) * 100,
        2
    ) AS lead_acpt_rate
FROM "ucando"."dbt_analytics"."analytics_customer_overview"
  );