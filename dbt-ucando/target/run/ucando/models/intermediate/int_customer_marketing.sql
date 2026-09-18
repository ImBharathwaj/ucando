
  
    

  create  table "ucando"."dbt"."int_customer_marketing__dbt_tmp"
  
  
    as
  
  (
    with marketing_agg as (
    select
        customer_id,
        count(*) as marketing_event_count,
        count(distinct campaign_id) as distinct_campaigns,
        count(distinct channel) as distinct_marketing_channels,
        min(event_time) as first_marketing_event_at,
        max(event_time) as last_marketing_event_at,
        -- Campaign engagement
        count(*) filter (
            where event_type = 'CAMPAIGN_SENT'
        ) as campaigns_sent,
        count(*) filter (
            where event_type = 'CAMPAIGN_DELIVERED'
        ) as campaigns_delivered,
        count(*) filter (
            where event_type = 'CAMPAIGN_OPENED'
        ) as campaigns_opened,
        count(*) filter (
            where event_type = 'CAMPAIGN_CLICKED'
        ) as campaigns_clicked,
        count(*) filter (
            where event_type = 'CONVERSION'
        ) as campaigns_converted,
        -- Campaign negative signals
        count(*) filter (
            where event_type = 'CAMPAIGN_BOUNCED'
        ) as campaigns_bounced,
        count(*) filter (
            where event_type = 'CAMPAIGN_UNSUBSCRIBED'
        ) as campaigns_unsubscribed,
        -- Lead funnel
        count(*) filter (
            where event_type = 'LEAD_CREATED'
        ) as leads_created,
        count(*) filter (
            where event_type = 'LEAD_ACCEPTED'
        ) as leads_accepted,
        count(*) filter (
            where event_type = 'LEAD_REJECTED'
        ) as leads_rejected
    from "ucando"."dbt_stage"."stg_marketing"
    group by customer_id
)
select
    customer_id,
    marketing_event_count,
    distinct_campaigns,
    distinct_marketing_channels,
    campaigns_sent,
    campaigns_delivered,
    campaigns_opened,
    campaigns_clicked,
    campaigns_converted,
    campaigns_bounced,
    campaigns_unsubscribed,
    leads_created,
    leads_accepted,
    leads_rejected,
    first_marketing_event_at,
    last_marketing_event_at
from marketing_agg
  );
  