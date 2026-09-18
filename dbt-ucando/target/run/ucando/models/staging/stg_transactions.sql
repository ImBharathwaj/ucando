
  create view "ucando"."dbt_stage"."stg_transactions__dbt_tmp"
    
    
  as (
    

select
    event_id,
    customer_id,
    account_id,
    transaction_id,
    event_time,
    event_date,
    transaction_type,
    amount,
    currency,
    merchant_id,
    merchant_name,
    merchant_category,
    channel,
    location,
    balance_after,
    customer_phone,
    kafka_topic,
    kafka_partition,
    kafka_offset,
    kafka_timestamp,
    sequence,
    emitted_at
from "ucando"."public"."transactions"
  );