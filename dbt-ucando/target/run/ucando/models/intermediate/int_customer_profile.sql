
  
    

  create  table "ucando"."dbt"."int_customer_profile__dbt_tmp"
  
  
    as
  
  (
    -- int_customer_profile.sql

with ranked as (
    select
        customer_id,
        customer_phone,
        source_system,
        event_time,
        operation,
        changed_field,
        new_value,
        row_number() over (
            partition by customer_id
            order by
                sequence desc,
                event_time desc,
                kafka_timestamp desc,
                kafka_partition desc,
                kafka_offset desc
        ) as rn
    from "ucando"."dbt_stage"."stg_customer_cdc"
),

latest as (
    select
        customer_id,
        customer_phone,
        source_system,
        event_time as profile_updated_at
    from ranked
    where rn = 1
)

select
    customer_id,
    customer_phone,
    source_system,
    profile_updated_at
from latest
  );
  