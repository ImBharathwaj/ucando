
  create view "ucando"."dbt_stage"."stg_marketing__dbt_tmp"
    
    
  as (
    

select *
from "ucando"."public"."marketing_events"
  );