
  create view "ucando"."dbt_stage"."stg_behavior__dbt_tmp"
    
    
  as (
    

select *
from "ucando"."public"."behavior_events"
  );