
  create view "ucando"."dbt_stage"."stg_credit__dbt_tmp"
    
    
  as (
    

select *
from "ucando"."public"."credit_events"
  );