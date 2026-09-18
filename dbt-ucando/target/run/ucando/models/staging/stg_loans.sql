
  create view "ucando"."dbt_stage"."stg_loans__dbt_tmp"
    
    
  as (
    

select *
from "ucando"."public"."loans"
  );