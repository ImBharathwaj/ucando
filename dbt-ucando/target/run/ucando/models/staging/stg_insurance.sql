
  create view "ucando"."dbt_stage"."stg_insurance__dbt_tmp"
    
    
  as (
    

select *
from "ucando"."public"."insurance_policies"
  );