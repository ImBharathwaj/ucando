
  create view "ucando"."dbt_stage"."stg_customer_cdc__dbt_tmp"
    
    
  as (
    

select *
from "ucando"."public"."customer_cdc"
  );