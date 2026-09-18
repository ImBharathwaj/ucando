{{
    config(
        schema="stage"
    )
}}

select *
from {{ source('public', 'customer_cdc') }}