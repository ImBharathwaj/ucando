{{
    config(
        schema="stage"
    )
}}

select *
from {{ source('public', 'credit_events') }}