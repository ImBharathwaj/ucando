{{
    config(
        schema="stage"
    )
}}

select *
from {{ source('public', 'marketing_events') }}