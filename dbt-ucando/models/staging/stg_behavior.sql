{{
    config(
        schema="stage"
    )
}}

select *
from {{ source('public', 'behavior_events') }}