{{
    config(
        schema="stage"
    )
}}

select *
from {{ source('public', 'loans') }}