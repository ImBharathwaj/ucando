{{
    config(
        schema="stage"
    )
}}

select *
from {{ source('public', 'insurance_policies') }}