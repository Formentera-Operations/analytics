{{ config(
    enabled=true,
    materialized='view'
) }}

with prodstatus as (
    select
        *,
        case
            when status = 'Active' then 'Producing'
            when status = 'Completing' then 'Producing'
            when status = 'ESP' then 'Producing'
            when status = 'ESP - OWNED' then 'Producing'
            when status = 'FLOWING' then 'Producing'
            when status = 'Flowing' then 'Producing'
            when status = 'FLOWING - CASING' then 'Producing'
            when status = 'FLOWING - TUBING' then 'Producing'
            when status = 'GAS LIFT' then 'Producing'
            when status = 'INACTIVE' then 'Shut In'
            when status = 'INACTIVE COMPLETED' then 'Shut In'
            when status = 'INACTIVE INJECTOR' then 'Shut In'
            when status = 'INACTIVE PRODUCER' then 'Shut In'
            when status = 'INJECTING' then 'Injecting'
            when status = 'Producer' then 'Producing'
            when status = 'SHUT IN' then 'Shut In'
            when status = 'Shut-In' then 'Shut In'
            else status
        end as status_clean
    from {{ ref('stg_prodview__status') }}
)

select s.*
from prodstatus s
