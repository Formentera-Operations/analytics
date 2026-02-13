{{
    config(
        enabled=true,
        materialized='view'
    )
}}

with header as (
    select * from {{ ref('stg_wellview__well_header') }}
),

wvintegration as (
    select
        wi.af_id_rec,
        u.id_rec as unit_record_id
    from {{ ref('stg_prodview__system_integrations') }} as wi
    left join {{ ref('stg_prodview__units') }} as u
        on
            wi.id_rec_parent = u.id_rec
            and wi.id_flownet = u.id_flownet
    where
        wi.product_description = 'WellView'
        and wi.table_key_parent = 'pvunit'
),

source as (
    select
        h.*,
        wi.unit_record_id,
        greatest(
            coalesce(h."Last Mod At (UTC)", '0000-01-01T00:00:00.000Z')
        ) as "Updated Date"
    from header as h
    left join wvintegration as wi
        on h."Well ID" = wi.af_id_rec
)

select *
from source
order by "Well ID" desc
