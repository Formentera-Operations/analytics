{{
    config(
        enabled=true,
        materialized='view'
    )
}}

with targetdaily as (
    select *
    from {{ ref('stg_prodview__production_targets_daily') }}
),

parenttarget as (
    select *
    from {{ ref('stg_prodview__production_targets') }}
),

header as (
    select *
    from {{ ref('int_prodview__well_header') }}
),

source as (
    select
        p."CC Forecast Name",
        t."Created At (UTC)",
        t."Created By",
        t."Flow Net ID",
        p."Is Use in Diff from Target Calculations",
        t."Last Mod At (UTC)",
        t."Last Mod By",
        t."Target Daily Date" as "Prod Date",
        --,t."Target Record ID"
        --,t."Target Daily Record ID"
        t."Target Daily Rate Condensate bbl per Day",
        t."Target Daily Rate Gas mcf per Day",
        t."Target Daily Rate Hcliq bbl per Day",
        t."Target Daily Rate Ngl bbl per Day",
        t."Target Daily Rate Oil bbl per Day",
        t."Target Daily Rate Sand bbl per Day",
        t."Target Daily Rate Water bbl per Day",
        t."Target Daily Record ID",
        p."Target Record ID",
        p."Target Start Date",
        p."Target Type",
        h.unit_record_id as "Unit Record ID"
    from targetdaily t
    left join parenttarget p
        on t."Target Record ID" = p."Target Record ID"
    left join header h
        on p."Parent Target Record ID" = h.completion_record_id
--where not i."Target Record ID" is null

)

select *
from source
--order by "Prod Date" Desc
