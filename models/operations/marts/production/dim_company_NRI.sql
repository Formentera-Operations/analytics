{{ config(
    enabled=true,
    materialized='table',
    tags=['marts', 'dim']
) }}

with revenuedeck_company as 
    (
    select * 
    from {{ ref('int_oda_latest_company_NRI')}}
    ),

    cc_wells as 
    (
    select * 
    from {{ref('stg_cc__company_wells')}} 
    ),

    cc_well_NRI as
    (
        select 
    cc.well_id as cc_well_id,
    cc.aries_id as cc_aries_id,
    cc.phdwin_id as cc_phdwin_id,
    cc.chosen_id as cc_chosen_id,
    rc.*
    from revenuedeck_company rc
    inner join cc_wells cc
    on rc.eid = cc.phdwin_id
    )

    select * from cc_well_NRI

