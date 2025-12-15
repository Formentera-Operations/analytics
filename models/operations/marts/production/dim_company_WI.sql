{{ config(
    enabled=true,
    materialized='table',
    tags=['marts', 'dim']
) }}

with expensedeck_company as 
    (
    select * 
    from {{ ref('int_oda_latest_company_WI')}}
    ),

    cc_wells as 
    (
    select * 
    from {{ref('stg_cc__company_wells')}} 
    ),

    cc_well_WI as
    (
        select 
    cc.well_id as cc_well_id,
    cc.aries_id as cc_aries_id,
    cc.phdwin_id as cc_phdwin_id,
    cc.chosen_id as cc_chosen_id,
    ec.*
    from expensedeck_company ec
    inner join cc_wells cc
    on ec.eid = cc.phdwin_id
    )

    select * from cc_well_WI

