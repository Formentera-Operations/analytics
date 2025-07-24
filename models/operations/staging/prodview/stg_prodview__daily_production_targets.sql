{{ config(
    materialized='view',
    tags=['prodview', 'completions', 'targets', 'daily', 'staging']
) }}

with source_data as (
    select * from {{ source('prodview', 'PVT_PVUNITCOMPTARGETDAY') }}
    where _fivetran_deleted = false
),

renamed as (
    select
        -- Primary identifiers
        idrec as target_id,
        idrecparent as completion_id,
        idflownet as flow_network_id,
        
        -- Date/Time information
        dttm as target_date,
        
        -- Target rates for liquids (converted from m³/day to BBL/day)
        ratehcliq / 0.1589873 as target_rate_hcliq_bbl_per_day,
        rateoil / 0.1589873 as target_rate_oil_bbl_per_day,
        ratecond / 0.1589873 as target_rate_condensate_bbl_per_day,
        ratengl / 0.1589873 as target_rate_ngl_bbl_per_day,
        ratewater / 0.1589873 as target_rate_water_bbl_per_day,
        ratesand / 0.1589873 as target_rate_sand_bbl_per_day,
        
        -- Target rate for gas (converted from m³/day to MCF/day)
        rategas / 28.316846592 as target_rate_gas_mcf_per_day,
        
        -- System fields
        syscreatedate as created_at,
        syscreateuser as created_by,
        sysmoddate as modified_at,
        sysmoduser as modified_by,
        systag as system_tag,
        syslockdate as system_lock_date,
        syslockme as system_lock_me,
        syslockchildren as system_lock_children,
        syslockmeui as system_lock_me_ui,
        syslockchildrenui as system_lock_children_ui,
        
        -- Fivetran fields
        _fivetran_synced as fivetran_synced_at

    from source_data
)

select * from renamed