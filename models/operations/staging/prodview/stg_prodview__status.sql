{{ config(
    materialized='view',
    tags=['prodview', 'status', 'completions', 'staging']
) }}

with source_data as (
    select * from {{ source('prodview', 'PVT_PVUNITCOMPSTATUS') }}
    where _fivetran_deleted = false
),

renamed as (
    select
        -- Primary identifiers
        idrec as status_id,
        idrecparent as completion_id,
        idflownet as flow_network_id,
        
        -- Status information
        dttm as status_date,
        status as status,
        
        -- Fluid and flow characteristics
        primaryfluidtyp as primary_fluid_type,
        flowdirection as flow_direction,
        commingled as commingled,
        typfluidprod as oil_or_condensate,
        
        -- Completion characteristics
        typcompletion as completion_type,
        methodprod as production_method,
        
        -- Calculation and reporting flags
        calclostprod as calculate_lost_production,
        wellcountincl as include_in_well_count,
        
        -- User-defined fields - Text
        usertxt1 as user_text_1,
        usertxt2 as user_text_2,
        usertxt3 as user_text_3,
        
        -- User-defined fields - Numeric
        usernum1 as user_num_1,
        usernum2 as user_num_2,
        usernum3 as user_num_3,
        
        -- Comments
        com as comment,
        
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