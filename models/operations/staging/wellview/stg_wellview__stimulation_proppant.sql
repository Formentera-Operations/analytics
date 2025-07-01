{{ config(
    materialized='view',
    tags=['wellview', 'stimulation', 'proppant', 'sand', 'staging']
) }}

with source_data as (
    select * from {{ source('wellview_calcs', 'WVT_WVSTIMINTPROP') }}
    where _fivetran_deleted = false
),

renamed as (
    select
        -- Primary identifiers
        idwell as well_id,
        idrecparent as parent_record_id,
        idrec as record_id,
        
        -- Proppant classification
        typ1 as proppant_type,
        typ2 as proppant_subtype,
        des as proppant_description,
        sz as sand_size,
        
        -- Amount information (converted to pounds)
        amountdesign / 0.45359237 as design_amount_lb,
        amount / 0.45359237 as actual_amount_lb,
        amountcalc / 0.45359237 as calculated_amount_lb,
        
        -- Performance ratio
        ratiotamountdesigncalc as actual_to_design_proppant_mass_ratio,
        
        -- Notes and references
        note as note,
        
        -- User fields
        usernum1 as user_number_1,
        usertxt1 as user_text_1,

        -- System locking fields
        syslockmeui as system_lock_me_ui,
        syslockchildrenui as system_lock_children_ui,
        syslockme as system_lock_me,
        syslockchildren as system_lock_children,
        syslockdate as system_lock_date,

        -- System tracking fields
        syscreatedate as created_at,
        syscreateuser as created_by,
        sysmoddate as modified_at,
        sysmoduser as modified_by,
        systag as system_tag,

        -- Fivetran metadata
        _fivetran_synced as fivetran_synced_at

    from source_data
)

select * from renamed