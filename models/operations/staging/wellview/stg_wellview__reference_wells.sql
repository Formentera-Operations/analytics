{{ config(
    materialized='view',
    tags=['wellview', 'reference-wells', 'relationships', 'staging']
) }}

with source_data as (
    select * from {{ source('wellview_calcs', 'WVT_WVREFWELLS') }}
    where _fivetran_deleted = false
),

renamed as (
    select
        -- Primary identifiers
        idwell as well_id,
        idrec as record_id,
        
        -- Reference well relationship
        idrecrefwell as reference_well_id,
        idrecrefwelltk as reference_well_table_key,
        typ1 as relationship_type,
        typ2 as relationship_subtype,
        des as relationship_description,
        
        -- Time period
        dttmstart as start_date,
        dttmend as end_date,
        
        -- WellView data link
        idrecitem as wellview_data_link_id,
        idrecitemtk as wellview_data_link_table_key,
        
        -- Distance information (converted to US units)
        dist / 1609.344 as distance_to_well_miles,
        
        -- Comments
        com as comment,
        
        -- Sequence
        sysseq as sequence_number,

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