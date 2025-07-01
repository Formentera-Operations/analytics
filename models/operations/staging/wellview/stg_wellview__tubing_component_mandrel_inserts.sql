{{ config(
    materialized='view',
    tags=['wellview', 'tubing', 'mandrels', 'inserts', 'staging']
) }}

with source_data as (
    select * from {{ source('wellview_calcs', 'WVT_WVTUBCOMPMANDRELINSERT') }}
    where _fivetran_deleted = false
),

renamed as (
    select
        -- Primary identifiers
        idrec as insert_id,
        idrecparent as mandrel_id,
        idwell as well_id,
        
        -- Valve information
        valvetyp as valve_type,
        valvedes as valve_description,
        valvematerial as valve_material,
        valvepacking as valve_packing,
        
        -- Dates
        dttmrun as run_date,
        dttmpull as pull_date,
        
        -- Manufacturing details
        make as manufacturer,
        model as model,
        sn as serial_number,
        refid as reference_id,
        
        -- Dimensions (converted to US units)
        szod / 0.0254 as od_inches,
        szport / 0.0254 as port_size_inches,
        
        -- Pressure specifications (converted to US units)
        trorun / 6.894757 as tro_run_psi,
        tropull / 6.894757 as tro_pull_psi,
        pressurfgaugeopen / 6.894757 as surface_gauge_pressure_open_psi,
        pressurfgaugeclose / 6.894757 as surface_gauge_pressure_close_psi,
        
        -- Temperature (converted to Fahrenheit)
        temp / 0.555555555555556 + 32 as temperature_fahrenheit,
        
        -- Latch specifications
        latchtyp as latch_type,
        latchmaterial as latch_material,
        
        -- Material specifications
        orificematerial as orifice_material,
        
        -- Operational information
        retrievemeth as retrieval_method,
        pullreason as pull_reason,
        service as service_type,
        
        -- Comments
        com as comments,
        
        -- System fields
        syscreatedate as created_at,
        syscreateuser as created_by,
        sysmoddate as modified_at,
        sysmoduser as modified_by,
        systag as system_tag,
        syslockmeui as system_lock_me_ui,
        syslockchildrenui as system_lock_children_ui,
        syslockme as system_lock_me,
        syslockchildren as system_lock_children,
        syslockdate as system_lock_date,
        
        -- Fivetran metadata
        _fivetran_synced as fivetran_synced_at

    from source_data
)

select * from renamed