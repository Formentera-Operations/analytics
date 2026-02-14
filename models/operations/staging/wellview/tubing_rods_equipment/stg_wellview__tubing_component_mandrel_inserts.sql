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
        latchtyp as latch_type,
        latchmaterial as latch_material,

        -- Pressure specifications (converted to US units)
        orificematerial as orifice_material,
        retrievemeth as retrieval_method,
        pullreason as pull_reason,
        service as service_type,

        -- Temperature (converted to Fahrenheit)
        com as comments,

        -- Latch specifications
        syscreatedate as created_at,
        syscreateuser as created_by,

        -- Material specifications
        sysmoddate as modified_at,

        -- Operational information
        sysmoduser as modified_by,
        systag as system_tag,
        syslockmeui as system_lock_me_ui,

        -- Comments
        syslockchildrenui as system_lock_children_ui,

        -- System fields
        syslockme as system_lock_me,
        syslockchildren as system_lock_children,
        syslockdate as system_lock_date,
        _fivetran_synced as fivetran_synced_at,
        szod / 0.0254 as od_inches,
        szport / 0.0254 as port_size_inches,
        trorun / 6.894757 as tro_run_psi,
        tropull / 6.894757 as tro_pull_psi,
        pressurfgaugeopen / 6.894757 as surface_gauge_pressure_open_psi,
        pressurfgaugeclose / 6.894757 as surface_gauge_pressure_close_psi,

        -- Fivetran metadata
        temp / 0.555555555555556 + 32 as temperature_fahrenheit

    from source_data
)

select * from renamed
