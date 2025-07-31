{{ config(
    materialized='view',
    tags=['prodview', 'parameters', 'completions', 'staging']
) }}

with source_data as (
    select * from {{ source('prodview', 'PVT_PVUNITCOMPPARAM') }}
    where _fivetran_deleted = false
),

renamed as (
    select
        -- Primary identifiers
        idrec as "Completion Parameter ID",
        idrecparent as "Completion Parameter Parent ID",
        idflownet as "Flow Net ID",

        -- Measurement date
        dttm as "Measurement Date",

        -- Pressure measurements (converted to PSI)
        prestub / 6.894757 as "Tubing Pressure psi",
        prescas / 6.894757 as "Casing Pressure psi",
        presannulus / 6.894757 as "Annulus Pressure psi",
        presline / 6.894757 as "Line Pressure psi",
        presinj / 6.894757 as "Injection Pressure psi",
        preswh / 6.894757 as "Wellhead Pressure psi",
        presbh / 6.894757 as "Bottomhole Pressure psi",
        prestubsi / 6.894757 as "Shut In Tubing Pressure psi",
        prescassi / 6.894757 as "Shut In Casing Pressure psi",

        -- Temperature measurements (converted to Fahrenheit)
        tempwh / 0.555555555555556 + 32 as "Wellhead Temperature F",
        tempbh / 0.555555555555556 + 32 as "Bottomhole Temperature F",

        -- Equipment specifications
        szchoke / 0.000396875 as "Choke Size 64ths",

        -- Fluid properties
        viscdynamic as "Dynamic Viscosity Pascal Seconds",
        visckinematic / 55.741824 as "Kinematic Viscosity In2 Per S",
        ph as "PH Level",
        salinity / 1E-06 as "H2s Daily Reading ppm",

        -- User-defined pressure measurements (converted to PSI)
        presuser1 / 6.894757 as "Surface Casing Pressure psi",
        presuser2 / 6.894757 as "Intermediate Casing Pressure psi",
        presuser3 / 6.894757 as "Plunger On Pressure psi",
        presuser4 / 6.894757 as "User Pressure 4 psi",
        presuser5 / 6.894757 as "Annulus Pressure 2 psi",

        -- User-defined temperature measurements (converted to Fahrenheit)
        tempuser1 / 0.555555555555556 + 32 as "Treater Temperature f",
        tempuser2 / 0.555555555555556 + 32 as "User Temperature 2 f",
        tempuser3 / 0.555555555555556 + 32 as "User Temperature 3 f",
        tempuser4 / 0.555555555555556 + 32 as "Fluid Level Csg P Psi f",
        tempuser5 / 0.555555555555556 + 32 as "Fluid Level Tbg P Psi f",

        -- User-defined fields - Text (plunger information)
        usertxt1 as "Spcc Inspection Complete",
        usertxt2 as "Plunger Model",
        usertxt3 as "Plunger Make",
        usertxt4 as "Plunger Size",
        usertxt5 as "Operational Work",

        -- User-defined fields - Numeric (plunger operations)
        usernum1 as "Cycles",
        usernum2 as "Arrivals",
        usernum3 / 0.000694444444444444 as "Travel Time min",
        usernum4 / 0.000694444444444444 as "After Flow min",
        usernum5 / 0.000694444444444444 as "Shut In Time min",

        -- User-defined fields - Datetime
        userdttm1 as "Plunger Inspection Date",
        userdttm2 as "Plunger Replace Date",
        userdttm3 as "User Date 3",
        userdttm4 as "User Date 4",
        userdttm5 as "User Date 5",

        -- Notes and comments
        com as "Note",

        -- System fields
        syscreatedate as "Created At (UTC)",
        syscreateuser as "Created By",
        sysmoddate as "Last Mod At (UTC)",
        sysmoduser as "Last Mod By",
        systag as "System Tag",
        syslockdate as "System Lock Date (UTC)",
        syslockme as "System Lock Me",
        syslockchildren as "System Lock Children",
        syslockmeui as "System Lock Me UI",
        syslockchildrenui as "System Lock Children UI",

        -- Fivetran fields
        _fivetran_synced as "Fivetran Synced At"

        
    from source_data
)

select * from renamed