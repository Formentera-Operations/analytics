{{
  config(
    materialized='view',
    alias='pvunitcompparam'
  )
}}

with source as (
    select * from {{ source('prodview', 'PVT_PVUNITCOMPPARAM') }}
),

renamed as (
    select
        -- Primary identifiers
        IDFLOWNET as id_flow_net,
        IDRECPARENT as id_rec_parent,
        IDREC as id_rec,
        DTTM as dttm,
        
        -- Pressure measurements (kPa to PSI)
        PRESTUB / 6.894757 as pres_tubing,
        case when PRESTUB is not null then 'PSI' else null end as pres_tubing_unit_label,
        PRESCAS / 6.894757 as pres_casing,
        case when PRESCAS is not null then 'PSI' else null end as pres_casing_unit_label,
        PRESANNULUS / 6.894757 as pres_annulus,
        case when PRESANNULUS is not null then 'PSI' else null end as pres_annulus_unit_label,
        PRESLINE / 6.894757 as pres_line,
        case when PRESLINE is not null then 'PSI' else null end as pres_line_unit_label,
        PRESINJ / 6.894757 as pres_injection,
        case when PRESINJ is not null then 'PSI' else null end as pres_injection_unit_label,
        PRESWH / 6.894757 as pres_wellhead,
        case when PRESWH is not null then 'PSI' else null end as pres_wellhead_unit_label,
        PRESBH / 6.894757 as pres_bottomhole,
        case when PRESBH is not null then 'PSI' else null end as pres_bottomhole_unit_label,
        PRESTUBSI / 6.894757 as pres_tubing_si,
        case when PRESTUBSI is not null then 'PSI' else null end as pres_tubing_si_unit_label,
        PRESCASSI / 6.894757 as pres_casing_si,
        case when PRESCASSI is not null then 'PSI' else null end as pres_casing_si_unit_label,
        
        -- Temperature measurements (Celsius to Fahrenheit)
        TEMPWH / 0.555555555555556 + 32 as temp_wellhead,
        case when TEMPWH is not null then '°F' else null end as temp_wellhead_unit_label,
        TEMPBH / 0.555555555555556 + 32 as temp_bottomhole,
        case when TEMPBH is not null then '°F' else null end as temp_bottomhole_unit_label,
        
        -- Choke size (mm to 64ths of inch)
        SZCHOKE / 0.000396875 as size_choke,
        case when SZCHOKE is not null then '1/64"' else null end as size_choke_unit_label,
        
        -- Viscosity measurements
        VISCDYNAMIC as visc_dynamic,
        case when VISCDYNAMIC is not null then 'PA•S' else null end as visc_dynamic_unit_label,
        VISCKINEMATIC / 55.741824 as visc_kinematic,
        case when VISCKINEMATIC is not null then 'IN²/S' else null end as visc_kinematic_unit_label,
        
        -- Chemical properties
        PH as ph,
        case when PH is not null then 'PROPORTION' else null end as ph_unit_label,
        SALINITY / 1E-06 as salinity,
        case when SALINITY is not null then 'PPM' else null end as salinity_unit_label,
        
        -- User-defined pressure fields
        PRESUSER1 / 6.894757 as pres_user_1,
        case when PRESUSER1 is not null then 'PSI' else null end as pres_user_1_unit_label,
        PRESUSER2 / 6.894757 as pres_user_2,
        case when PRESUSER2 is not null then 'PSI' else null end as pres_user_2_unit_label,
        PRESUSER3 / 6.894757 as pres_user_3,
        case when PRESUSER3 is not null then 'PSI' else null end as pres_user_3_unit_label,
        PRESUSER4 / 6.894757 as pres_user_4,
        case when PRESUSER4 is not null then 'PSI' else null end as pres_user_4_unit_label,
        PRESUSER5 / 6.894757 as pres_user_5,
        case when PRESUSER5 is not null then 'PSI' else null end as pres_user_5_unit_label,
        
        -- User-defined temperature fields
        TEMPUSER1 / 0.555555555555556 + 32 as temp_user_1,
        case when TEMPUSER1 is not null then '°F' else null end as temp_user_1_unit_label,
        TEMPUSER2 / 0.555555555555556 + 32 as temp_user_2,
        case when TEMPUSER2 is not null then '°F' else null end as temp_user_2_unit_label,
        TEMPUSER3 / 0.555555555555556 + 32 as temp_user_3,
        case when TEMPUSER3 is not null then '°F' else null end as temp_user_3_unit_label,
        TEMPUSER4 / 0.555555555555556 + 32 as temp_user_4,
        case when TEMPUSER4 is not null then '°F' else null end as temp_user_4_unit_label,
        TEMPUSER5 / 0.555555555555556 + 32 as temp_user_5,
        case when TEMPUSER5 is not null then '°F' else null end as temp_user_5_unit_label,
        
        -- User-defined text fields
        USERTXT1 as user_txt_1,
        USERTXT2 as user_txt_2,
        USERTXT3 as user_txt_3,
        USERTXT4 as user_txt_4,
        USERTXT5 as user_txt_5,
        
        -- User-defined numeric fields (first two are dimensionless)
        USERNUM1 as user_num_1,
        USERNUM2 as user_num_2,
        -- Time conversions for user numeric fields 3-5 (seconds to minutes)
        USERNUM3 / 0.000694444444444444 as user_num_3,
        case when USERNUM3 is not null then 'MIN' else null end as user_num_3_unit_label,
        USERNUM4 / 0.000694444444444444 as user_num_4,
        case when USERNUM4 is not null then 'MIN' else null end as user_num_4_unit_label,
        USERNUM5 / 0.000694444444444444 as user_num_5,
        case when USERNUM5 is not null then 'MIN' else null end as user_num_5_unit_label,
        
        -- User-defined datetime fields
        USERDTTM1 as user_dttm_1,
        USERDTTM2 as user_dttm_2,
        USERDTTM3 as user_dttm_3,
        USERDTTM4 as user_dttm_4,
        USERDTTM5 as user_dttm_5,
        
        -- General information
        COM as com,
        
        -- System locking fields
        SYSLOCKMEUI as sys_lock_me_ui,
        SYSLOCKCHILDRENUI as sys_lock_children_ui,
        SYSLOCKME as sys_lock_me,
        SYSLOCKCHILDREN as sys_lock_children,
        SYSLOCKDATE as sys_lock_date,
        
        -- System audit fields
        SYSMODDATE as sys_mod_date,
        SYSMODUSER as sys_mod_user,
        SYSCREATEDATE as sys_create_date,
        SYSCREATEUSER as sys_create_user,
        SYSTAG as sys_tag,
        
        -- Fivetran metadata
        _FIVETRAN_SYNCED as update_date,
        _FIVETRAN_DELETED as deleted

    from source
)

select * from renamed