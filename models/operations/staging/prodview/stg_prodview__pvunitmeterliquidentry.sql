{{
  config(
    materialized='view',
    alias='pvunitmeterliquidentry'
  )
}}

with source as (
    select * from {{ source('prodview', 'PVT_PVUNITMETERLIQUIDENTRY') }}
),

renamed as (
    select
        -- Primary identifiers
        IDFLOWNET as id_flow_net,
        IDRECPARENT as id_rec_parent,
        IDREC as id_rec,
        DTTM as dttm,
        
        -- Meter readings
        READINGEND as reading_end,
        READINGSTART as reading_start,
        
        -- Quality measurements (decimal to percentage)
        BSW / 0.01 as bsw,
        case when BSW is not null then '%' else null end as bsw_unit_label,
        SANDCUT / 0.01 as sand_cut,
        case when SANDCUT is not null then '%' else null end as sand_cut_unit_label,
        
        -- General information
        COM as com,
        REASONOR as reason_or,
        
        -- Uncorrected volumes (cubic meters to barrels)
        VOLUNCORRTOTALCALC / 0.158987294928 as vol_uncorr_total_calc,
        case when VOLUNCORRTOTALCALC is not null then 'BBL' else null end as vol_uncorr_total_calc_unit_label,
        VOLUNCORRHCLIQCALC / 0.158987294928 as vol_uncorr_hc_liq_calc,
        case when VOLUNCORRHCLIQCALC is not null then 'BBL' else null end as vol_uncorr_hc_liq_calc_unit_label,
        
        -- Sample conditions (temperature: Celsius to Fahrenheit, pressure: kPa to PSI)
        TEMPOFVOL / 0.555555555555556 + 32 as temp_of_vol,
        case when TEMPOFVOL is not null then '°F' else null end as temp_of_vol_unit_label,
        PRESOFVOL / 6.894757 as pres_of_vol,
        case when PRESOFVOL is not null then 'PSI' else null end as pres_of_vol_unit_label,
        TEMPSAMPLE / 0.555555555555556 + 32 as temp_sample,
        case when TEMPSAMPLE is not null then '°F' else null end as temp_sample_unit_label,
        PRESSAMPLE / 6.894757 as pres_sample,
        case when PRESSAMPLE is not null then 'PSI' else null end as pres_sample_unit_label,
        
        -- Density measurements (complex formula to API gravity)
        power(nullif(DENSITYSAMPLE, 0), -1) / 7.07409872233005E-06 + -131.5 as density_sample,
        case when DENSITYSAMPLE is not null then '°API' else null end as density_sample_unit_label,
        power(nullif(DENSITYSAMPLE60F, 0), -1) / 7.07409872233005E-06 + -131.5 as density_sample_60f,
        case when DENSITYSAMPLE60F is not null then '°API' else null end as density_sample_60f_unit_label,
        
        -- Corrected volumes
        VOLCORRTOTALCALC / 0.158987294928 as vol_corr_total_calc,
        case when VOLCORRTOTALCALC is not null then 'BBL' else null end as vol_corr_total_calc_unit_label,
        VOLCORRHCLIQCALC / 0.158987294928 as vol_corr_hc_liq_calc,
        case when VOLCORRHCLIQCALC is not null then 'BBL' else null end as vol_corr_hc_liq_calc_unit_label,
        
        -- Corrected quality measurements
        BSWCORRCALC / 0.01 as bsw_corr_calc,
        case when BSWCORRCALC is not null then '%' else null end as bsw_corr_calc_unit_label,
        SANDCUTCORRCALC / 0.01 as sand_cut_corr_calc,
        case when SANDCUTCORRCALC is not null then '%' else null end as sand_cut_corr_calc_unit_label,
        
        -- Override conditions
        TEMPOR / 0.555555555555556 + 32 as temp_or,
        case when TEMPOR is not null then '°F' else null end as temp_or_unit_label,
        PRESOR / 6.894757 as pres_or,
        case when PRESOR is not null then 'PSI' else null end as pres_or_unit_label,
        power(nullif(DENSITYOR, 0), -1) / 7.07409872233005E-06 + -131.5 as density_or,
        case when DENSITYOR is not null then '°API' else null end as density_or_unit_label,
        
        -- Reference and tracking
        REFID as ref_id,
        ORIGSTATEMENTID as orig_statement_id,
        SOURCE as source,
        VERIFIED as verified,
        
        -- Override volumes
        VOLORHCLIQ / 0.158987294928 as vol_or_hc_liq,
        case when VOLORHCLIQ is not null then 'BBL' else null end as vol_or_hc_liq_unit_label,
        VOLORWATER / 0.158987294928 as vol_or_water,
        case when VOLORWATER is not null then 'BBL' else null end as vol_or_water_unit_label,
        VOLORSAND / 0.158987294928 as vol_or_sand,
        case when VOLORSAND is not null then 'BBL' else null end as vol_or_sand_unit_label,
        
        -- Final calculated volumes
        VOLTOTALCALC / 0.158987294928 as vol_total_calc,
        case when VOLTOTALCALC is not null then 'BBL' else null end as vol_total_calc_unit_label,
        VOLHCLIQCALC / 0.158987294928 as vol_hc_liq_calc,
        case when VOLHCLIQCALC is not null then 'BBL' else null end as vol_hc_liq_calc_unit_label,
        VOLHCLIQGASEQCALC / 28.316846592 as vol_hc_liq_gas_eq_calc,
        case when VOLHCLIQGASEQCALC is not null then 'MCF' else null end as vol_hc_liq_gas_eq_calc_unit_label,
        VOLWATERCALC / 0.158987294928 as vol_water_calc,
        case when VOLWATERCALC is not null then 'BBL' else null end as vol_water_calc_unit_label,
        VOLSANDCALC / 0.158987294928 as vol_sand_calc,
        case when VOLSANDCALC is not null then 'BBL' else null end as vol_sand_calc_unit_label,
        
        -- Final calculated quality
        BSWCALC / 0.01 as bsw_calc,
        case when BSWCALC is not null then '%' else null end as bsw_calc_unit_label,
        SANDCUTCALC / 0.01 as sand_cut_calc,
        case when SANDCUTCALC is not null then '%' else null end as sand_cut_calc_unit_label,
        
        -- Ticket information
        TICKETNO as ticket_no,
        TICKETSUBNO as ticket_sub_no,
        
        -- Analysis and seal references
        IDRECHCLIQANALYSISCALC as id_rec_hc_liq_analysis_calc,
        IDRECHCLIQANALYSISCALCTK as id_rec_hc_liq_analysis_calc_tk,
        IDRECSEALENTRY as id_rec_seal_entry,
        IDRECSEALENTRYTK as id_rec_seal_entry_tk,
        
        -- User-defined text fields
        USERTXT1 as user_txt_1,
        USERTXT2 as user_txt_2,
        USERTXT3 as user_txt_3,
        
        -- User-defined numeric fields
        USERNUM1 as user_num_1,
        USERNUM2 as user_num_2,
        USERNUM3 as user_num_3,
        
        -- User-defined datetime fields
        USERDTTM1 as user_dttm_1,
        USERDTTM2 as user_dttm_2,
        USERDTTM3 as user_dttm_3,
        
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