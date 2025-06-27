{{
  config(
    materialized='view',
    alias='unitnode_v1'
  )
}}

with pvunitnode as (
    select * from {{ ref('stg_prodview__pvunitnode') }}
    where deleted = false
)

select
    -- Key IDs
    id_rec as node_id,
    id_rec_parent as unit_id,
    
    -- Node configuration
    name,
    type as typ,
    dttm_start,
    dttm_end,
    
    -- Component and fluid properties
    component,
    component_phase,
    des_fluid,
    keep_whole,
    type_fluid_base_restrict,
    
    -- Flow diagram configuration
    sort_flow_diag,
    
    -- Correction and external IDs
    correction_id_1,
    correction_id_2,
    correction_type_1,
    correction_type_2,
    other_id,
    
    -- Product and facility information
    fac_product_name,
    use_virtual_analysis,
    
    -- Disposition configuration
    disposition_point,
    disp_product_name,
    type_disp_1,
    type_disp_2,
    type_disp_hc_liq,
    disp_id_a,
    disp_id_b,
    
    -- Purchaser information
    purchaser_name,
    purchaser_code_1,
    purchaser_code_2,
    
    -- General configuration
    com,
    
    -- User-defined fields
    user_txt_1,
    user_txt_2,
    user_txt_3,
    user_num_1,
    user_num_2,
    user_num_3,
    
    -- System audit fields
    sys_create_date,
    sys_create_user,
    sys_mod_date,
    sys_mod_user,
    
    -- Update tracking
    update_date

from pvunitnode