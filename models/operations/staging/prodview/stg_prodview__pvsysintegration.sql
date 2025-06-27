with source as (
    select * from {{ source('prodview', 'PVT_PVSYSINTEGRATION') }}
),

renamed as (
    select
        -- Primary identifiers
        IDFLOWNET as id_flow_net,
        IDRECPARENT as id_rec_parent,
        IDREC as id_rec,
        
        -- Integration metadata
        TBLKEYPARENT as tbl_key_parent,
        INTEGRATORDES as integrator_description,
        INTEGRATORVER as integrator_version,
        AFPRODUCT as af_product,
        AFIDENTITY as af_identity,
        AFIDREC as af_id_rec,
        NOTE as note,
        
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