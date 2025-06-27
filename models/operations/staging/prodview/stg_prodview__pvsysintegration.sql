with source as (
    select * from {{ source('prodview', 'PVT_PVSYSINTEGRATION') }}
),

renamed as (
    select
        -- Primary identifiers
        IDFLOWNET,
        IDRECPARENT,
        IDREC,
        
        -- Integration metadata
        TBLKEYPARENT,
        INTEGRATORDES,
        INTEGRATORVER,
        AFPRODUCT,
        AFIDENTITY,
        AFIDREC,
        NOTE,
        
        -- System locking fields
        SYSLOCKMEUI,
        SYSLOCKCHILDRENUI,
        SYSLOCKME,
        SYSLOCKCHILDREN,
        SYSLOCKDATE,
        
        -- System audit fields
        SYSMODDATE,
        SYSMODUSER,
        SYSCREATEDATE,
        SYSCREATEUSER,
        SYSTAG,
        
        -- Fivetran metadata
        _FIVETRAN_SYNCED as UPDATE_DATE,
        _FIVETRAN_DELETED as DELETED

    from source
)

select * from renamed