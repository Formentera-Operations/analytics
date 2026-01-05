with source as (
    select * from {{ source('oda', 'AFEBUDGET') }}
),

renamed as (
    select
        -- ids
        id,
        afebudgetidentity as afe_budget_identity,
        afeid as afe_id,
        companyid as company_id,
        createeventid as create_event_id,
        currencyid as currency_id,
        importdataid as import_data_id,
        updateeventid as update_event_id,
        wellid as well_id,

        -- timestamps
        createdate as created_at,
        recordinsertdate as record_inserted_at,
        recordupdatedate as record_updated_at,
        updatedate as updated_at,

        -- numerics
        basisid as basis_id,
        fiscalyear as fiscal_year,
        nid as n_id,

        -- booleans
        isvalue as is_value,

        -- metadata
        "_meta/op" as _meta_op,
        flow_published_at,
        flow_document

    from source
)

select * from renamed