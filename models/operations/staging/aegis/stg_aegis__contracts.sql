{{
    config(
        materialized='view',
        tags=['aegis', 'staging', 'hedging']
    )
}}

with

source as (

    select * from {{ source('aegis_raw', 'CONTRACTS_INFO') }}

),

renamed as (

    select
        -- identifiers
        contractnumber::varchar as contract_number,
        aegiscontractid::int as aegis_contract_id,
        
        -- entities
        entity::varchar as entity_name,
        counterparty::varchar as counterparty_name,
        
        -- contract attributes
        contractdescription::varchar as contract_description,
        contractstatus::varchar as contract_status,
        contractpressurebase::float as contract_pressure_base,
        
        -- dates
        contractdate::timestamp_ntz as contract_date,
        contracteffectivedate::varchar as contract_effective_date_string,
        try_to_date(contracteffectivedate, 'YYYY-MM-DD') as contract_effective_date,
        initialterm::timestamp_ntz as initial_term_date,
        terminationdate::timestamp_ntz as termination_date,
        
        -- term details
        termdescription::varchar as term_description,
        noticerequirements::varchar as notice_requirements,
        
        -- geography
        basin::varchar as basin,
        play::varchar as play,
        geographicareaname::varchar as geographic_area_name,
        
        -- tags
        businessunittag::varchar as business_unit_tag,
        regiontag::varchar as region_tag,
        
        -- options and flags
        consenttoassign::boolean as consent_to_assign_flag,
        consenttodisclose::boolean as consent_to_disclose_flag,
        fullyexecuted::boolean as fully_executed_flag,
        hasattachment::boolean as has_attachment_flag,
        takeinkindoption::boolean as take_in_kind_option_flag,
        ethaneelection::boolean as ethane_election_flag,
        
        -- ethane details
        ethanenoticereq::varchar as ethane_notice_requirements,
        tiktypedescription::varchar as tik_type_description,
        
        -- metadata
        _portable_extracted::timestamp_ntz as extracted_at

    from source

),

filtered as (

    select * from renamed
    where contract_number is not null

),

enhanced as (

    select
        *,
        year(contract_effective_date) as contract_effective_year,
        year(termination_date) as termination_year,
        datediff(day, contract_effective_date, termination_date) as contract_term_days,
        current_timestamp() as _loaded_at

    from filtered

),

final as (

    select
        contract_number,
        aegis_contract_id,
        entity_name,
        counterparty_name,
        contract_description,
        contract_status,
        contract_pressure_base,
        contract_date,
        contract_effective_date,
        contract_effective_year,
        initial_term_date,
        termination_date,
        termination_year,
        contract_term_days,
        term_description,
        notice_requirements,
        basin,
        play,
        geographic_area_name,
        business_unit_tag,
        region_tag,
        consent_to_assign_flag,
        consent_to_disclose_flag,
        fully_executed_flag,
        has_attachment_flag,
        take_in_kind_option_flag,
        ethane_election_flag,
        ethane_notice_requirements,
        tik_type_description,
        extracted_at,
        _loaded_at

    from enhanced

)

select * from final