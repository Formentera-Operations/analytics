{{
    config(
        materialized='table',
        tags=['orm', 'mart', 'formentera']
    )
}}

{#
    Dimension: Owner 360

    Purpose: Unified owner dimension for Owner Relationship Management.
    Combines contact-based owners (persons with optional company association)
    and company-only owners (entities with no linked contact).

    Grain: One row per owner entity
    - contact_id-based rows for contacts (19K)
    - company_id-based rows for companies with no contacts (45K)

    Sources:
    - stg_hubspot__contacts (primary for contact-based owners)
    - stg_hubspot__companies (join for contact owners + standalone for company-only)

    Entity type classification is derived from company_name patterns:
    - LLC: name contains 'llc' or 'l.l.c'
    - Trust: name contains 'trust', 'revocable', 'irrevocable', or ends with ' tr'
    - Estate: name contains 'estate'
    - LP: name contains ' lp' or 'l.p.'
    - Corporation: name contains 'inc', 'corp'
    - Individual: everything else
#}

with

contacts as (
    select * from {{ ref('stg_hubspot__contacts') }}
),

companies as (
    select * from {{ ref('stg_hubspot__companies') }}
),

-- Company IDs that have at least one contact linked
contact_company_ids as (
    select distinct associated_company_id
    from contacts
    where associated_company_id is not null
),

-- Reusable entity type macro-like logic via a helper CTE
-- Contact-based owners: one row per contact, company fields denormalized
contact_owners as (
    select
        -- surrogate key
        contacts.contact_sk as owner_sk,

        -- identifiers
        contacts.contact_id,
        contacts.associated_company_id as company_id,

        -- owner source
        'contact' as owner_source,

        -- name
        contacts.first_name,
        contacts.last_name,
        coalesce(
            nullif(
                trim(contacts.first_name || ' ' || contacts.last_name), ''
            ),
            contacts.last_name,
            contacts.first_name,
            contacts.email
        ) as full_name,

        -- contact info
        contacts.email,
        contacts.email_domain,
        contacts.phone,
        contacts.mobile_phone,
        contacts.job_title,

        -- company / legal entity
        companies.company_name,

        -- address (prefer company, fall back to contact)
        coalesce(
            companies.address_line_one, contacts.address
        ) as address,
        coalesce(companies.city, contacts.city) as city,
        coalesce(companies.state, contacts.state) as state,
        coalesce(companies.zip, contacts.zip) as zip,
        coalesce(companies.country, contacts.country) as country,
        companies.address_line_one is not null as is_address_from_company,

        -- financial / tax (from company)
        companies.tax_status,
        companies.is_tax_exempt,
        companies.tax_id_number,
        companies.revenue_payment_type,
        companies.minimum_check_amount,
        companies.dba_1099_name,

        -- operations (from company)
        companies.owner_number_ogsys,
        companies.acquired_owner_number,
        companies.asset,
        companies.is_active as is_company_active,
        companies.is_working_interest_owner,
        companies.is_revenue_checks_on_hold,
        companies.statement_delivery,
        companies.suspense_reason,

        -- revenue statement (from contact)
        contacts.rev_statement_delivery,
        contacts.rev_statement_email,

        -- status
        contacts.lifecycle_stage,
        contacts.hubspot_owner_id,
        contacts.associated_company_id is not null as has_company,

        -- dates
        contacts.created_at,
        contacts.last_modified_at

    from contacts
    left join companies
        on contacts.associated_company_id = companies.company_id
),

-- Company-only owners: companies with no linked contacts
company_only_owners as (
    select
        -- surrogate key (derived from company_id since no contact exists)
        companies.company_sk as owner_sk,

        -- identifiers
        null::varchar as contact_id,
        companies.company_id,

        -- owner source
        'company_only' as owner_source,

        -- name (use company_name as full_name)
        null::varchar as first_name,
        null::varchar as last_name,
        companies.company_name as full_name,

        -- contact info (none available)
        companies.email,
        null::varchar as email_domain,
        companies.phone,
        null::varchar as mobile_phone,
        null::varchar as job_title,

        -- company / legal entity
        companies.company_name,

        -- address (company only)
        companies.address_line_one as address,
        companies.city,
        companies.state,
        companies.zip,
        companies.country,
        true as is_address_from_company,

        -- financial / tax
        companies.tax_status,
        companies.is_tax_exempt,
        companies.tax_id_number,
        companies.revenue_payment_type,
        companies.minimum_check_amount,
        companies.dba_1099_name,

        -- operations
        companies.owner_number_ogsys,
        companies.acquired_owner_number,
        companies.asset,
        companies.is_active as is_company_active,
        companies.is_working_interest_owner,
        companies.is_revenue_checks_on_hold,
        companies.statement_delivery,
        companies.suspense_reason,

        -- revenue statement (none for company-only)
        null::varchar as rev_statement_delivery,
        null::varchar as rev_statement_email,

        -- status
        companies.lifecycle_stage,
        companies.hubspot_owner_id,
        true as has_company,

        -- dates
        companies.created_at,
        companies.last_modified_at

    from companies
    left join contact_company_ids
        on companies.company_id = contact_company_ids.associated_company_id
    where contact_company_ids.associated_company_id is null
),

-- Combine both owner sources
unioned as (
    select * from contact_owners
    union all
    select * from company_only_owners
),

-- Entity type classification and flags
final as (
    select -- noqa: ST06
        -- surrogate key
        owner_sk,

        -- identifiers
        contact_id,
        company_id,
        owner_source,

        -- name
        first_name,
        last_name,
        full_name,

        -- contact info
        email,
        email_domain,
        phone,
        mobile_phone,
        job_title,

        -- company / legal entity
        company_name,
        case
            when
                lower(company_name) like '%llc%'
                or lower(company_name) like '%l.l.c%'
                then 'LLC'
            when
                lower(company_name) like '%trust%'
                or lower(company_name) like '%revocable%'
                or lower(company_name) like '%irrevocable%'
                or lower(company_name) like '% tr'
                or lower(company_name) like '% tr.'
                then 'Trust'
            when lower(company_name) like '%estate%'
                then 'Estate'
            when
                lower(company_name) like '% lp%'
                or lower(company_name) like '%l.p.%'
                then 'LP'
            when
                lower(company_name) like '%inc%'
                or lower(company_name) like '%corp%'
                then 'Corporation'
            when company_name is not null
                then 'Individual'
        end as entity_type,
        company_name is not null
        and (
            lower(company_name) like '%llc%'
            or lower(company_name) like '%l.l.c%'
        ) as is_llc,
        company_name is not null
        and (
            lower(company_name) like '%trust%'
            or lower(company_name) like '%revocable%'
            or lower(company_name) like '%irrevocable%'
            or lower(company_name) like '% tr'
            or lower(company_name) like '% tr.'
        ) as is_trust,

        -- address
        address,
        city,
        state,
        zip,
        country,
        is_address_from_company,

        -- financial / tax
        tax_status,
        is_tax_exempt,
        tax_id_number,
        revenue_payment_type,
        minimum_check_amount,
        dba_1099_name,

        -- operations
        owner_number_ogsys,
        acquired_owner_number,
        asset,
        is_company_active,
        is_working_interest_owner,
        is_revenue_checks_on_hold,
        statement_delivery,
        suspense_reason,

        -- revenue statement
        rev_statement_delivery,
        rev_statement_email,

        -- status
        lifecycle_stage,
        hubspot_owner_id,
        has_company,

        -- dates
        created_at,
        last_modified_at,

        -- metadata
        current_timestamp() as _loaded_at

    from unioned
)

select * from final
