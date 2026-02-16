{{
    config(
        materialized='table',
        tags=['orm', 'mart', 'formentera']
    )
}}

{#
    Dimension: Owner 360

    Purpose: Contact-centric owner dimension for Owner Relationship Management.
    Each mineral royalty owner is represented as a contact, with company (legal entity)
    fields denormalized for convenience.

    Grain: One row per contact (contact_id)

    Sources:
    - stg_hubspot__contacts (primary)
    - stg_hubspot__companies (left join via associated_company_id)

    Entity type classification is derived from company_name patterns:
    - LLC: name contains 'llc' or 'l.l.c'
    - Trust: name contains 'trust', 'revocable', or 'irrevocable'
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

joined as (
    select
        -- === Owner Identity ===
        contacts.contact_sk as owner_sk,
        contacts.contact_id,
        contacts.associated_company_id as company_id,

        -- contact name
        contacts.first_name,
        contacts.last_name,
        contacts.email,

        -- contact info
        contacts.email_domain,
        contacts.phone,
        contacts.mobile_phone,
        contacts.job_title,
        companies.company_name,

        -- === Company / Legal Entity (denormalized) ===
        companies.tax_status,

        -- entity type classification
        companies.is_tax_exempt,

        companies.tax_id_number,

        companies.revenue_payment_type,

        -- === Address (prefer company address, fall back to contact) ===
        companies.minimum_check_amount,
        companies.dba_1099_name,
        companies.owner_number_ogsys,
        companies.acquired_owner_number,
        companies.asset,
        companies.is_active as is_company_active,

        -- === Financial / Tax (from company) ===
        companies.is_working_interest_owner,
        companies.is_revenue_checks_on_hold,
        companies.statement_delivery,
        companies.suspense_reason,
        contacts.rev_statement_delivery,
        contacts.rev_statement_email,

        -- === Operations (from company) ===
        contacts.lifecycle_stage,
        contacts.hubspot_owner_id,
        contacts.created_at,
        contacts.last_modified_at,
        coalesce(
            nullif(trim(contacts.first_name || ' ' || contacts.last_name), ''),
            contacts.last_name,
            contacts.first_name,
            contacts.email
        ) as full_name,
        case
            when
                lower(companies.company_name) like '%llc%'
                or lower(companies.company_name) like '%l.l.c%'
                then 'LLC'
            when
                lower(companies.company_name) like '%trust%'
                or lower(companies.company_name) like '%revocable%'
                or lower(companies.company_name) like '%irrevocable%'
                or lower(companies.company_name) like '% tr'
                or lower(companies.company_name) like '% tr.'
                then 'Trust'
            when lower(companies.company_name) like '%estate%'
                then 'Estate'
            when
                lower(companies.company_name) like '% lp%'
                or lower(companies.company_name) like '%l.p.%'
                then 'LP'
            when
                lower(companies.company_name) like '%inc%'
                or lower(companies.company_name) like '%corp%'
                then 'Corporation'
            when companies.company_name is not null
                then 'Individual'
        end as entity_type,
        companies.company_name is not null
        and (
            lower(companies.company_name) like '%llc%'
            or lower(companies.company_name) like '%l.l.c%'
        ) as is_llc,
        companies.company_name is not null
        and (
            lower(companies.company_name) like '%trust%'
            or lower(companies.company_name) like '%revocable%'
            or lower(companies.company_name) like '%irrevocable%'
            or lower(companies.company_name) like '% tr'
            or lower(companies.company_name) like '% tr.'
        ) as is_trust,

        -- === Revenue Statement (from contact) ===
        coalesce(companies.address_line_one, contacts.address) as address,
        coalesce(companies.city, contacts.city) as city,

        -- === Status ===
        coalesce(companies.state, contacts.state) as state,
        coalesce(companies.zip, contacts.zip) as zip,
        coalesce(companies.country, contacts.country) as country,

        -- === Dates ===
        companies.address_line_one is not null as is_address_from_company,
        contacts.associated_company_id is not null as has_company,

        -- === Metadata ===
        current_timestamp() as _loaded_at

    from contacts
    left join companies
        on contacts.associated_company_id = companies.company_id
)

select * from joined
