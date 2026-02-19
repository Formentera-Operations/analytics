{{
    config(
        materialized='incremental',
        unique_key='gl_id',
        on_schema_change='sync_all_columns',
        incremental_strategy='merge',
        cluster_by=['company_code', 'journal_date', 'main_account', 'sub_account'],
        snowflake_warehouse=set_warehouse_size('M') if target.name in ['prod', 'ci'] else target.warehouse
    )
}}

with source_gl as (
    select *
    from {{ ref('stg_oda__gl') }}
),

vouchers as (
    select *
    from {{ ref('stg_oda__voucher_v2') }}
),

accounts as (
    select *
    from {{ ref('stg_oda__account_v2') }}
),

companies as (
    select *
    from {{ ref('stg_oda__company_v2') }}
),

entities as (
    select *
    from {{ ref('stg_oda__entity_v2') }}
),

owners as (
    select *
    from {{ ref('stg_oda__owner_v2') }}
),

purchasers as (
    select *
    from {{ ref('stg_oda__purchaser_v2') }}
),

vendors as (
    select *
    from {{ ref('stg_oda__vendor_v2') }}
),

wells as (
    select *
    from {{ ref('stg_oda__wells') }}
),

userfields as (
    select
        id,
        user_field_name,
        user_field_value_string
    from {{ ref('stg_oda__userfield') }}
    where
        user_field_name = 'UF-SEARCH KEY'
        and entity_type_id = 2  -- Wells only
    qualify row_number() over (
        partition by id
        order by user_field_identity desc
    ) = 1
),

source_modules as (
    select *
    from {{ ref('stg_oda__source_module') }}
),

rev_deck_revisions as (
    select *
    from {{ ref('stg_oda__revenue_deck_revision') }}
),

rev_decks as (
    select *
    from {{ ref('stg_oda__revenue_deck_v2') }}
),

rev_deck_sets as (
    select *
    from {{ ref('stg_oda__revenue_deck_set') }}
),

payment_types as (
    select *
    from {{ ref('stg_oda__payment_type') }}
),

afes as (
    select *
    from {{ ref('stg_oda__afe_v2') }}
),

exp_deck_revisions as (
    select *
    from {{ ref('stg_oda__expense_deck_revision') }}
),

exp_decks as (
    select *
    from {{ ref('stg_oda__expense_deck_v2') }}
),

exp_deck_sets as (
    select *
    from {{ ref('stg_oda__expense_deck_set') }}
),

recon_types as (
    select *
    from {{ ref('stg_oda__gl_reconciliation_type') }}
),

location_codes as (
    -- Pre-process all location codes to ensure consistent type
    select
        gld.id as gl_id,
        cast(loc_company.code as varchar) as company_code,
        cast(loc_owner_entity.code as varchar) as owner_entity_code,
        cast(loc_purchaser_entity.code as varchar) as purchaser_entity_code,
        cast(loc_vendor_entity.code as varchar) as vendor_entity_code,
        cast(loc_well.code as varchar) as well_code
    from source_gl as gld
    left outer join companies as loc_company
        on gld.location_company_id = loc_company.id
    left outer join owners as loc_owner
        on gld.location_owner_id = loc_owner.id
    left outer join entities as loc_owner_entity
        on loc_owner.entity_id = loc_owner_entity.id
    left outer join purchasers as loc_purchaser
        on gld.location_purchaser_id = loc_purchaser.id
    left outer join entities as loc_purchaser_entity
        on loc_purchaser.entity_id = loc_purchaser_entity.id
    left outer join vendors as loc_vendor
        on gld.location_vendor_id = loc_vendor.id
    left outer join entities as loc_vendor_entity
        on loc_vendor.entity_id = loc_vendor_entity.id
    left outer join wells as loc_well
        on gld.location_well_id = loc_well.id
),

location_names as (
    -- Pre-process all location names to ensure consistent type
    select
        gld.id as gl_id,
        cast(loc_company.name as varchar) as company_name,
        cast(loc_owner_entity.name as varchar) as owner_entity_name,
        cast(loc_purchaser_entity.name as varchar) as purchaser_entity_name,
        cast(loc_vendor_entity.name as varchar) as vendor_entity_name,
        cast(loc_well.name as varchar) as well_name
    from source_gl as gld
    left outer join companies as loc_company
        on gld.location_company_id = loc_company.id
    left outer join owners as loc_owner
        on gld.location_owner_id = loc_owner.id
    left outer join entities as loc_owner_entity
        on loc_owner.entity_id = loc_owner_entity.id
    left outer join purchasers as loc_purchaser
        on gld.location_purchaser_id = loc_purchaser.id
    left outer join entities as loc_purchaser_entity
        on loc_purchaser.entity_id = loc_purchaser_entity.id
    left outer join vendors as loc_vendor
        on gld.location_vendor_id = loc_vendor.id
    left outer join entities as loc_vendor_entity
        on loc_vendor.entity_id = loc_vendor_entity.id
    left outer join wells as loc_well
        on gld.location_well_id = loc_well.id
),

entity_codes as (
    -- Pre-process all entity codes to ensure consistent type
    select
        gld.id as gl_id,
        cast(ent_company.code as varchar) as company_code,
        cast(ent_owner_entity.code as varchar) as owner_entity_code,
        cast(ent_purchaser_entity.code as varchar) as purchaser_entity_code,
        cast(ent_vendor_entity.code as varchar) as vendor_entity_code
    from source_gl as gld
    left outer join companies as ent_company
        on gld.entity_company_id = ent_company.id
    left outer join owners as ent_owner
        on gld.entity_owner_id = ent_owner.id
    left outer join entities as ent_owner_entity
        on ent_owner.entity_id = ent_owner_entity.id
    left outer join purchasers as ent_purchaser
        on gld.entity_purchaser_id = ent_purchaser.id
    left outer join entities as ent_purchaser_entity
        on ent_purchaser.entity_id = ent_purchaser_entity.id
    left outer join vendors as ent_vendor
        on gld.entity_vendor_id = ent_vendor.id
    left outer join entities as ent_vendor_entity
        on ent_vendor.entity_id = ent_vendor_entity.id
),

entity_names as (
    -- Pre-process all entity names to ensure consistent type
    select
        gld.id as gl_id,
        cast(ent_company.name as varchar) as company_name,
        cast(ent_owner_entity.name as varchar) as owner_entity_name,
        cast(ent_purchaser_entity.name as varchar) as purchaser_entity_name,
        cast(ent_vendor_entity.name as varchar) as vendor_entity_name
    from source_gl as gld
    left outer join companies as ent_company
        on gld.entity_company_id = ent_company.id
    left outer join owners as ent_owner
        on gld.entity_owner_id = ent_owner.id
    left outer join entities as ent_owner_entity
        on ent_owner.entity_id = ent_owner_entity.id
    left outer join purchasers as ent_purchaser
        on gld.entity_purchaser_id = ent_purchaser.id
    left outer join entities as ent_purchaser_entity
        on ent_purchaser.entity_id = ent_purchaser_entity.id
    left outer join vendors as ent_vendor
        on gld.entity_vendor_id = ent_vendor.id
    left outer join entities as ent_vendor_entity
        on ent_vendor.entity_id = ent_vendor_entity.id
)


select
    -- Metadata and timestamps
    gld.id as gl_id,
    c.code as company_code,
    c.name as company_name,
    acct.main_account as main_account,

    -- Company and account info
    acct.sub_account as sub_account,
    acct.account_name as account_name,
    cast(afes.afe_type_id as varchar) as afe_type_id,
    cast(afes.afe_type_code as varchar) as afe_type_code,
    cast(afes.afe_type_label as varchar) as afe_type_label,

    -- AFE Type Classification
    cast(afes.afe_type_full_name as varchar) as afe_type_full_name,
    gld.accrual_date_key as accrual_date_key,
    gld.journal_date as journal_date_key,
    cast(gld.source_module_code as varchar) as source_module_code,

    -- Location information with CASE logic
    cast(source_modules.name as varchar) as source_module_name,

    -- Use the pre-processed location code and name
    cast(vouchers.code as varchar) as voucher_code,

    cast(vouchers.voucher_type_id as varchar) as voucher_type_id,

    -- Posted information
    cast(wells.property_reference_code as varchar) as op_ref,
    cast(wells.code as varchar) as well_code,
    wells.id as well_id,
    cast(wells.name as varchar) as well_name,

    -- Date dimensions - Accrual
    cast(loc_search.user_field_value_string as varchar) as search_key,
    gld.is_include_in_journal_report as include_in_journal_report,
    gld.is_present_in_journal_balance as present_in_journal_balance,
    gld.is_include_in_cash_report as include_in_cash_report,
    gld.is_present_in_cash_balance as present_in_cash_balance,

    -- Date dimensions - Cash
    gld.is_include_in_accrual_report as include_in_accrual_report,
    gld.is_present_in_accrual_balance as present_in_accrual_balance,
    cast(rev_deck_revisions.revision_number as varchar) as revenue_deck_change_code,
    cast(rev_deck_revisions.nri_actual as varchar) as net_revenue_interest_actual,

    -- Date dimensions - Journal
    cast(rev_deck_revisions.total_interest_expected as varchar) as total_interest_expected,
    cast(payment_types.code as varchar) as reference_type,
    cast(gld.reference as varchar) as reference,
    cast(c.code as varchar) as currency_code,
    cast(afes.code as varchar) as afe_code,

    -- Source and reference information
    cast(coalesce(
        'C: ' || ec.company_code,
        'O: ' || ec.owner_entity_code,
        'P: ' || ec.purchaser_entity_code,
        'V: ' || ec.vendor_entity_code
    ) as varchar) as entity,
    cast(coalesce(
        en.company_name,
        en.owner_entity_name,
        en.purchaser_entity_name,
        en.vendor_entity_name
    ) as varchar) as entity_name,
    cast(gld.description as varchar) as gl_description,
    cast(exp_deck_sets.code as varchar) as expense_deck,
    cast(exp_deck_revisions.revision_number as varchar) as expense_deck_change_code,
    cast(recon_types.code as varchar) as reconciliation_type,
    gld.is_reconciled_trial as reconciled_trial,
    gld.is_reconciled as reconciled,
    gld.is_generated_entry as generated_entry,

    -- Report inclusion flags
    cast(gld.entry_group as varchar) as entry_group,
    gld.ordinal as entry_seq,
    cast(convert_timezone('UTC', 'America/Chicago', current_timestamp()) as timestamp_tz) as last_refresh_time,
    cast(gld.created_at as timestamp_ntz) as created_date,
    cast(gld.updated_at as timestamp_ntz) as updated_date,
    cast(case
        when lc.company_code is not null then 'Company'
        when lc.owner_entity_code is not null then 'Owner'
        when lc.purchaser_entity_code is not null then 'Purchaser'
        when lc.vendor_entity_code is not null then 'Vendor'
        when lc.well_code is not null then 'Well'
    end as varchar) as location_type,

    -- Revenue deck information
    cast(coalesce(
        lc.company_code,
        lc.owner_entity_code,
        lc.purchaser_entity_code,
        lc.vendor_entity_code,
        lc.well_code
    ) as varchar) as location_code,
    cast(coalesce(
        ln.company_name,
        ln.owner_entity_name,
        ln.purchaser_entity_name,
        ln.vendor_entity_name,
        ln.well_name
    ) as varchar) as location_name,
    cast(case when gld.is_posted then 'Y' else 'N' end as varchar) as posted,
    cast(vouchers.posted_date as timestamp_ntz) as posted_date_time,

    -- Reference and currency
    cast(convert_timezone('UTC', 'America/Chicago', vouchers.posted_date) as timestamp_tz) as posted_date_time_cst,
    cast(to_char(vouchers.posted_date, 'MM-DD-YYYY') as varchar) as posted_date,
    cast(gld.accrual_date as date) as accrual_date,
    cast(to_char(gld.accrual_date, 'MM') as varchar) as accrual_month,

    -- Entity information using pre-processed entity data
    cast(to_char(gld.accrual_date, 'YYYY') as varchar) as accrual_year,

    cast(to_char(gld.accrual_date, 'MM-YYYY') as varchar) as accrual_month_year,

    -- Transaction values
    cast(gld.cash_date as date) as cash_date,
    cast(to_char(gld.cash_date, 'MM') as varchar) as cash_month,
    cast(to_char(gld.cash_date, 'YYYY') as varchar) as cash_year,
    cast(to_char(gld.cash_date, 'MM-YYYY') as varchar) as cash_month_year,
    cast(gld.journal_date as date) as journal_date,

    -- Expense deck information
    cast(to_char(gld.journal_date, 'MM') as varchar) as journal_month,
    cast(to_char(gld.journal_date, 'YYYY') as varchar) as journal_year,
    cast(to_char(gld.journal_date, 'MM-YYYY') as varchar) as journal_month_year,
    cast(rev_decks.effective_date as date) as revenue_deck_version_date,

    -- Reconciliation information
    cast(gld.gross_value as float) as gross_amount,
    cast(gld.net_value as float) as net_amount,
    cast(gld.gross_volume as float) as gross_volume,

    -- Entry metadata
    cast(gld.net_volume as float) as net_volume,
    cast(exp_decks.effective_date as date) as expense_deck_version_date,
    cast(exp_deck_revisions.total_interest_actual as float) as expense_deck_interest_total

from source_gl as gld
left outer join companies as c
    on gld.company_id = c.id
left outer join accounts as acct
    on gld.account_id = acct.id

-- Use the pre-processed location data
left outer join location_codes lc
    on gld.id = lc.gl_id
left outer join location_names ln
    on gld.id = ln.gl_id

-- Use the pre-processed entity data
left outer join entity_codes ec
    on gld.id = ec.gl_id
left outer join entity_names en
    on gld.id = en.gl_id

-- Other joins
left outer join source_modules
    on gld.source_module_code = source_modules.code
left outer join vouchers
    on gld.voucher_id = vouchers.id
left outer join wells
    on gld.well_id = wells.id
left outer join userfields as loc_search
    on gld.location_well_id = loc_search.id
left outer join rev_deck_revisions
    on gld.source_revenue_deck_revision_id = rev_deck_revisions.id
left outer join rev_decks
    on rev_deck_revisions.deck_id = rev_decks.id
left outer join rev_deck_sets
    on rev_decks.deck_set_id = rev_deck_sets.id
left outer join payment_types
    on gld.payment_type_id = payment_types.id
left outer join afes
    on gld.afe_id = afes.id
left outer join exp_deck_revisions
    on gld.source_expense_deck_revision_id = exp_deck_revisions.id
left outer join exp_decks
    on exp_deck_revisions.deck_id = exp_decks.id
left outer join exp_deck_sets
    on exp_decks.deck_set_id = exp_deck_sets.id
left outer join recon_types
    on gld.reconciliation_type_id = recon_types.id

where
    1 = 1
    {% if is_incremental() %}
    -- Only process new or updated GL entries since last run
        and (
            gld.created_at > (select max(created_date) from {{ this }})
            or gld.updated_at > (select max(updated_date) from {{ this }})
        )
    {% endif %}
