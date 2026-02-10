{{
    config(
        materialized='table',
        tags=['marts', 'finance', 'dimension']
    )
}}

{#
    Dimension: Accounting - Accounts Payable Check Register
    
    
    Sources:
    - stg_oda__apcheck
    - stg_oda__payment_type
    - stg_oda__vendor_v2
    - stg_oda__voucher_v2
    - stg_oda__entity_v2
    - stg_oda__company_v2
#}


with

ap_checks as (

    select
        a.transaction_number,
        a.company_id,
        a.issued_date,
        a.payment_type_code,
        a.system_generated,
        a.payment_amount,
        a.reconciled,
        a.voided_date,
        a.payment_type_id,
        a.vendor_id,
        a.voucher_id
    from {{ ref('stg_oda__apcheck') }} as a

),

payment_types as (

    select
        id,
        name
    from {{ ref('stg_oda__payment_type') }}

),

vendors as (

    select
        id,
        entity_id
    from {{ ref('stg_oda__vendor_v2') }}

),

vouchers as (

    select
        id,
        code
    from {{ ref('stg_oda__voucher_v2') }}

),

entities as (

    select
        id,
        code,
        name
    from {{ ref('stg_oda__entity_v2') }}

),

companies as (

    select
        id,
        code,
        name
    from {{ ref('stg_oda__company_v2') }}

),


final as (

    select
        c.code                                                             as company_code,
        c.name                                                             as company_name,
        ac.transaction_number                                              as check_number,
        ac.issued_date                                                     as check_date,
        ac.payment_type_code                                               as check_type,
        concat(case when ac.system_generated = 0 then 'Manual ' else 'System ' end, pt.name)       as check_type_name,
        ac.payment_amount                                                  as check_amount,
        vo.code                                                            as voucher_code,
        e.code                                                             as entity_code,
        e.name                                                             as entity_name,
        case when ac.reconciled = '1' then 'YES' else '' end              as reconciled,
        case when ac.voided_date is null then 'NO' else 'YES' end         as voided,
        cast(ac.voided_date as date)                           as void_date
    from ap_checks as ac
    inner join companies as c
        on ac.company_id = c.id
    inner join payment_types as pt
        on pt.id = ac.payment_type_id
    left join vendors as v
        on v.id = ac.vendor_id
    left join vouchers as vo
        on vo.id = ac.voucher_id
    inner join entities as e
        on e.id = v.entity_id

)

select * from final