{#
    Dimension: Accounting - Accounts Revenue Check Register
    

    Sources:
    - stg_oda__revenuecheck
    - stg_oda__payment_type
    - stg_oda__owner_v2
    - stg_oda__voucher_v2
    - stg_oda__entity_v2
    - stg_oda__company_v2
#}


with

revenue_checks as (

    select
        c.transaction_number,
        c.company_id,
        c.issued_date,
        c.payment_type_code,
        c.system_generated,
        c.check_amount,
        c.reconciled,
        c.voided_date,
        c.payment_type_id,
        c.owner_id,
        c.voucher_id
    from {{ ref('stg_oda__checkrevenue') }} as c

),

payment_types as (

    select
        id,
        name
    from {{ ref('stg_oda__payment_type') }}

),

owners as (

    select
        id,
        entity_id
    from {{ ref('stg_oda__owner_v2') }}

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
        co.code                                                            as company_code,
        co.name                                                            as company_name,
        rc.transaction_number                                              as check_number,
        rc.issued_date                                                     as check_date,
        rc.payment_type_code                                               as check_type,
        concat(case when rc.system_generated = 0 then 'Manual ' else 'System ' end, pt.name)       as check_type_name,
        rc.check_amount,
        vo.code                                                            as voucher_code,
        e.code                                                             as entity_code,
        e.name                                                             as entity_name,
        case when rc.reconciled = '1' then 'YES' else '' end              as reconciled,
        case when rc.voided_date is null then 'NO' else 'YES' end         as voided,
        cast(rc.voided_date as date)                           as void_date
    from revenue_checks as rc
    inner join companies as co
        on rc.company_id = co.id
    inner join payment_types as pt
        on pt.id = rc.payment_type_id
    left join owners as o
        on o.id = rc.owner_id
    left join vouchers as vo
        on vo.id = rc.voucher_id
    inner join entities as e
        on e.id = o.entity_id

)

select * from final