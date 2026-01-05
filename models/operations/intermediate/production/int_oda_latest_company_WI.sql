{{ config(
    enabled=true,
    materialized='view'
) }}

{#
    Dimension: Company WI
    
    Purpose: FP Company WI per Expense Deck by Latest Version, Latest Effective Date, Deck Code =1
    Grain: One company interest record per well
    
    Use cases:
    - Review latests working interest
    
    Sources:
    - stg_oda__expense_deck_participant
    - stg_oda__expense_deck_revision
    - stg_oda__expense_deck_set
    - stg_oda__expense_deck_v2
    - stg_oda__company_v2
    - stg_oda__wells
    - stg_oda__interest_type
    - stg_oda__revision_state
#}

with expensedeck_company as (

    select
    
     -- =================================================================
        -- Well Data
     -- =================================================================
    w.code as well_code,
    right(w.code, 6) as eid,
    w.name as well_name,
    w.api_number as api_number, 
    w.state_code as state_code,
    w.county_name as county_name,
    
     -- =================================================================
        -- Deck Code, Effective Date, Revision, Company
     -- =================================================================
    edr.name as deck_name,
    ed.effective_date as latest_effective_date,
    edr.revision_number as revision_number,
    case 
            when (edpc.code is null) then null
            else concat(edpc.code, ': ', edpc.name) 
            end as company_code_name,

    -- =================================================================
        -- Interest
    -- =================================================================       
    --case   
    --    when it.id = 1 then 'Working'
    --    when it.id = 2 then 'Royalty'
    --    when it.id = 3 then 'Override'
    --   end as interest_name,
    'WI' as interest_type,
    cast(sum(edp.decimal_interest) * 100 as Decimal(12,8)) as total_interest,
    
    -- =================================================================
        -- Change Dates
    -- =================================================================               
    edr.create_date as create_date,
    edr.update_date as update_date
   

    from {{ ref('stg_oda__expense_deck_participant') }} edp
    inner join {{ ref('stg_oda__expense_deck_revision') }} edr
        on edr.id = edp.deck_revision_id
    inner join {{ ref('stg_oda__expense_deck_v2') }} ed
        on ed.id = edr.deck_id
    inner join {{ ref('stg_oda__expense_deck_set') }} eds
        on  eds.id = ed.deck_set_id
    left join {{ ref('stg_oda__wells') }} w
        on w.id = eds.well_id
    left join {{ ref('stg_oda__company_v2') }} edpc
        on edpc.id = edp.company_id
    left join {{ ref('stg_oda__interest_type') }} it
        on it.id = edp.interest_type_id
    left join {{ ref('stg_oda__revision_state') }} rs
        on rs.id = edr.revision_state_id


    where
       -- edr.ImportDataId is null --Quorum Datahub Added Filter to Remove Duplication of Latest Version Records
	--and 
    eds.company_id = '57d809a6-7302-ee11-bd5d-f40669ee7a09'  --Company 200
	and rs.id = '1' --Latest Deck Version
	and edr.close_date is null --Deck is not Closed
	and edpc.id is not null --Company Participant only
	and eds.code = '1' --Deck Code 1 
	and ed.effective_date = (select max(effdateed.effective_date)   --Latest Effective Date of Deck 
							from {{ ref('stg_oda__expense_deck_v2') }} effdateed
							where effdateed.deck_set_id = ed.deck_set_id)

     group by
        w.code,
        w.name,
        w.api_number,
        w.state_code,
        w.county_name,
        edr.name,
        ed.effective_date,
        edr.revision_number,
        edpc.code,
        edpc.name,
        it.id,
        edr.create_date,
        edr.update_date   
        
)

select * from expensedeck_company



