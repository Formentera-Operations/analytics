{{ config(
    enabled=true,
    materialized='view'
) }}

{#
    Dimension: Company NRI
    
    Purpose: FP Company NRI per Revenue Deck by Latest Version, Latest Effective Date, Deck Code =1
    Grain: One company interest record per well
    
    Use cases:
    - Review latests net revenue interest
    
    Sources:
    - stg_oda__revenue_deck_participant
    - stg_oda__revenue_deck_revision
    - stg_oda__revenue_deck_set
    - stg_oda__revenue_deck_v2
    - stg_oda__company_v2
    - stg_oda__wells
    - stg_oda__interest_type
    - stg_oda__revision_state
#}

with revenuedeck_company as (

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
    rdr.name as deck_name,
    rd.effective_date as latest_effective_date,
    rdr.revision_number as revision_number,
    case 
            when (rdpc.code is null) then null
            else concat(rdpc.code, ': ', rdpc.name) 
            end as company_code_name,

    -- =================================================================
        -- Interest & Product
    -- =================================================================       
    p.name as product_name,
    --case   
    --   when it.id = 1 then 'Working'
    --    when it.id = 2 then 'Royalty'
     --   when it.id = 3 then 'Override'
     --   end as interest_name,
     'NRI' as interest_type,
    cast(sum(rdp.decimal_interest) as Decimal(12,10)) as total_interest,
    
    -- =================================================================
        -- Change Dates
    -- =================================================================               
    rdr.create_date as create_date,
    rdr.update_date as update_date
   

    from {{ ref('stg_oda__revenue_deck_participant') }} rdp
    inner join {{ ref('stg_oda__revenue_deck_revision') }} rdr
        on rdr.id = rdp.deck_revision_id
    inner join {{ ref('stg_oda__revenue_deck_v2') }} rd
        on rd.id = rdr.deck_id
    inner join {{ ref('stg_oda__revenue_deck_set') }} rds
        on  rds.id = rd.deck_set_id
    left join {{ ref('stg_oda__wells') }} w
        on w.id = rds.well_id
    left join {{ ref('stg_oda__company_v2') }} rdpc
        on rdpc.id = rdp.company_id
    left join {{ ref('stg_oda__interest_type') }} it
        on it.id = rdp.interest_type_id
    left join {{ ref('stg_oda__product') }} p
        on p.id = rds.product_id
    left join {{ ref('stg_oda__revision_state') }} rs
        on rs.id = rdr.revision_state_id


    where
    rdr.import_data_id is null --Quorum Datahub Added Filter to Remove Duplication of Latest Version Records
    and rds.company_id = '57d809a6-7302-ee11-bd5d-f40669ee7a09'  --Company 200
	and rs.id = '1' --Latest Deck Version
	and rdr.close_date is null --Deck is not Closed
	and rdpc.id is not null --Company Participant only
	and rds.code = '1' --Deck Code 1 
	and rd.effective_date = (select max(effdaterd.effective_date)   --Latest Effective Date of Deck 
							from {{ ref('stg_oda__revenue_deck_v2') }} effdaterd
							where effdaterd.deck_set_id = rd.deck_set_id)


     group by
        w.code,
        w.name,
        w.api_number,
        w.state_code,
        w.county_name,
        rdr.name,
        rd.effective_date,
        rdr.revision_number,
        rdpc.code,
        rdpc.name,
        --it.id,
        p.name,
        rdr.create_date,
        rdr.update_date   
    
    order by
    w.code,
    w.name,
    p.name
    
     
)

select * from revenuedeck_company



