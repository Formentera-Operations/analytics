{{
    config(
        materialized='view'
    )
}}

WITH unitalloc AS (
    SELECT*
    FROM {{ ref('stg_prodview__daily_allocations') }}
),

compdowntime AS (
    SELECT*
    FROM {{ ref('stg_prodview__completion_downtimes') }}
),

compparam AS (
    SELECT *
    FROM {{ ref('stg_prodview__completion_parameters') }}
),

unitstatus AS (
    SELECT*
    FROM {{ ref('stg_prodview__status') }}
),

source AS (
    SELECT
        a."Allocated Gas Equivalent of HCLiq mcf"
        ,a."Allocated Gas mcf"
        ,a."Allocated HCLiq bbl"
        ,a."Allocated NGL bbl"
        ,a."Allocated Oil bbl"
        ,a."Allocated Sand bbl"
        ,a."Allocated Water bbl"
        ,a."Allocation Date"
        ,a."Allocation Day of Month"
        ,a."Allocation Factor Gas"
        ,a."Allocation Factor HCLiq"
        ,a."Allocation Factor Sand"
        ,a."Allocation Factor Water"
        ,a."Allocation Month"
        ,a."Allocation Record ID"
        ,a."Allocation Year"
        ,p."Bottomhole Pressure psi"
        ,p."Bottomhole Temperature F"
        ,p."Casing Pressure psi"
        ,a."Change In Inventory Gas Equivalent Oil Cond mcf"
        ,a."Change In Inventory Oil Condensate bbl"
        ,a."Change In Inventory Sand bbl"
        ,a."Change In Inventory Water bbl"
        ,p."Choke Size 64ths"
        ,a."Closing Inventory Gas Equiv Oil Condensate mcf"
        ,a."Closing Inventory Oil Condensate bbl"
        ,a."Closing Inventory Sand bbl"
        ,a."Closing Inventory Water bbl"
        ,a."Created At"
        ,a."Created By"
        ,a."Cumulated Condensate bbl"
        ,a."Cumulated Gas mcf"
        ,a."Cumulated Hcliq bbl"
        ,a."Cumulated Ngl bbl"
        ,a."Cumulated Oil bbl"
        ,a."Cumulated Sand bbl"
        ,a."Cumulated Water bbl"
        ,a."Deferred Gas Production mcf"
        ,a."Deferred Oil Condensate Production bbl"
        ,a."Deferred Sand Production bbl"
        ,a."Deferred Water Production bbl"
        ,a."Difference From Target Condensate bbl"
        ,a."Difference From Target Gas mcf"
        ,a."Difference From Target Hcliq bbl"
        ,a."Difference From Target Ngl bbl"
        ,a."Difference From Target Oil bbl"
        ,a."Difference From Target Sand bbl"
        ,a."Difference From Target Water bbl"
        ,a."Disposed Allocated Flare Gas mcf"
        ,a."Disposed Allocated Fuel Gas mcf"
        ,a."Disposed Allocated Incineration Gas mcf"
        ,a."Disposed Allocated Injected Gas mcf"
        ,a."Disposed Allocated Injected Water bbl"
        ,a."Disposed Allocated Sales Condensate bbl"
        ,a."Disposed Allocated Sales Gas mcf"
        ,a."Disposed Allocated Sales Hcliq bbl"
        ,a."Disposed Allocated Sales Ngl bbl"
        ,a."Disposed Allocated Sales Oil bbl"
        ,a."Disposed Allocated Vent Gas mcf"
        ,d."Downtime Code 2"
        ,d."Downtime Code 3"
        ,d."Downtime Code"
        ,a."Downtime Hours"
        ,a."Downtime ID"
        ,p."Dynamic Viscosity Pascal Seconds"
        ,a."Gathered Gas mcf"
        ,a."Gathered HCLiq bbl"
        ,a."Gathered Sand bbl"
        ,a."Gathered Water bbl"
        ,p."H2s Daily Reading ppm"
        ,a."Injected Lift Gas bbl"
        ,a."Injected Load Oil Condensate bbl"
        ,a."Injected Load Water bbl"
        ,a."Injected Sand bbl"
        ,p."Injection Pressure psi"
        ,a."Injection Well Gas mcf"
        ,a."Injection Well Oil Cond bbl"
        ,a."Injection Well Sand bbl"
        ,a."Injection Well Water bbl"
        ,p."Kinematic Viscosity In2 Per S"
        ,GREATEST(
            NVL(a."Last Mod At", TO_TIMESTAMP_TZ('0000-01-01T00:00:00.000Z'))
            ,NVL(d."Last Mod At", TO_TIMESTAMP_TZ('0000-01-01T00:00:00.000Z'))
            ,NVL(p."Last Mod At", TO_TIMESTAMP_TZ('0000-01-01T00:00:00.000Z'))
            ,NVL(s."Last Mod At", TO_TIMESTAMP_TZ('0000-01-01T00:00:00.000Z'))
            ) AS "Last Mod At"
        ,a."Last Mod By"
        ,a."Last Param ID"
        ,a."Last Pump Entry ID"
        ,a."Last Pump Entry Table"
        ,a."Last Test ID"
        ,p."Line Pressure psi"
        ,a."Net Revenue Interest Gas pct"
        ,a."Net Revenue Interest Oil Cond pct"
        ,a."Net Revenue Interest Sand pct"
        ,a."Net Revenue Interest Water pct"
        ,a."New Production Condensate bbl"
        ,a."New Production Gas mcf"
        ,a."New Production HCLiq bbl"
        ,a."New Production Hcliq Gas Equivalent mcf"
        ,a."New Production Ngl bbl"
        ,a."New Production Oil bbl"
        ,a."New Production Sand bbl"
        ,a."New Production Water bbl"
        ,a."Opening Inventory Gas Equivalent Oil Cond mcf"
        ,a."Opening Inventory Oil Condensate bbl"
        ,a."Opening Inventory Sand bbl"
        ,a."Opening Inventory Water bbl"
        ,a."Operating Time Hours"
        ,p."PH Level"
        ,a."Pump Efficiency pct"
        ,a."Recovered Lift Gas mcf"
        ,a."Recovered Load Oil Condensate bbl"
        ,a."Recovered Load Water bbl"
        ,a."Recovered Sand bbl"
        ,a."Remaining Lift Gas mcf"
        ,a."Remaining Load Oil Condensate bbl"
        ,a."Remaining Load Water bbl"
        ,a."Remaining Sand bbl"
        ,a."Reporting Facility ID"
        ,p."Shut In Casing Pressure psi"
        ,p."Shut In Tubing Pressure psi"
        ,a."Starting Lift Gas mcf"
        ,a."Starting Load Oil Condensate bbl"
        ,a."Starting Load Water bbl"
        ,a."Starting Sand bbl"
        ,a."Status ID"
        ,s."Status"
        ,p."Tubing Pressure psi"
        ,a."Unit ID"
        ,p."Wellhead Pressure psi"
        ,p."Wellhead Temperature F"
        ,a."Working Interest Gas pct"
        ,a."Working Interest Oil Cond pct"
        ,a."Working Interest Sand pct"
        ,a."Working Interest Water pct"
        --,IFF(PVUNITALLOCMONTHDAY.IDRECPUMPENTRYTK = 'pvunitagreemt', 'AGREEMENT_V1', IFF(PVUNITALLOCMONTHDAY.IDRECPUMPENTRYTK = 'pvunitagreemtpartner', 'AGREEMENTPARTNER_V1', IFF(PVUNITALLOCMONTHDAY.IDRECPUMPENTRYTK = 'pvunitcompressor', 'COMPRESSOR_V1', IFF(PVUNITALLOCMONTHDAY.IDRECPUMPENTRYTK = 'pvunitcompressorentry', 'COMPRESSORENTRIES_V1', IFF(PVUNITALLOCMONTHDAY.IDRECPUMPENTRYTK = 'pvfacilitymonthdaycalc', 'DAILYFACILITY_V1', IFF(PVUNITALLOCMONTHDAY.IDRECPUMPENTRYTK = 'pvunitnodemonthdaycalc', 'DAILYNODE_V1', IFF(PVUNITALLOCMONTHDAY.IDRECPUMPENTRYTK = 'pvunittankmonthdaycalc', 'DAILYTANK_V1', IFF(PVUNITALLOCMONTHDAY.IDRECPUMPENTRYTK = 'pvunitequip', 'EQUIPMENT_V1', IFF(PVUNITALLOCMONTHDAY.IDRECPUMPENTRYTK = 'pvunitequipservice', 'EQUIPMENTSERVICE_V1', IFF(PVUNITALLOCMONTHDAY.IDRECPUMPENTRYTK = 'pvunitequipservicerec', 'EQUIPMENTSERVICEREC_V1', IFF(PVUNITALLOCMONTHDAY.IDRECPUMPENTRYTK = 'pvunitequipdowntm', 'EQUIPMENTDOWNTIME_V1', IFF(PVUNITALLOCMONTHDAY.IDRECPUMPENTRYTK = 'pvunitevent', 'EVENT_V1', IFF(PVUNITALLOCMONTHDAY.IDRECPUMPENTRYTK = 'pvunitevententry', 'EVENTENTRIES_V1', IFF(PVUNITALLOCMONTHDAY.IDRECPUMPENTRYTK = 'pvfacility', 'FACILITY_V1', IFF(PVUNITALLOCMONTHDAY.IDRECPUMPENTRYTK = 'pvfacrecdispcalc', 'FACILITYRECDISP_V1', IFF(PVUNITALLOCMONTHDAY.IDRECPUMPENTRYTK = 'pvflownetheader', 'FLOWNETWORK_V1', IFF(PVUNITALLOCMONTHDAY.IDRECPUMPENTRYTK = 'pvgasanalysis', 'GASANALYSES_V1', IFF(PVUNITALLOCMONTHDAY.IDRECPUMPENTRYTK = 'pvgasanalysiscomp', 'GASANALYSESCOMP_V1', IFF(PVUNITALLOCMONTHDAY.IDRECPUMPENTRYTK = 'pvgasanaly', 'GASANALYSISGROUP_V1', IFF(PVUNITALLOCMONTHDAY.IDRECPUMPENTRYTK = 'pvhcliqanalysis', 'HCLIQANALYSES_V1', IFF(PVUNITALLOCMONTHDAY.IDRECPUMPENTRYTK = 'pvhcliqanalysiscomp', 'HCLIQANALYSESCOMP_V1', IFF(PVUNITALLOCMONTHDAY.IDRECPUMPENTRYTK = 'pvhcliqanaly', 'HCLIQANALYSISGROUP_V1', IFF(PVUNITALLOCMONTHDAY.IDRECPUMPENTRYTK = 'pvunitmeterpdgas', 'METERGASPD_V1', IFF(PVUNITALLOCMONTHDAY.IDRECPUMPENTRYTK = 'pvunitmeterpdgasentry', 'METERGASPDENTRIES_V1', IFF(PVUNITALLOCMONTHDAY.IDRECPUMPENTRYTK = 'pvunitmeterliquid', 'METERLIQUID_V1', IFF(PVUNITALLOCMONTHDAY.IDRECPUMPENTRYTK = 'pvunitmeterliquidentry', 'METERLIQUIDENTRIES_V1', IFF(PVUNITALLOCMONTHDAY.IDRECPUMPENTRYTK = 'pvunitmeterliquidfact', 'METERLIQUIDFACTOR_V1', IFF(PVUNITALLOCMONTHDAY.IDRECPUMPENTRYTK = 'pvunitmeterorifice', 'METERORIFICE_V1', IFF(PVUNITALLOCMONTHDAY.IDRECPUMPENTRYTK = 'pvunitmeterorificeecf', 'METERORIFICEECF_V1', IFF(PVUNITALLOCMONTHDAY.IDRECPUMPENTRYTK = 'pvunitmeterorificeentry', 'METERORIFICEENTRIES_V1', IFF(PVUNITALLOCMONTHDAY.IDRECPUMPENTRYTK = 'pvunitmeterrate', 'METERRATE_V1', IFF(PVUNITALLOCMONTHDAY.IDRECPUMPENTRYTK = 'pvunitmeterrateentry', 'METERRATEENTRIES_V1', IFF(PVUNITALLOCMONTHDAY.IDRECPUMPENTRYTK = 'pvunitnodemonthcalc', 'MONTHLYNODE_V1', IFF(PVUNITALLOCMONTHDAY.IDRECPUMPENTRYTK = 'pvunitmeaspt', 'OTHERMEASUREMENTPOINT_V1', IFF(PVUNITALLOCMONTHDAY.IDRECPUMPENTRYTK = 'pvunitmeasptentry', 'OTHERMEASUREMENTPOINTENTRY_V1', IFF(PVUNITALLOCMONTHDAY.IDRECPUMPENTRYTK = 'pvunitcomppumpesp', 'PUMPESP_V2', IFF(PVUNITALLOCMONTHDAY.IDRECPUMPENTRYTK = 'pvunitcomppumpespentry', 'PUMPESPENTRIES_V1', IFF(PVUNITALLOCMONTHDAY.IDRECPUMPENTRYTK = 'pvunitcomppumpjet', 'PUMPJET_V1', IFF(PVUNITALLOCMONTHDAY.IDRECPUMPENTRYTK = 'pvunitcomppumpjetentry', 'PUMPJETENTRIES_V1', IFF(PVUNITALLOCMONTHDAY.IDRECPUMPENTRYTK = 'pvunitcomppumppcp', 'PUMPPCP_V1', IFF(PVUNITALLOCMONTHDAY.IDRECPUMPENTRYTK = 'pvunitcomppumppcpentry', 'PUMPPCPENTRIES_V1', IFF(PVUNITALLOCMONTHDAY.IDRECPUMPENTRYTK = 'pvunitcomppumprod', 'PUMPROD_V1', IFF(PVUNITALLOCMONTHDAY.IDRECPUMPENTRYTK = 'pvunitcomppumprodentry', 'PUMPRODENTRIES_V1', IFF(PVUNITALLOCMONTHDAY.IDRECPUMPENTRYTK = 'pvunitregbodykey', 'REGBODYKEYS_V1', IFF(PVUNITALLOCMONTHDAY.IDRECPUMPENTRYTK = 'pvunitremark', 'REMARKS_V1', IFF(PVUNITALLOCMONTHDAY.IDRECPUMPENTRYTK = 'pvrespteam', 'RESPONSIBLETEAMS_V1', IFF(PVUNITALLOCMONTHDAY.IDRECPUMPENTRYTK = 'pvroutesetrouteuserid', 'ROUTEUSERS_V1', IFF(PVUNITALLOCMONTHDAY.IDRECPUMPENTRYTK = 'pvunitseal', 'SEAL_V1', IFF(PVUNITALLOCMONTHDAY.IDRECPUMPENTRYTK = 'pvunitsealentry', 'SEALENTRIES_V1', IFF(PVUNITALLOCMONTHDAY.IDRECPUMPENTRYTK = 'pvunittank', 'TANK_V1', IFF(PVUNITALLOCMONTHDAY.IDRECPUMPENTRYTK = 'pvunittankentry', 'TANKENTRIES_V1', IFF(PVUNITALLOCMONTHDAY.IDRECPUMPENTRYTK = 'pvunittankfactht', 'TANKHEIGHTFACTOR_V1', IFF(PVUNITALLOCMONTHDAY.IDRECPUMPENTRYTK = 'pvunittankstartinv', 'TANKSTARTINV_V1', IFF(PVUNITALLOCMONTHDAY.IDRECPUMPENTRYTK = 'pvunittankstrap', 'TANKSTRAP_V1', IFF(PVUNITALLOCMONTHDAY.IDRECPUMPENTRYTK = 'pvunittankstrapdata', 'TANKSTRAPDETAILS_V1', IFF(PVUNITALLOCMONTHDAY.IDRECPUMPENTRYTK = 'pvticket', 'TICKETS_V1', IFF(PVUNITALLOCMONTHDAY.IDRECPUMPENTRYTK = 'pvunit', 'UNIT_V2', IFF(PVUNITALLOCMONTHDAY.IDRECPUMPENTRYTK = 'pvunitallocmonthday', 'UNITDAILYALLOCEXTENDED_V1', IFF(PVUNITALLOCMONTHDAY.IDRECPUMPENTRYTK = 'pvunitdispmonthday', 'UNITDAILYDISP_V1', IFF(PVUNITALLOCMONTHDAY.IDRECPUMPENTRYTK = 'pvunitallocmonth', 'UNITMONTHLYALLOC_V1', IFF(PVUNITALLOCMONTHDAY.IDRECPUMPENTRYTK = 'pvunitnode', 'UNITNODE_V1', IFF(PVUNITALLOCMONTHDAY.IDRECPUMPENTRYTK = 'pvunitnodeanaly', 'UNITNODEANALY_V1', IFF(PVUNITALLOCMONTHDAY.IDRECPUMPENTRYTK = 'pvunitnodecorr', 'UNITNODECORR_V1', IFF(PVUNITALLOCMONTHDAY.IDRECPUMPENTRYTK = 'pvunitnodenetfact', 'UNITNODESHRINK_V1', IFF(PVUNITALLOCMONTHDAY.IDRECPUMPENTRYTK = 'pvunitothertag', 'UNITOTHERTAG_V1', IFF(PVUNITALLOCMONTHDAY.IDRECPUMPENTRYTK = 'pvroutesetrouteunit', 'UNITROUTE_V1', IFF(PVUNITALLOCMONTHDAY.IDRECPUMPENTRYTK = 'pvunitcompgathmonthdaycalc', 'WELLDAILYGATHERED_V1', IFF(PVUNITALLOCMONTHDAY.IDRECPUMPENTRYTK = 'pvunitcompdowntm', 'WELLDOWNTIME_V1', IFF(PVUNITALLOCMONTHDAY.IDRECPUMPENTRYTK = 'pvunitcompfluidlevel', 'WELLFLUIDLEVEL_V1', IFF(PVUNITALLOCMONTHDAY.IDRECPUMPENTRYTK = 'pvunitcompwhcut', 'WELLHEADCUT_V1', IFF(PVUNITALLOCMONTHDAY.IDRECPUMPENTRYTK = 'pvunitcompmeasmeth', 'WELLMEASMETHOD_V1', IFF(PVUNITALLOCMONTHDAY.IDRECPUMPENTRYTK = 'pvunitcompgathmonthcalc', 'WELLMONTHLYGATHERED_V1', IFF(PVUNITALLOCMONTHDAY.IDRECPUMPENTRYTK = 'pvunitcompparam', 'WELLPARAM_V1', IFF(PVUNITALLOCMONTHDAY.IDRECPUMPENTRYTK = 'pvunitcompratios', 'WELLRATIOS_V1', IFF(PVUNITALLOCMONTHDAY.IDRECPUMPENTRYTK = 'pvunitcomptestreqexcalc', 'WELLREQUIREDTESTSREMAINING_V1', IFF(PVUNITALLOCMONTHDAY.IDRECPUMPENTRYTK = 'pvunitcompstatus', 'WELLSTATUS_V1', IFF(PVUNITALLOCMONTHDAY.IDRECPUMPENTRYTK = 'pvunitcomptarget', 'WELLTARGET_V1', IFF(PVUNITALLOCMONTHDAY.IDRECPUMPENTRYTK = 'pvunitcomptargetday', 'WELLTARGETDAILY_V1', IFF(PVUNITALLOCMONTHDAY.IDRECPUMPENTRYTK = 'pvunitcomptest', 'WELLTEST_V1', IFF(PVUNITALLOCMONTHDAY.IDRECPUMPENTRYTK = 'pvunitcomptestreq', 'WELLTESTINGREQUIREMENT_V1', IFF(PVUNITALLOCMONTHDAY.IDRECPUMPENTRYTK = 'pvunitcompzone', 'WELLZONE_V1', IFF(PVUNITALLOCMONTHDAY.IDRECPUMPENTRYTK = 'pvunitcompcmnglratio', 'WELLZONERATIO_V1', '')))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))) AS PUMPTK
    FROM  unitalloc a
    LEFT JOIN  compdowntime d 
        ON a."Downtime ID" = d."Completion Downtime ID"
    LEFT JOIN compparam p 
        ON a."Last Param ID" = p."Completion Parameter ID"
    LEFT JOIN unitstatus s 
        ON a."Status ID" = s."Status Record ID"
)

SELECT *
FROM source