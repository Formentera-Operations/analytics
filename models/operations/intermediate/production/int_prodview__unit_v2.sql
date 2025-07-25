{{
  config(
    full_refresh=false,
    materialized='view',
    alias='unit_v2'
  )
}}

with pvunit as (
    select * from {{ ref('stg_prodview__units') }}
    where DELETED = false
),

pvroutesetroute as (
    select * from {{ ref('stg_prodview__routes') }}
    where DELETED = false
),

pvunitcomp as (
    select * from {{ ref('stg_prodview__completions') }}
    where DELETED = false
),

svintegration as (
    select * 
    from {{ ref('stg_prodview__system_integrations') }}
    where DELETED = false 
    and AFPRODUCT = 'SiteView' 
    and TBLKEYPARENT = 'pvunit'
),

wvcompintegration as (
    select * 
    from {{ ref('stg_prodview__system_integrations') }}
    where DELETED = false 
    and AFPRODUCT = 'WellView' 
    and TBLKEYPARENT = 'pvunitcomp'
),

wvintegration as (
    select * 
    from {{ ref('stg_prodview__system_integrations') }}
    where DELETED = false 
    and AFPRODUCT = 'WellView' 
    and TBLKEYPARENT = 'pvunit'
)

select
    -- Route information
    r.route_name as "Route Name",
    r.USERTXT1 as user_text_1,
    r.USERTXT2 as user_text_2,
    r.USERTXT3 as user_text_3,

    -- Unit information
    u.AREA,
    u.COM,
    u.COSTCENTERIDA,
    u.COSTCENTERIDB,
    u.COUNTRY,
    u.COUNTY,
    u.DISPPRODUCTNAME,
    u.DISTRICT,
    u.DIVISION,
    u.DIVISIONCODE,
    u.DTTMEND,
    u.DTTMSTART,
    u.DTTMHIDE,
    u.ELEVATION,
    u.IDRECFACILITYCALC as FACILITYID,
    u.FIELD,
    u.FIELDCODE,
    u.FIELDOFFICE,
    u.FIELDOFFICECODE,
    u.IDFLOWNET as FLOWNETID,
    u.LATITUDE,
    u.LEASE,
    u.LEASEIDA,
    u.LEGALSURFLOC,
    u.LOCATIONTYP,
    u.LONGITUDE,
    u.LATLONGSOURCE,
    u.LATLONGDATUM,
    u.UTMGRIDZONE,
    u.UTMSOURCE,
    u.UTMX,
    u.UTMY,
    u.NAME,
    u.NAMESHORT,
    u.OPERATED,
    u.OPERATORIDA,
    u.OPERATOR,
    u.PADCODE,
    u.PADNAME,
    u.PLATFORM,
    u.SLOT,
    u.GOVAUTHORITY,
    u.IDRECRESP1 as PRIMARYRESPTEAMID,
    u.IDRECRESP2 as SECONDARYRESPTEAMID,
    u.PURCHASER,
    u.PRIORITY,
    u.STATEPROV,
    u.SYSCREATEDATE,
    u.SYSCREATEUSER,
    u.SYSMODDATE,
    u.SYSMODUSER,
    u.TYP1,
    u.TYP2,
    u.TYPDISPHCLIQ,
    u.TYPPA,
    u.TYPREGULATORY,
    u.IDREC as UNITID,
    u.UNITIDA,
    u.UNITIDB,
    u.UNITIDC,
    u.UNITIDPA,
    u.UNITIDREGULATORY,
    u.KEYMIGRATIONSOURCE,
    u.TYPMIGRATIONSOURCE,
    u.USERNUM1,
    u.USERNUM2,
    u.USERNUM3,
    u.USERNUM4,
    u.USERNUM5,
    u.USERTXT1,
    u.USERTXT2,
    u.USERTXT3,
    u.USERTXT4,
    u.USERTXT5,
    u.USERDTTM1,
    u.USERDTTM2,
    u.USERDTTM3,
    u.USERDTTM4,
    u.USERDTTM5,

    -- Completion information
    c.DTTMSTARTALLOC as ALLOCSTARTDATE,
    c.DTTMEND as COMPDTTMEND,
    c.DTTMLASTPRODUCEDCALC as DTTMLASTPRODUCEDCALC,
    c.DTTMLASTPRODUCEDHCLIQCALC as DTTMLASTPRODUCEDHCLIQCALC,
    c.DTTMLASTPRODUCEDGASCALC as DTTMLASTPRODUCEDGASCALC,
    c.HELDBYPRODUCTIONTHRESHOLD as HELDBYPRODUCTIONTHRESHOLD,
    c.COMPIDA,
    c.COMPIDB,
    c.COMPIDC,
    c.COMPIDD,
    c.COMPLETIONIDE as COMPIDE,
    c.COMPIDPA,
    c.COMPLETIONCODE,
    c.COMPIDREGULATORY,
    c.COMPLETIONNAME,
    c.COMPLETIONLICENSEE,
    c.COMPLETIONLICENSENO,
    c.SYSCREATEDATE as COMPSYSCREATEDATE,
    c.SYSCREATEUSER as COMPSYSCREATEUSER,
    c.SYSMODDATE as COMPSYSMODDATE,
    c.SYSMODUSER as COMPSYSMODUSER,
    c.USERDTTM1 as COMPUSERDTTM1,
    c.USERDTTM2 as COMPUSERDTTM2,
    c.USERDTTM3 as COMPUSERDTTM3,
    c.USERDTTM4 as COMPUSERDTTM4,
    c.USERDTTM5 as COMPUSERDTTM5,
    c.USERNUM1 as COMPUSERNUM1,
    c.USERNUM2 as COMPUSERNUM2,
    c.USERNUM3 as COMPUSERNUM3,
    c.USERNUM4 as COMPUSERNUM4,
    c.USERNUM5 as COMPUSERNUM5,
    c.USERTXT1 as COMPUSERTXT1,
    c.USERTXT2 as COMPUSERTXT2,
    c.USERTXT3 as COMPUSERTXT3,
    c.USERTXT4 as COMPUSERTXT4,
    c.USERTXT5 as COMPUSERTXT5,
    c.LATITUDE as COMPLATITUDE,
    c.LONGITUDE as COMPLONGITUDE,
    c.LATLONGSOURCE as COMPLATLONGSOURCE,
    c.LATLONGDATUM as COMPLATLONGDATUM,
    c.ENTRYREQPERIODFLUIDLEVEL,
    c.ENTRYREQPERIODPARAM,
    c.DTTMLICENSE,
    c.EXPORTID1,
    c.EXPORTID2,
    c.EXPORTTYP1,
    c.EXPORTTYP2,
    c.DTTMFIRSTSALE as FIRSTSALEDATE,
    c.DTTMFLOWBACKEND as FLOWBACKENDDATE,
    c.DTTMFLOWBACKSTART as FLOWBACKSTARTDATE,
    c.DTTMABANDON as ABANDONDATE,
    c.KEYMIGRATIONSOURCE as COMPKEYMIGRATIONSOURCE,
    c.TYPMIGRATIONSOURCE as COMPTYPMIGRATIONSOURCE,
    c.IMPORTID1,
    c.IMPORTID2,
    c.IMPORTTYP1,
    c.IMPORTTYP2,
    c.DTTMONPROD as PRODUCTIONDATE,
    c.WELLIDA,
    c.WELLIDB,
    c.WELLIDC,
    c.WELLIDD,
    c.WELLIDE,
    c.WELLIDPA,
    c.WELLIDREGULATORY,
    c.WELLLICENSENO,
    c.WELLNAME,

    -- Integration IDs
    si.AFIDREC as SVSITEID,
    wci.AFIDREC as WVCOMPLETIONID,
    wi.AFIDREC as WVWELLID,

    -- Update tracking
    greatest(
        coalesce(r.UPDATE_DATE, '0000-01-01T00:00:00.000Z'),
        coalesce(u.UPDATE_DATE, '0000-01-01T00:00:00.000Z'),
        coalesce(c.UPDATE_DATE, '0000-01-01T00:00:00.000Z'),
        coalesce(si.UPDATE_DATE, '0000-01-01T00:00:00.000Z'),
        coalesce(wci.UPDATE_DATE, '0000-01-01T00:00:00.000Z'),
        coalesce(wi.UPDATE_DATE, '0000-01-01T00:00:00.000Z')
    ) as UPDATE_DATE

from pvunit u
left join pvroutesetroute r 
    on u.IDRECROUTESETROUTECALC = r.IDREC
left join pvunitcomp c 
    on u.IDREC = c.IDRECPARENT
left join svintegration si 
    on u.IDREC = si.IDRECPARENT 
    and si.IDFLOWNET = u.IDFLOWNET
left join wvcompintegration wci 
    on u.IDREC = wci.IDRECPARENT 
    and wci.IDFLOWNET = u.IDFLOWNET
left join wvintegration wi 
    on u.IDREC = wi.IDRECPARENT 
    and wi.IDFLOWNET = u.IDFLOWNET