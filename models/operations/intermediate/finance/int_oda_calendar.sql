{{ config(
    enabled=true,
    materialized='view'
) }}


SELECT
    DATE_KEY AS "DateKey", 
    DATE AS "Date",
    DAY AS "Day",
    DAY_SUFFIX AS "DaySuffix",
    DAY_OF_YEAR AS "DayOfYear",
    WEEKDAY AS "Weekday",
    WEEKDAY_NAME AS "WeekDayName",
    WEEKDAY_NAME_SHORT AS "WeekDayName_Short",
    WEEKDAY_NAME_FIRST_LETTER AS "WeekDayName_FirstLetter",
    IS_WEEKEND AS "IsWeekend",
    WEEK AS "Week",
    WEEK_NAME AS "WeekName",
    WEEK_OF_MONTH AS "WeekOfMonth",
    WEEK_OF_YEAR AS "WeekOfYear",
    FIRST_DATE_OF_WEEK AS "FirstDateofWeek",
    LAST_DATE_OF_WEEK AS "LastDateofWeek",
    DOW_IN_MONTH AS "DOWInMonth",
    MONTH AS "Month",
    MONTH_NAME AS "MonthName",
    MONTH_NAME_SHORT AS "MonthName_Short",
    MONTH_NAME_FIRST_LETTER AS "MonthName_FirstLetter",
    MM_YYYY AS "MMYYYY",
    MONTH_YEAR AS "MonthYear",
    MONTH_YEAR_NAME AS "MonthYearName",
    FIRST_DATE_OF_MONTH AS "FirstDateofMonth",
    LAST_DATE_OF_MONTH AS "LastDateofMonth",
    QUARTER AS "Quarter",
    QUARTER_NAME AS "QuarterName",
    FIRST_DATE_OF_QUARTER AS "FirstDateofQuater",
    LAST_DATE_OF_QUARTER AS "LastDateofQuater",
    YEAR AS "Year",
    FIRST_DATE_OF_YEAR AS "FirstDateofYear",
    LAST_DATE_OF_YEAR AS "LastDateofYear",
FROM {{ ref('stg_oda__calendar') }}
--WHERE DATE >= '2020-01-01'