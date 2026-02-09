{#
    Peloton ProdView Unit Conversion Macros
    ========================================
    Source: Peloton's official Snowflake conversion script
            (snowflake_ProdView_5.0.20251217_20260209081737-Formentera.txt)

    ProdView stores all measurements in metric/SI units internally.
    These macros apply Peloton's exact divisor factors to convert
    to imperial/oilfield units for Formentera's internal models.

    WiseRock models consume raw metric values and do NOT use these macros.

    Usage:
        {{ pv_meters_to_inches('strokelength') }} as stroke_length_in
        {{ pv_decimal_to_pct('usernum1') }} as run_time_pct
        {{ pv_cbm_to_bbl_per_day('volperdaycalc') }} as vol_per_day_calc_bbl
#}


{# ==================== LENGTH ==================== #}

{% macro pv_meters_to_inches(column_name) %}
{# Peloton factor: / 0.0254 | meters → inches (IN) #}
    {{ column_name }} / 0.0254
{% endmacro %}

{% macro pv_meters_to_feet(column_name) %}
{# Peloton factor: / 0.3048 | meters → feet (FT) #}
    {{ column_name }} / 0.3048
{% endmacro %}

{% macro pv_meters_to_64ths_inch(column_name) %}
{# Peloton factor: / 0.000396875 | meters → 1/64 inch (1/64") #}
    {{ column_name }} / 0.000396875
{% endmacro %}


{# ==================== VOLUME ==================== #}

{% macro pv_cbm_to_bbl(column_name) %}
{# Peloton factor: / 0.158987294928 | cubic meters → barrels (BBL) #}
    {{ column_name }} / 0.158987294928
{% endmacro %}

{% macro pv_cbm_to_mcf(column_name) %}
{# Peloton factor: / 28.316846592 | cubic meters → thousand cubic feet (MCF) #}
    {{ column_name }} / 28.316846592
{% endmacro %}

{% macro pv_joules_to_mmbtu(column_name) %}
{# Peloton factor: / 1055055852.62 | joules → million BTU (MMBTU) #}
    {{ column_name }} / 1055055852.62
{% endmacro %}


{# ==================== RATES ==================== #}

{% macro pv_cbm_to_bbl_per_day(column_name) %}
{# Peloton factor: / 0.1589873 | cubic meters/day → barrels/day (BBL/DAY) #}
    {{ column_name }} / 0.1589873
{% endmacro %}

{% macro pv_cbm_ratio_to_mcf_per_bbl(column_name) %}
{# Peloton factor: / 178.107606679035 | m³/m³ → MCF/BBL (gas-oil ratio) #}
    {{ column_name }} / 178.107606679035
{% endmacro %}

{% macro pv_cbm_per_m_to_bbl_per_inch(column_name) %}
{# Peloton factor: / 6.25934251968504 | m³/m → BBL/IN #}
    {{ column_name }} / 6.25934251968504
{% endmacro %}

{% macro pv_cbm_ratio_to_bbl_per_mcf(column_name) %}
{# Peloton factor: / 0.00561458333333333 | m³/m³ → BBL/MCF #}
    {{ column_name }} / 0.00561458333333333
{% endmacro %}


{# ==================== PRESSURE ==================== #}

{% macro pv_kpa_to_psi(column_name) %}
{# Peloton factor: / 6.894757 | kilopascals → PSI #}
    {{ column_name }} / 6.894757
{% endmacro %}


{# ==================== TIME / DURATION ==================== #}

{% macro pv_days_to_hours(column_name) %}
{# Peloton factor: / 0.0416666666666667 | days → hours (HR) #}
    {{ column_name }} / 0.0416666666666667
{% endmacro %}

{% macro pv_seconds_to_minutes(column_name) %}
{# Peloton factor: / 0.000694444444444444 | seconds → minutes (MIN) #}
    {{ column_name }} / 0.000694444444444444
{% endmacro %}


{# ==================== PERCENTAGE ==================== #}

{% macro pv_decimal_to_pct(column_name) %}
{# Peloton factor: / 0.01 | decimal (0-1) → percent (0-100) (%) #}
    {{ column_name }} / 0.01
{% endmacro %}


{# ==================== MASS / WEIGHT ==================== #}

{% macro pv_kg_to_lb(column_name) %}
{# Peloton factor: / 0.45359237 | kilograms → pounds (LB) #}
    {{ column_name }} / 0.45359237
{% endmacro %}


{# ==================== DENSITY ==================== #}

{% macro pv_kgm3_to_lb_per_gal(column_name) %}
{# Peloton factor: / 119.826428404623 | kg/m³ → pounds/gallon (LB/GAL) #}
    {{ column_name }} / 119.826428404623
{% endmacro %}

{% macro pv_kgm3_to_lb_per_1000ft3(column_name) %}
{# Peloton factor: / 0.01601846250554 | kg/m³ → LB/1000FT³ #}
    {{ column_name }} / 0.01601846250554
{% endmacro %}

{% macro pv_kgm3_to_sg(column_name) %}
{# Peloton factor: / 1000 | kg/m³ → specific gravity water (SG) #}
    {{ column_name }} / 1000
{% endmacro %}


{# ==================== VISCOSITY ==================== #}

{% macro pv_pas_to_cp(column_name) %}
{# Peloton factor: / 0.001 | pascal-seconds → centipoise (CP) #}
    {{ column_name }} / 0.001
{% endmacro %}

{% macro pv_m2s_to_in2s(column_name) %}
{# Peloton factor: / 55.741824 | m²/s → IN²/S #}
    {{ column_name }} / 55.741824
{% endmacro %}


{# ==================== POWER / ENERGY ==================== #}

{% macro pv_watts_to_hp(column_name) %}
{# Peloton factor: / 745.6999 | watts → horsepower (HP) #}
    {{ column_name }} / 745.6999
{% endmacro %}

{% macro pv_jm3_to_btu_per_ft3(column_name) %}
{# Peloton factor: / 37258.9458078313 | J/m³ → BTU/FT³ #}
    {{ column_name }} / 37258.9458078313
{% endmacro %}


{# ==================== TORQUE ==================== #}

{% macro pv_nm_to_1000in_lb(column_name) %}
{# Peloton factor: / 112.984829027617 | newton-meters → 1000 IN•LB #}
    {{ column_name }} / 112.984829027617
{% endmacro %}