{#
    Peloton WellView Unit Conversion Macros
    ========================================
    Source: Peloton's official Snowflake conversion script
            (snowflake_WellView_12.1.20241202_20250304073311-US Units.txt)

    WellView stores all measurements in metric/SI units internally.
    These macros apply Peloton's exact divisor factors to convert
    to imperial/oilfield units for Formentera's analytics models.

    Usage:
        {{ wv_meters_to_feet('depth') }} as depth_ft
        {{ wv_kpa_to_psi('pressure') }} as pressure_psi
        {{ wv_cbm_to_bbl('volume') }} as volume_bbl
#}


{# ==================== LENGTH ==================== #}

{% macro wv_meters_to_feet(column_name) %}
{# Peloton factor: / 0.3048 | meters -> feet (FT, FTKB) #}
    {{ column_name }} / 0.3048
{% endmacro %}

{% macro wv_meters_to_inches(column_name) %}
{# Peloton factor: / 0.0254 | meters -> inches (IN) #}
    {{ column_name }} / 0.0254
{% endmacro %}

{% macro wv_per_meter_to_per_foot(column_name) %}
{# Peloton factor: / 3.28083989501312 | per-meter -> per-foot (for rates: $/m, hrs/m) #}
    {{ column_name }} / 3.28083989501312
{% endmacro %}

{% macro wv_meters_to_miles(column_name) %}
{# Peloton factor: / 1609.344 | meters -> miles (MILES) #}
    {{ column_name }} / 1609.344
{% endmacro %}


{# ==================== VOLUME ==================== #}

{% macro wv_cbm_to_bbl(column_name) %}
{# Peloton factor: / 0.158987294928 | cubic meters -> barrels (BBL) #}
    {{ column_name }} / 0.158987294928
{% endmacro %}

{% macro wv_cbm_to_mcf(column_name) %}
{# Peloton factor: / 28.316846592 | cubic meters -> thousand cubic feet (MCF) #}
    {{ column_name }} / 28.316846592
{% endmacro %}


{# ==================== RATES ==================== #}

{% macro wv_cbm_per_day_to_bbl_per_day(column_name) %}
{# Peloton factor: / 0.1589873 | cubic meters/day -> barrels/day (BBL/DAY) #}
    {{ column_name }} / 0.1589873
{% endmacro %}

{% macro wv_cbm_per_day_to_mcf_per_day(column_name) %}
{# Peloton factor: / 28.316846592 | cubic meters/day -> MCF/day (MCF/DAY) #}
    {{ column_name }} / 28.316846592
{% endmacro %}

{% macro wv_cbm_per_sec_to_bbl_per_min(column_name) %}
{# Peloton factor: / 228.941712 | cubic meters/sec -> barrels/min (BBL/MIN) #}
    {{ column_name }} / 228.941712
{% endmacro %}

{% macro wv_cbm_per_sec_to_gpm(column_name) %}
{# Peloton factor: / 5.45099328 | cubic meters/sec -> gallons/min (GPM) #}
    {{ column_name }} / 5.45099328
{% endmacro %}

{% macro wv_cbm_per_sec_to_ft3_per_hr(column_name) %}
{# Peloton factor: / 0.679604318208 | cubic meters/sec -> cubic feet/hr (FT3/HR) #}
    {{ column_name }} / 0.679604318208
{% endmacro %}


{# ==================== SPEED ==================== #}

{% macro wv_mps_to_ft_per_hr(column_name) %}
{# Peloton factor: / 7.3152 | meters/sec -> feet/hr (FT/HR) — rate of penetration #}
    {{ column_name }} / 7.3152
{% endmacro %}

{% macro wv_mps_to_ft_per_min(column_name) %}
{# Peloton factor: / 438.912 | meters/sec -> feet/min (FT/MIN) — tripping speed #}
    {{ column_name }} / 438.912
{% endmacro %}


{# ==================== PRESSURE ==================== #}

{% macro wv_kpa_to_psi(column_name) %}
{# Peloton factor: / 6.894757 | kilopascals -> PSI #}
    {{ column_name }} / 6.894757
{% endmacro %}


{# ==================== FORCE ==================== #}

{% macro wv_newtons_to_lbf(column_name) %}
{# Peloton factor: / 4.4482216152605 | newtons -> pound-force (LBF) — hookload, WOB #}
    {{ column_name }} / 4.4482216152605
{% endmacro %}


{# ==================== MASS / WEIGHT ==================== #}

{% macro wv_kg_to_lb(column_name) %}
{# Peloton factor: / 0.45359237 | kilograms -> pounds (LB) #}
    {{ column_name }} / 0.45359237
{% endmacro %}


{# ==================== DENSITY ==================== #}

{% macro wv_kgm3_to_lb_per_gal(column_name) %}
{# Peloton factor: / 119.826428404623 | kg/m3 -> pounds/gallon (LB/GAL) — mud weight #}
    {{ column_name }} / 119.826428404623
{% endmacro %}


{# ==================== POWER ==================== #}

{% macro wv_watts_to_hp(column_name) %}
{# Peloton factor: / 745.6999 | watts -> horsepower (HP) #}
    {{ column_name }} / 745.6999
{% endmacro %}


{# ==================== ANGULAR RATE ==================== #}

{% macro wv_per_m_to_per_100ft(column_name) %}
{# Peloton factor: / 0.0328083989501312 | deg/m -> deg/100ft (dogleg severity) #}
    {{ column_name }} / 0.0328083989501312
{% endmacro %}


{# ==================== TIME / DURATION ==================== #}

{% macro wv_days_to_hours(column_name) %}
{# Peloton factor: / 0.0416666666666667 | days -> hours (HR) #}
    {{ column_name }} / 0.0416666666666667
{% endmacro %}

{% macro wv_days_to_minutes(column_name) %}
{# Peloton factor: / 0.000694444444444444 | days -> minutes (MIN) #}
    {{ column_name }} / 0.000694444444444444
{% endmacro %}
