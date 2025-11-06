{% macro is_date_effective(start_date, end_date, as_of_date=none) %}
{#
    Checks if a record is effective as of a given date.
    
    Args:
        start_date: Effective start date column
        end_date: Effective end date column
        as_of_date: Date to check (defaults to CURRENT_DATE)
        
    Returns:
        BOOLEAN - TRUE if effective
        
    Example:
        {{ is_date_effective('startdate', 'enddate') }}
        {{ is_date_effective('startdate', 'enddate', "'2024-01-01'") }}
#}
    {% if as_of_date is none %}
        {% set check_date = 'CURRENT_DATE()' %}
    {% else %}
        {% set check_date = as_of_date %}
    {% endif %}
    
    (
        ({{ start_date }} IS NULL OR {{ start_date }} <= {{ check_date }})
        AND 
        ({{ end_date }} IS NULL OR {{ end_date }} >= {{ check_date }})
    )
{% endmacro %}