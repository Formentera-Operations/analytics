{% macro clean_null_int(column_name) %}
{#
    Converts 0 values to NULL for integer foreign keys.
    IFS Procount uses 0 to indicate "no relationship".
    
    Args:
        column_name: Integer column to clean
        
    Returns:
        Integer value or NULL
        
    Example:
        {{ clean_null_int('gatheringsystemid') }}
#}
    NULLIF({{ column_name }}, 0)
{% endmacro %}