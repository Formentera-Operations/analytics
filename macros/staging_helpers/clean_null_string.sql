{% macro clean_null_string(column_name) %}
{#
    Converts empty strings to NULL for text fields.
    
    Args:
        column_name: String column to clean
        
    Returns:
        String value or NULL
        
    Example:
        {{ clean_null_string('description') }}
#}
    NULLIF(TRIM({{ column_name }}), '')
{% endmacro %}