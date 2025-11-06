{% macro standardize_date(column_name) %}
{#
    Standardizes date strings to proper DATE values.
    Handles common NULL-equivalent dates from IFS Procount.
    
    Args:
        column_name: The column containing date strings
        
    Returns:
        Proper DATE value or NULL
        
    Example:
        {{ standardize_date('effectivedate') }}
#}
    CASE
        WHEN {{ column_name }} IS NULL THEN NULL
        WHEN {{ column_name }} = '1900-01-01' THEN NULL
        WHEN {{ column_name }} = '1899-12-31' THEN NULL
        WHEN {{ column_name }} = '2999-01-01' THEN '9999-12-31'::DATE
        WHEN TRY_CAST({{ column_name }} AS DATE) IS NOT NULL 
            THEN TRY_CAST({{ column_name }} AS DATE)
        ELSE NULL
    END
{% endmacro %}