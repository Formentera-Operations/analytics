{% macro standardize_timestamp(date_column, time_column) %}
{#
    Combines separate date and time columns into a single TIMESTAMP.
    
    Args:
        date_column: Column containing date (DATE or string)
        time_column: Column containing time as string (HH:MM:SS)
        
    Returns:
        TIMESTAMP value or NULL
        
    Example:
        {{ standardize_timestamp('userdatestamp', 'usertimestamp') }}
#}
    CASE
        WHEN {{ date_column }} IS NULL THEN NULL
        WHEN {{ time_column }} IS NULL OR {{ time_column }} = '00:00:00' 
            THEN TRY_CAST({{ date_column }} AS TIMESTAMP)
        ELSE TRY_CAST(
            CONCAT(
                TRY_CAST({{ date_column }} AS DATE)::VARCHAR, 
                ' ', 
                {{ time_column }}
            ) AS TIMESTAMP
        )
    END
{% endmacro %}