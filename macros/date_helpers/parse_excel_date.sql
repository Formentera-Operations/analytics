{% macro parse_excel_date(column_name) %}
{#
    Converts Excel serial date numbers to proper DATE values.
    Excel dates are stored as days since 1899-12-30.
    
    Note: Excel has a bug where 1900 is treated as a leap year.
    For dates >= 60 (Mar 1, 1900), we subtract 1 day to correct.
    
    Args:
        column_name: The column containing Excel serial dates
        
    Returns:
        Proper DATE value or NULL
        
    Example:
        {{ parse_excel_date('startdate') }}
#}
    CASE 
        WHEN {{ column_name }} IS NULL THEN NULL
        WHEN {{ column_name }} = 0 THEN NULL
        WHEN {{ column_name }} < 60 THEN 
            DATEADD(DAY, {{ column_name }}, '1899-12-31'::DATE)
        ELSE 
            DATEADD(DAY, {{ column_name }} - 1, '1899-12-31'::DATE)
    END
{% endmacro %}