
{% macro decode_object_type(type_column) %}
{#
    Decodes numeric object type codes to human-readable names.
    
    Based on IFS Procount object type discriminator pattern:
    - Type 1 = Completion
    - Type 2 = Meter
    - Type 3 = Tank
    - Type 4 = Equipment
    - Type 5 = Gathering System
    - Type 6 = Route
    
    Args:
        type_column: Column containing object type code
        
    Returns:
        VARCHAR object type name
        
    Example:
        {{ decode_object_type('objectmerricktype') }}
#}
    CASE {{ type_column }}
        WHEN 1 THEN 'COMPLETION'
        WHEN 2 THEN 'METER'
        WHEN 3 THEN 'TANK'
        WHEN 4 THEN 'EQUIPMENT'
        WHEN 5 THEN 'GATHERING_SYSTEM'
        WHEN 6 THEN 'ROUTE'
        ELSE 'UNKNOWN_' || CAST({{ type_column }} AS VARCHAR)
    END
{% endmacro %}