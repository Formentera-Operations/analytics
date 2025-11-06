{% macro generate_object_key(id_column, type_column) %}
{#
    Generates a universal object key combining ID and type.
    Format: {TYPE}_{ID}
    
    Args:
        id_column: Object ID column
        type_column: Object type column
        
    Returns:
        VARCHAR universal object key
        
    Example:
        {{ generate_object_key('merrickid', 'merricktype') }}
#}
    CONCAT(
        {{ decode_object_type(type_column) }},
        '_',
        CAST({{ id_column }} AS VARCHAR)
    )
{% endmacro %}