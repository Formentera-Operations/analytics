{% macro object_type_case(type_column) %}
{#
    Creates a CASE expression for object type.
    Useful for filtering or joining polymorphic relationships.
    
    Returns a CTE-ready expression.
    
    Example:
        {{ object_type_case('objecttype') }}
#}
    CASE {{ type_column }}
        WHEN 1 THEN 'completion'
        WHEN 2 THEN 'meter'
        WHEN 3 THEN 'tank'
        WHEN 4 THEN 'equipment'
        WHEN 5 THEN 'gathering_system'
        WHEN 6 THEN 'route'
    END
{% endmacro %}
