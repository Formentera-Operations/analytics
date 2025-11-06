{% macro generate_surrogate_key(field_list) %}
{#
    Generates a deterministic surrogate key from multiple fields.
    Uses MD5 hash of concatenated field values.
    
    Args:
        field_list: List of column names to include in hash
        
    Returns:
        VARCHAR(32) MD5 hash
        
    Example:
        {{ generate_surrogate_key(['merrickid', 'merricktype']) }}
#}
    MD5(
        CONCAT(
            {% for field in field_list %}
                COALESCE(CAST({{ field }} AS VARCHAR), '')
                {% if not loop.last %} || '|' || {% endif %}
            {% endfor %}
        )
    )
{% endmacro %}