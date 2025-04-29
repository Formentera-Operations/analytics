{% macro transform_reserve_category(column_name) %}
    case
        when {{ column_name }} in ('01PDP', '1PDP', '1PDP_COST', '1PDP_NEW', '1PDP_PLANT', '1PDP_SOLD', '1PDP_WO') 
            then '1PDP'
        when {{ column_name }} = '2WIP'
            then '2WIP'
        when {{ column_name }} = '3PDSI'
            then '3PDNP'
        when {{ column_name }} in ('3PUD', '4PUD', '4PUD_HZ', '4PUD_RC', '4PUD_VT')
            then '4PUD'
        else {{ column_name }}
    end
{% endmacro %}