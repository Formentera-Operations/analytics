{% macro transform_company_name(column_name) %}
    case
        when {{ column_name }} in ('Meramec')
            then 'FP Meramec'
        when {{ column_name }} in ('Saddleback', 'FP_GOLDSMITH-SBE')
            then 'FP Goldsmith (Saddleback)'
        when {{ column_name }} in ('FP_GOLDSMITH-SLANT', 'Slant')
            then 'FP Goldsmith (SLANT)'
        when {{ column_name }} = 'Green'
            then 'FP Green'
        when {{ column_name }} = 'Lake Fork'
            then 'FP Lakefork'
        when {{ column_name }} = 'Overlook'
            then 'FP Overlook'
        when {{ column_name }} = 'Rio Grande'
            then 'FP Rio Grande'
        when {{ column_name }} = 'Balboa LA'
            then 'FP Balboa LA'
        when {{ column_name }} = 'Balboa MS'
            then 'FP Balboa MS'
        when {{ column_name }} = 'Balboa ND'
            then 'FP Balboa ND'
        when {{ column_name }} = 'Divide'
            then 'FP Divide'
        when {{ column_name }} in ('Kingfisher', 'FP_KINGFISHER')
            then 'FP Kingfisher'
        when {{ column_name }} in ('Maverick', 'FP_MAVERICK')
            then 'FP Maverick'
        when {{ column_name }} = 'Wheeler'
            then 'FP Wheeler'
        when {{ column_name }} = 'Snyder'
            then 'Snyder Drillco'
        when {{ column_name }} = 'FP_PRONGHORN'
            then 'FP Pronghorn'
        when {{ column_name }} = 'FP_DRAKE'
            then 'FP Drake'
        else coalesce({{ column_name }}, get(output, 'basin')::string)
    end
{% endmacro %}