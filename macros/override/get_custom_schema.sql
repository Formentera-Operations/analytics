{% macro generate_schema_name(custom_schema_name, node) -%}

    {%- set default_schema = target.schema -%}
    {%- set custom_database = none -%}

    {# Set custom database based on node configuration #}
    {%- if node.config.get('database', none) -%}
        {%- set custom_database = node.config.get('database') -%}
    {%- endif -%}

    {# Environment prefix for non-prod environments #}
    {%- set env_prefix = target.name + '_' if target.name not in ['prod', 'ci'] else '' -%}

    {# If custom schema name is provided, use it with environment prefix #}
    {%- if custom_schema_name -%}
        {{ env_prefix }}{{ custom_schema_name | trim }}
    {%- else -%}
        {{ env_prefix }}{{ default_schema | trim }}
    {%- endif -%}

{%- endmacro -%}

{% macro generate_database_name(custom_database_name, node) -%}

    {%- set default_database = target.database -%}

    {%- if custom_database_name -%}
        {{ custom_database_name | trim }}
    {%- else -%}
        {{ default_database | trim }}
    {%- endif -%}

{%- endmacro -%}