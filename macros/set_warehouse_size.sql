{% macro set_warehouse_size(size) %}
    {#
        All warehouses are created at:

        https://github.com/Formentera-Operations/snowflake-infrastructure/blob/f10d9a849c3bfcb7df91acb3af10fd3407bb2703/snowflake/warehouse_services.tf
    #}

    {% if not var("enable_dynamic_warehouse") %}
        {{ return(target.warehouse) }}
    {% elif not execute %}
        {# No need to switch warehouses during compilation #}
        {{ return(target.warehouse) }}
    {% endif %}

    {% if var("available_warehouse_sizes", None) == None %}
        {{ exceptions.raise_compiler_error("Please set the `available_warehouse_sizes` variable in the dbt_project.yml.") }}
    {% endif %}

    {% if size not in var("available_warehouse_sizes") %}
        {{ exceptions.raise_compiler_error("Warehouse size not one of " ~ var("available_warehouse_sizes")) }}
    {% endif %}

    {% if target.name == "prod" %}
        {{ return("DBT_PROD_WH_" ~ size) }}
    {% elif target.name == "dev" %}
        {{ return("DBT_DEV_WH_" ~ size) }}
    {% elif target.name == "ci" %}
        {{ return("DBT_CI_WH_" ~ size) }}
    {% else %}
        {% do log("Unknown target '" ~ target.name ~ "' in macro 'set_warehouse_size'. Running with default warehouse '" ~ target.warehouse ~ "'", True) %}
        {{ return(target.warehouse) }}
    {% endif %}
{% endmacro %}
