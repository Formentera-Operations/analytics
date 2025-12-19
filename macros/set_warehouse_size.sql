{% macro set_warehouse_size(size) %}
    {#
        All warehouses are created at:

        https://github.com/Formentera-Operations/snowflake-infrastructure/blob/f10d9a849c3bfcb7df91acb3af10fd3407bb2703/snowflake/warehouse_services.tf
    #}

    {% if not var("enable_dynamic_warehouse", false) %}
        {{ return(target.warehouse) }}
    {% elif not execute %}
        {# No need to switch warehouses during compilation #}
        {{ return(target.warehouse) }}
    {% endif %}

    {% if var("available_warehouse_sizes", None) == None %}
        {{ exceptions.raise_compiler_error("Please set the `available_warehouse_sizes` variable in the dbt_project.yml.") }}
    {% elif size not in var("available_warehouse_sizes") %}
        {{ exceptions.raise_compiler_error("Warehouse size not one of " ~ var("available_warehouse_sizes")) }}
    {% endif %}

    {% if target.name == "prod" %}
        {{ return("DBT_PROD_WH_" ~ size) }}
    {% elif target.name == "ci" %}
        {{ return("DBT_CI_WH_" ~ size) }}
    {% elif target.name == "dev" %}
        {% if target.role.startswith("FO_") %}
            {{ return("FO_DEV_WH_" ~ size) }}
        {% elif target.role.startswith("FP_") %}
            {{ return("FP_DEV_WH_" ~ size) }}
        {% else %}
            {% do log("Dev role '" ~ target.role ~ "' in macro 'set_warehouse_size' does not start with 'FO_' or 'FP_'. Running with default warehouse '" ~ target.warehouse ~ "'", True) %}
            {{ return(target.warehouse) }}
        {% endif %}
    {% else %}
        {% do log("Unknown target '" ~ target.name ~ "' in macro 'set_warehouse_size'. Running with default warehouse '" ~ target.warehouse ~ "'", True) %}
        {{ return(target.warehouse) }}
    {% endif %}
{% endmacro %}
