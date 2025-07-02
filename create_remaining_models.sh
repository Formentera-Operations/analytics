#!/bin/bash

models=(
    "perforations"
    "production_failures"
    "reference_wells"
    "rig_mud_pump_checks"
    "rig_mud_pump_operations"
    "rig_mud_pumps"
    "rigs"
    "rod_components"
    "rod_strings"
    "stimulation_fluid_additives"
    "stimulation_fluid_systems"
    "stimulation_intervals"
    "stimulation_proppant"
    "stimulations"
    "tubing_component_mandrel_inserts"
    "tubing_component_mandrels"
    "tubing_components"
    "tubing_run_tallies"
    "tubing_strings"
    "well_status_history"
    "wellbore_depths"
    "wellbore_directional_survey_data"
    "wellbore_directional_surveys"
    "wellhead_components"
    "wellheads"
    "wvsysintegration"
    "zones"
)

for model in "${models[@]}"; do
    cat > "models/operations/applications/wiserock/wiserock_app__${model}.sql" << EOF
{{ config(
    materialized='view',
    schema='wiserock_app',
    tags=['wiserock', 'wellview']
) }}

select * from {{ ref('stg_wellview__${model}') }}
EOF
done