{% macro generate_schema_name(custom_schema_name, node) -%}
    {#-
        databricks-enzyme runs in per-experiment isolation: every
        DATABRICKS_EXPERIMENT_ID minted by the benchmark-server
        orchestrator gets its own set of schemas all prefixed
        ``exp_<microsec_ts>_``. This macro encodes that prefix into
        every model's target schema.

        Layout (5 schemas per experiment):
          exp_<ts>_data    — source-tables (set via profiles.yml schema)
          exp_<ts>_bronze  — dbt bronze layer (custom_schema_name="bronze")
          exp_<ts>_silver  — dbt silver layer
          exp_<ts>_gold    — dbt gold layer
          exp_<ts>_work    — dbt work layer (ephemeral)

        End-of-experiment teardown drops the 5 schemas CASCADE so MV
        REFRESH bills stop. The stale-schema sweeper drops anything
        older than 1 day to recover from crashed experiments.
    -#}
    {%- set exp_id = env_var('DATABRICKS_EXPERIMENT_ID', 'unset') | trim -%}
    {%- if exp_id == '' or exp_id == 'unset' -%}
        {{ exceptions.raise_compiler_error(
            "DATABRICKS_EXPERIMENT_ID env var is required for the "
            "databricks-enzyme dbt project — the orchestrator should set "
            "it before invoking dbt build."
        ) }}
    {%- endif -%}
    {%- if custom_schema_name is none -%}
        {{ target.schema }}
    {%- else -%}
        exp_{{ exp_id }}_{{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}
