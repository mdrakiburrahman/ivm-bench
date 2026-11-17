{#
  on-run-start hook: Configure OpenIVM settings and ensure schemas exist.
  LOAD openivm is handled by the adapter's CLI binary subprocess.
  Source table management is handled externally by the benchmark-server.
#}

{% macro openivm_setup() %}
  {# Ensure schemas exist for materialized views #}
  {% do run_query("CREATE SCHEMA IF NOT EXISTS ducklake.tpcdi") %}
  {% do run_query("CREATE SCHEMA IF NOT EXISTS ducklake.bronze") %}
  {% do run_query("CREATE SCHEMA IF NOT EXISTS ducklake.silver") %}
  {% do run_query("CREATE SCHEMA IF NOT EXISTS ducklake.gold") %}
{% endmacro %}
