{% macro warm_fabric_session() %}
  {# Opens the normal dbt-owned session with the same environment/JAR config.
     run-operation does not execute the source-loading on-run-start hook. #}
  {% do run_query('SELECT 1') %}
{% endmacro %}
