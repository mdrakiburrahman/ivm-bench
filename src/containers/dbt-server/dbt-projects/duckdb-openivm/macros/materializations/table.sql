{#
  Custom table materialization for DuckDB-OpenIVM.

  The default dbt table materialization uses create-temp + drop + rename,
  which fails with the stateless CLI adapter. This override uses
  CREATE OR REPLACE TABLE directly.
#}

{% materialization table, adapter='openivm' %}

  {%- set target_relation = this.incorporate(type='table') -%}

  {{ run_hooks(pre_hooks, inside_transaction=False) }}

  {% call statement('main') %}
    CREATE OR REPLACE TABLE {{ target_relation }} AS (
      {{ sql }}
    )
  {% endcall %}

  {% do persist_docs(target_relation, model) %}
  {{ run_hooks(post_hooks, inside_transaction=False) }}

  {{ return({'relations': [target_relation]}) }}

{% endmaterialization %}
