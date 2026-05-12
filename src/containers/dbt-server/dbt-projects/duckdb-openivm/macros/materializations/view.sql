{#
  Custom view materialization for DuckDB-OpenIVM.

  dbt's default view materialization uses a CREATE __dbt_tmp -> DROP
  original -> RENAME __dbt_tmp dance. That fails in the OpenIVM adapter
  because each SQL statement runs in its own CLI subprocess with its own
  connection — by the time RENAME executes in a fresh subprocess, the
  original view may still be visible to the catalog because the DROP's
  effects haven't fully propagated, and the RENAME aborts with
  "another entry with this name already exists".

  Use CREATE OR REPLACE VIEW so the operation is atomic within a single
  CLI subprocess and there is no temp-name juggling for the catalog.

  Run 25703070711 (SF=1000 batch 2) failed all 5 gold.analytics views
  with the rename error after they were switched from materialized_view
  to view — this macro is what makes that switch actually work.
#}

{% materialization view, adapter='openivm' %}

  {%- set target_relation = this.incorporate(type='view') -%}

  {{ run_hooks(pre_hooks, inside_transaction=False) }}

  {% call statement('main') %}
    CREATE OR REPLACE VIEW {{ target_relation }} AS (
      {{ sql }}
    )
  {% endcall %}

  {% do persist_docs(target_relation, model) %}
  {{ run_hooks(post_hooks, inside_transaction=False) }}

  {{ return({'relations': [target_relation]}) }}

{% endmaterialization %}
