{#
  Custom materialization for DuckDB-OpenIVM.

  ALL SQL goes through the OpenIVM CLI binary (adapter's execute() method).
  dbt handles DAG ordering, progress tracking, and per-model timing.

  - full_refresh (batch 1): CREATE MATERIALIZED VIEW ... AS (model_sql)
  - incremental  (batch 2/3): PRAGMA refresh_options('ducklake', schema, name)

  We use flags.FULL_REFRESH as the sole branch condition because the
  adapter cannot introspect materialized views created via CLI subprocess.
#}

{% materialization materialized_view, default %}

  {%- set target_relation = this.incorporate(type='view') -%}

  {{ run_hooks(pre_hooks, inside_transaction=False) }}

  {% if flags.FULL_REFRESH %}
    {# ── Batch 1: Create the materialized view ── #}
    {% call statement('main') %}
      CREATE MATERIALIZED VIEW {{ target_relation }} AS (
        {{ sql }}
      )
    {% endcall %}

  {% else %}
    {# ── Batch 2/3: Incremental refresh ── #}
    {% set schema_name = target_relation.schema %}
    {% set view_name = target_relation.identifier %}

    {% call statement('main') %}
      PRAGMA refresh_options('ducklake', '{{ schema_name }}', '{{ view_name }}')
    {% endcall %}

  {% endif %}

  {% do persist_docs(target_relation, model) %}
  {{ run_hooks(post_hooks, inside_transaction=False) }}

  {{ return({'relations': [target_relation]}) }}

{% endmaterialization %}
