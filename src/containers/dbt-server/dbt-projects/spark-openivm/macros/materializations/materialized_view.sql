{#
  Custom materialization for spark-openivm.

  Routes the model through the openivm-spark DDL extension:

  - full_refresh (batch 1):
      DROP MATERIALIZED VIEW IF EXISTS <target> CASCADE;
      CREATE MATERIALIZED VIEW <target> AS (<model_sql>);

  - incremental (batch 2/3):
      REFRESH MATERIALIZED VIEW <target>;

  We rely on `flags.FULL_REFRESH` (set by the benchmark-server via the
  `--full-refresh` dbt CLI flag) as the sole branch condition, because
  the fabricspark adapter cannot introspect MV metadata through its
  generic relation cache.
#}

{% materialization materialized_view, adapter='fabricspark' %}

  {%- set target_relation = this.incorporate(type='view') -%}

  {{ run_hooks(pre_hooks, inside_transaction=False) }}

  {% if flags.FULL_REFRESH %}
    {# Drop first — openivm-spark's grammar does not support CREATE OR
       REPLACE MATERIALIZED VIEW. Then create from the compiled model SQL. #}
    {% call statement('drop') %}
      DROP MATERIALIZED VIEW IF EXISTS {{ target_relation }}
    {% endcall %}

    {% call statement('main') %}
      CREATE MATERIALIZED VIEW {{ target_relation }} AS (
        {{ sql }}
      )
    {% endcall %}

  {% else %}
    {# Incremental refresh — openivm-spark's REFRESH path picks up the
       staging deltas that IvmDmlInterceptorRule has logged since the
       last refresh and applies the RefreshType-specific rewrite. #}
    {% call statement('main') %}
      REFRESH MATERIALIZED VIEW {{ target_relation }}
    {% endcall %}

  {% endif %}

  {% do persist_docs(target_relation, model) %}
  {{ run_hooks(post_hooks, inside_transaction=False) }}

  {{ return({'relations': [target_relation]}) }}

{% endmaterialization %}
