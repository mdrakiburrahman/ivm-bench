{#
  Custom materialization for spark-openivm.

  Routes the model through the openivm-spark DDL extension:

  - full_refresh (batch 1):
      DROP MATERIALIZED VIEW IF EXISTS <target>; (unless first build in fresh lakehouse)
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
    {# CREATE OR REPLACE is unsupported. Only the first invocation against a
       freshly provisioned lakehouse skips DROP; retries retain cleanup. #}
    {% if env_var('FABRIC_OPENIVM_FRESH_BUILD', '0') != '1' %}
    {% call statement('drop') %}
      DROP MATERIALIZED VIEW IF EXISTS {{ target_relation }}
    {% endcall %}
    {% endif %}

    {% call statement('main') %}
      CREATE MATERIALIZED VIEW {{ target_relation }} AS (
        {{ sql }}
      )
    {% endcall %}

  {% else %}
    {# Incremental refresh — under spark.openivm.changeFeed.mode=cdf the
       REFRESH path reads Delta Change Data Feed from each source table and
       applies the RefreshType-specific rewrite. Sources must have
       delta.enableChangeDataFeed=true (set in spark_openivm_sources). #}
    {% call statement('main') %}
      REFRESH MATERIALIZED VIEW {{ target_relation }}
    {% endcall %}

  {% endif %}

  {% do persist_docs(target_relation, model) %}
  {{ run_hooks(post_hooks, inside_transaction=False) }}

  {{ return({'relations': [target_relation]}) }}

{% endmaterialization %}
