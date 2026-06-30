{#
  Databricks Materialized View materialization for the databricks-enzyme
  engine.

  Routes every model through Databricks SQL's MATERIALIZED VIEW DDL:

  - full_refresh (batch 1):
      DROP MATERIALIZED VIEW IF EXISTS <target>;
      CREATE MATERIALIZED VIEW <target>
      [SCHEDULE <schedule>]
      [REFRESH POLICY <policy>]
      AS <model_sql>;

  - incremental (batch 2/3):
      REFRESH MATERIALIZED VIEW <target>;

  The branch condition is `flags.FULL_REFRESH` (set by dbt-server via the
  `--full-refresh` dbt CLI flag), mirroring the spark-openivm and
  duckdb-openivm pattern.

  Config knobs (read from `dbt_project.yml` or per-model `{{ config(...) }}`):

    +refresh_policy   default 'INCREMENTAL STRICT'
                      'AUTO' | 'INCREMENTAL' | 'INCREMENTAL STRICT' | 'FULL'
                      Emitted as `REFRESH POLICY <p>` in the CREATE statement.
                      `ALTER MATERIALIZED VIEW ... SET REFRESH POLICY` is
                      NOT valid Databricks SQL — the policy is fixed at
                      CREATE time.

    +mv_schedule      default none
                      e.g. "EVERY 1 HOUR" — when set, emitted as
                      `SCHEDULE <schedule>` in the CREATE statement.

  References:
   - https://learn.microsoft.com/azure/databricks/sql/language-manual/sql-ref-syntax-ddl-create-materialized-view-refresh-policy
   - https://learn.microsoft.com/azure/databricks/sql/language-manual/sql-ref-syntax-ddl-create-materialized-view
#}

{% materialization materialized_view, adapter='databricks' %}

  {%- set target_relation = this.incorporate(type='view') -%}
  {%- set policy = config.get('refresh_policy', 'INCREMENTAL STRICT') -%}
  {%- set schedule = config.get('mv_schedule', none) -%}

  {{ run_hooks(pre_hooks, inside_transaction=False) }}

  {% if flags.FULL_REFRESH %}
    {# Drop first — Databricks does not support CREATE OR REPLACE MATERIALIZED VIEW. #}
    {% call statement('drop') %}
      DROP MATERIALIZED VIEW IF EXISTS {{ target_relation }}
    {% endcall %}

    {# REFRESH POLICY must be set inside the CREATE statement; there is no
       ALTER form. Order per docs: name, SCHEDULE, REFRESH POLICY, AS query. #}
    {%- set create_sql -%}
      CREATE MATERIALIZED VIEW {{ target_relation }}
      {% if schedule %}SCHEDULE {{ schedule }}{% endif %}
      {% if policy %}REFRESH POLICY {{ policy }}{% endif %}
      AS
      {{ sql }}
    {%- endset -%}

    {% call statement('main') %}
      {{ create_sql }}
    {% endcall %}

  {% else %}
    {# Incremental refresh — Databricks Enzyme reads source-table change
       logs (row-tracking / change-data-feed) and applies only the delta.
       Under REFRESH POLICY INCREMENTAL STRICT this statement FAILS if
       Enzyme can't incrementalize — that is the whole point of the
       'strict' suffix and the signal we want surfaced loudly. #}
    {% call statement('main') %}
      REFRESH MATERIALIZED VIEW {{ target_relation }}
    {% endcall %}

  {% endif %}

  {% do persist_docs(target_relation, model) %}
  {{ run_hooks(post_hooks, inside_transaction=False) }}

  {{ return({'relations': [target_relation]}) }}

{% endmaterialization %}
