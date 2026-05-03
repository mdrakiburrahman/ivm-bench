{# Adapter SQL macros for openivm — executes via CLI binary #}

{% macro openivm__list_schemas(database) %}
  {% call statement('list_schemas', fetch_result=True, auto_begin=False) %}
    SELECT schema_name FROM information_schema.schemata
  {% endcall %}
  {{ return(load_result('list_schemas').table) }}
{% endmacro %}

{% macro openivm__check_schema_exists(information_schema, schema) %}
  {% call statement('check_schema', fetch_result=True, auto_begin=False) %}
    SELECT count(*) FROM information_schema.schemata
    WHERE schema_name = '{{ schema }}'
  {% endcall %}
  {{ return(load_result('check_schema').table) }}
{% endmacro %}

{% macro openivm__create_schema(relation) %}
  {% call statement('create_schema') %}
    CREATE SCHEMA IF NOT EXISTS {{ relation.without_identifier() }}
  {% endcall %}
{% endmacro %}

{% macro openivm__drop_schema(relation) %}
  {% call statement('drop_schema') %}
    DROP SCHEMA IF EXISTS {{ relation.without_identifier() }} CASCADE
  {% endcall %}
{% endmacro %}

{% macro openivm__drop_relation(relation) %}
  {% call statement('drop_relation') %}
    DROP {{ relation.type }} IF EXISTS {{ relation }} CASCADE
  {% endcall %}
{% endmacro %}

{% macro openivm__rename_relation(from_relation, to_relation) %}
  {% call statement('rename_relation') %}
    ALTER {{ from_relation.type }} {{ from_relation }} RENAME TO {{ to_relation.identifier }}
  {% endcall %}
{% endmacro %}

{% macro openivm__create_view_as(relation, sql) %}
  {% call statement('create_view') %}
    CREATE OR REPLACE VIEW {{ relation }} AS (
      {{ sql }}
    )
  {% endcall %}
{% endmacro %}

{% macro openivm__create_table_as(temporary, relation, sql) %}
  {% call statement('create_table') %}
    CREATE {% if temporary %}TEMPORARY {% endif %}TABLE {{ relation }} AS (
      {{ sql }}
    )
  {% endcall %}
{% endmacro %}

{% macro openivm__get_columns_in_relation(relation) %}
  {% call statement('get_columns', fetch_result=True, auto_begin=False) %}
    SELECT column_name, data_type, character_maximum_length,
           numeric_precision, numeric_scale
    FROM information_schema.columns
    WHERE table_catalog = '{{ relation.database }}'
      AND table_schema = '{{ relation.schema }}'
      AND table_name = '{{ relation.identifier }}'
    ORDER BY ordinal_position
  {% endcall %}
  {{ return(load_result('get_columns').table) }}
{% endmacro %}

{% macro openivm__list_relations_without_caching(schema_relation) %}
  {% call statement('list_relations', fetch_result=True, auto_begin=False) %}
    SELECT table_catalog as database, table_schema as schema, table_name as name,
           CASE table_type
             WHEN 'BASE TABLE' THEN 'table'
             WHEN 'VIEW' THEN 'view'
             ELSE 'table'
           END as type
    FROM information_schema.tables
    WHERE table_schema = '{{ schema_relation.schema }}'
      AND table_catalog = '{{ schema_relation.database }}'
  {% endcall %}
  {{ return(load_result('list_relations').table) }}
{% endmacro %}

{% macro openivm__current_timestamp() %}
  now()
{% endmacro %}

{% macro openivm__make_temp_relation(base_relation, suffix) %}
  {% set tmp_identifier = base_relation.identifier ~ suffix %}
  {% do return(base_relation.incorporate(
    path={"identifier": tmp_identifier, "schema": none},
    type="table"
  )) %}
{% endmacro %}

{% macro openivm__snapshot_get_time() -%}
  {{ current_timestamp() }}
{%- endmacro %}
