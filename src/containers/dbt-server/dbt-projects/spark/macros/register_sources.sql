{% macro register_tpcdi_sources() %}
  {#
    Register raw Delta Lake tables as external tables in Spark's catalog.
    Called via on-run-start in dbt_project.yml.
    Reference tables: /data/raw/delta/batch1/<table>
    Staging tables: /data/raw/delta/staging/<table>
  #}

  {# Ensure database exists #}
  {% set create_db %}
    CREATE DATABASE IF NOT EXISTS tpcdi
  {% endset %}
  {% do run_query(create_db) %}

  {# ── Batch1-only reference tables ── #}
  {% set batch1_tables = [
    'customer_mgmt', 'date', 'finwire', 'hr', 'industry',
    'status_type', 'tax_rate', 'trade_history', 'trade_type'
  ] %}

  {% for tbl in batch1_tables %}
    {% set create_tbl %}
      CREATE TABLE IF NOT EXISTS tpcdi.batch1_{{ tbl }}
      USING DELTA
      LOCATION '/data/raw/delta/batch1/{{ tbl }}'
    {% endset %}
    {% do run_query(create_tbl) %}
  {% endfor %}

  {# ── Staging tables (grow with batch appends) ── #}
  {% set staging_tables = [
    'cash_transaction', 'daily_market', 'holding_history', 'prospect',
    'trade', 'watch_history', 'account', 'customer', 'batch_date'
  ] %}

  {% for tbl in staging_tables %}
    {% set create_tbl %}
      CREATE TABLE IF NOT EXISTS tpcdi.staging_{{ tbl }}
      USING DELTA
      LOCATION '/data/raw/delta/staging/{{ tbl }}'
    {% endset %}
    {% do run_query(create_tbl) %}
  {% endfor %}

  {# ── Audit table ── #}
  {% set create_audit %}
    CREATE TABLE IF NOT EXISTS tpcdi.audit
    USING DELTA
    LOCATION '/data/raw/delta/audit'
  {% endset %}
  {% do run_query(create_audit) %}

{% endmacro %}
