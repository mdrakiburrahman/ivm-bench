{% macro register_tpcdi_sources() %}
  {#
    Register raw Delta Lake tables as views using DuckDB's delta_scan().
    Called via on-run-start in dbt_project.yml.
    Reference tables: /data/raw/delta/batch1/<table>
    Staging tables: /data/raw/delta/staging/<table>
    Views are created in the ducklake catalog.
  #}

  {# Ensure ducklake is the active database for all DDL #}
  {% set use_ducklake %}
    USE ducklake
  {% endset %}
  {% do run_query(use_ducklake) %}

  {# Ensure schemas exist #}
  {% set create_schema %}
    CREATE SCHEMA IF NOT EXISTS tpcdi
  {% endset %}
  {% do run_query(create_schema) %}

  {# ── Batch1-only reference tables ── #}
  {% set batch1_tables = [
    'customer_mgmt', 'date', 'finwire', 'hr', 'industry',
    'status_type', 'tax_rate', 'trade_history', 'trade_type'
  ] %}

  {% for tbl in batch1_tables %}
    {% set create_view %}
      CREATE OR REPLACE VIEW tpcdi.batch1_{{ tbl }} AS
      SELECT * FROM delta_scan('/data/raw/delta/batch1/{{ tbl }}')
    {% endset %}
    {% do run_query(create_view) %}
  {% endfor %}

  {# ── Staging tables (grow with batch appends) ── #}
  {% set staging_tables = [
    'cash_transaction', 'daily_market', 'holding_history', 'prospect',
    'trade', 'watch_history', 'account', 'customer', 'batch_date'
  ] %}

  {% for tbl in staging_tables %}
    {% set create_view %}
      CREATE OR REPLACE VIEW tpcdi.staging_{{ tbl }} AS
      SELECT * FROM delta_scan('/data/raw/delta/staging/{{ tbl }}')
    {% endset %}
    {% do run_query(create_view) %}
  {% endfor %}

  {# ── Audit table ── #}
  {% set create_audit %}
    CREATE OR REPLACE VIEW tpcdi.audit AS
    SELECT * FROM delta_scan('/data/raw/delta/audit')
  {% endset %}
  {% do run_query(create_audit) %}

{% endmacro %}
