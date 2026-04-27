{% macro register_tpcdi_sources() %}
  {#
    Register raw Delta Lake tables as views using DuckDB's delta_scan().
    Called via on-run-start in dbt_project.yml.
    Sources live at /data/raw/delta/<batch>/<table>.
  #}

  {# Ensure schemas exist #}
  {% set create_schema %}
    CREATE SCHEMA IF NOT EXISTS tpcdi
  {% endset %}
  {% do run_query(create_schema) %}

  {# ── Batch1-only tables (reference + historical) ── #}
  {% set batch1_tables = [
    'cash_transaction', 'customer_mgmt', 'daily_market', 'date', 'finwire',
    'holding_history', 'hr', 'industry', 'prospect', 'status_type',
    'tax_rate', 'time', 'trade', 'trade_history', 'trade_type', 'watch_history'
  ] %}

  {% for tbl in batch1_tables %}
    {% set create_view %}
      CREATE OR REPLACE VIEW tpcdi.batch1_{{ tbl }} AS
      SELECT * FROM delta_scan('/data/raw/delta/batch1/{{ tbl }}')
    {% endset %}
    {% do run_query(create_view) %}
  {% endfor %}

  {# ── Batch2/3 incremental tables ── #}
  {% set incremental_tables = [
    'account', 'batch_date', 'cash_transaction', 'customer',
    'daily_market', 'holding_history', 'prospect', 'trade', 'watch_history'
  ] %}

  {% for batch_num in [2, 3] %}
    {% for tbl in incremental_tables %}
      {% set create_view %}
        CREATE OR REPLACE VIEW tpcdi.batch{{ batch_num }}_{{ tbl }} AS
        SELECT * FROM delta_scan('/data/raw/delta/batch{{ batch_num }}/{{ tbl }}')
      {% endset %}
      {% do run_query(create_view) %}
    {% endfor %}
  {% endfor %}

  {# ── Audit table ── #}
  {% set create_audit %}
    CREATE OR REPLACE VIEW tpcdi.audit AS
    SELECT * FROM delta_scan('/data/raw/delta/audit')
  {% endset %}
  {% do run_query(create_audit) %}

{% endmacro %}
