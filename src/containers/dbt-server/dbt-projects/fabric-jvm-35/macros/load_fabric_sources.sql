{#-- On-run-start hook: stage the TPC-DI source tables into the lakehouse from
     the OneLake Files cache that fabric.py (azcopy) has already populated.

     Runs INSIDE the dbt Livy session so the openivm extension sees the source
     CREATE/INSERT (CDF-tracked) exactly as the local spark-openivm flow does.

     FABRIC_BATCH_NUM selects the phase:
       1        -> CREATE the batch1 reference + initial staging + audit tables
                   (managed Delta, CDF enabled) from sf=<N>/{batch1,staging,audit}
       2 | 3    -> INSERT the per-batch staging increment from
                   sf=<N>/staging_batch<N>/<t> into the growing staging_<t> tables
--#}
{% macro load_fabric_sources() %}
  {% if target.type != 'fabricspark' %}{% do return('') %}{% endif %}

  {% set batch = (env_var('FABRIC_BATCH_NUM', '1')) | int %}
  {% set sf = env_var('SCALE_FACTOR', '3') %}
  {% set ws = env_var('FABRIC_WORKSPACE_ID', '') %}
  {% set lh = env_var('FABRIC_CACHE_LAKEHOUSE_ID', '') %}
  {% set onelake = env_var('FABRIC_ONELAKE_HOST', 'msit-onelake.dfs.fabric.microsoft.com') %}
  {% set db = target.schema %}
  {% set cache = 'abfss://' ~ ws ~ '@' ~ onelake ~ '/' ~ lh ~ '/' ~ env_var('FABRIC_CACHE_ROOT') %}

  {% set batch1_tables = ['customer_mgmt','date','finwire','hr','industry','status_type','tax_rate','trade_history','trade_type'] %}
  {% set staging_tables = ['cash_transaction','daily_market','holding_history','prospect','trade','watch_history','account','customer','batch_date'] %}
  {% set tblprops = "TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')" %}

  {% if batch <= 1 %}
    {#-- Deleting OneLake files (fabric.py cleanup) does NOT unregister the
         Fabric metastore entry, so a prior engine's same-named relation leaks
         in. DROP in-session to clear the catalog before recreating. Guarded on
         `execute` so the result-returning run_query is skipped at parse time. --#}
    {% if execute %}
      {% set existing = run_query('SHOW TABLES IN ' ~ db) %}
      {% set dropped = namespace(n=0) %}
      {% if existing is not none %}
        {% for row in existing.rows %}
          {% if (row[-1] | string | lower) != 'true' %}
            {% do run_query('DROP TABLE IF EXISTS ' ~ db ~ '.`' ~ row[1] ~ '`') %}
            {% set dropped.n = dropped.n + 1 %}
          {% endif %}
        {% endfor %}
      {% endif %}
      {{ log("[fabric] load_fabric_sources: dropped " ~ dropped.n ~ " pre-existing relations in " ~ db, info=True) }}
    {% endif %}
    {{ log("[fabric] load_fabric_sources: CREATE sources (batch 1, sf=" ~ sf ~ ")", info=True) }}
    {% for t in batch1_tables %}
      {% set sql %}CREATE OR REPLACE TABLE {{ db }}.batch1_{{ t }} USING DELTA {{ tblprops }} AS SELECT * FROM delta.`{{ cache }}/batch1/{{ t }}`{% endset %}
      {% do run_query(sql) %}
    {% endfor %}
    {% for t in staging_tables %}
      {% set sql %}CREATE OR REPLACE TABLE {{ db }}.staging_{{ t }} USING DELTA {{ tblprops }} AS SELECT * FROM delta.`{{ cache }}/staging/{{ t }}`{% endset %}
      {% do run_query(sql) %}
    {% endfor %}
    {% set sql %}CREATE OR REPLACE TABLE {{ db }}.audit USING DELTA {{ tblprops }} AS SELECT * FROM delta.`{{ cache }}/audit`{% endset %}
    {% do run_query(sql) %}
  {% else %}
    {{ log("[fabric] load_fabric_sources: INSERT staging increment (batch " ~ batch ~ ", sf=" ~ sf ~ ")", info=True) }}
    {% for t in staging_tables %}
      {% set sql %}INSERT INTO {{ db }}.staging_{{ t }} SELECT * FROM delta.`{{ cache }}/staging_batch{{ batch }}/{{ t }}`{% endset %}
      {% do run_query(sql) %}
    {% endfor %}
  {% endif %}
{% endmacro %}
