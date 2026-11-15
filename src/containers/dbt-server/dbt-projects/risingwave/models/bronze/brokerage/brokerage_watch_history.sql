/* Bronze: read watch_history from staging table (all loaded batches) */
SELECT
  w_c_id,
  w_s_symb,
  w_dts,
  w_action
FROM {{ source('tpcdi', 'staging_watch_history') }}
