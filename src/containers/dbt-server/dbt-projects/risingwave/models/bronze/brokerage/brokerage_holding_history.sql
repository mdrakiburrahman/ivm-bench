/* Bronze: read holding_history from staging table (all loaded batches) */
SELECT
  hh_h_t_id,
  hh_t_id,
  hh_before_qty,
  hh_after_qty
FROM {{ source('tpcdi', 'staging_holding_history') }}
