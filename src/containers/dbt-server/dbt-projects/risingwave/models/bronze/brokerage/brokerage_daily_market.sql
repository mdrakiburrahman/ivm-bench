/* Bronze: read daily_market from staging table (all loaded batches) */
SELECT
  dm_date,
  dm_s_symb,
  dm_close,
  dm_high,
  dm_low,
  dm_vol
FROM {{ source('tpcdi', 'staging_daily_market') }}
