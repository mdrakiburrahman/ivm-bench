/* Bronze: read cash_transaction from staging table (all loaded batches) */
SELECT
  ct_ca_id,
  ct_dts,
  ct_amt,
  ct_name
FROM {{ source('tpcdi', 'staging_cash_transaction') }}
