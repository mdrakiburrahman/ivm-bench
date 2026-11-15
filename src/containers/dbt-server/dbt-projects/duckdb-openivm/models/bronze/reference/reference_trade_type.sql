SELECT
  *
FROM {{ source('tpcdi', 'batch1_trade_type') }}
