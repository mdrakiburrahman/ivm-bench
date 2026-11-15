SELECT
  *
FROM {{ source('tpcdi', 'batch1_tax_rate') }}
