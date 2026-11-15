SELECT
  *
FROM {{ source('tpcdi', 'batch1_industry') }}
