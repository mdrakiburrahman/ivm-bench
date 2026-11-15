SELECT
  *
FROM {{ source('tpcdi', 'batch1_status_type') }}
