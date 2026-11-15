SELECT
  sk_customer_id,
  sk_security_id,
  CAST(placed_timestamp AS DATE) AS sk_date_placed,
  CAST(removed_timestamp AS DATE) AS sk_date_removed,
  1 AS watch_cnt
FROM {{ ref('watches') }} AS w
JOIN {{ ref('dim_customer') }} AS c
  ON w.customer_id = c.customer_id
  AND placed_timestamp BETWEEN c.effective_timestamp AND c.end_timestamp
JOIN {{ ref('dim_security') }} AS s
  ON w.symbol = s.symbol
  AND placed_timestamp BETWEEN s.effective_timestamp AND s.end_timestamp
