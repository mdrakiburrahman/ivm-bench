SELECT
  sk_trade_id,
  sk_broker_id,
  sk_customer_id,
  sk_account_id,
  sk_security_id,
  CAST(create_timestamp AS DATE) AS sk_create_date,
  create_timestamp,
  CAST(close_timestamp AS DATE) AS sk_close_date,
  close_timestamp,
  executed_by,
  quantity,
  bid_price,
  trade_price,
  fee,
  commission,
  tax
FROM {{ ref('trades') }} AS t
JOIN {{ ref('dim_trade') }} AS dt
  ON t.trade_id = dt.trade_id
  AND t.create_timestamp BETWEEN dt.effective_timestamp AND dt.end_timestamp
JOIN {{ ref('dim_account') }} AS a
  ON t.account_id = a.account_id
  AND t.create_timestamp BETWEEN a.effective_timestamp AND a.end_timestamp
JOIN {{ ref('dim_security') }} AS s
  ON t.symbol = s.symbol
  AND t.create_timestamp BETWEEN s.effective_timestamp AND s.end_timestamp
