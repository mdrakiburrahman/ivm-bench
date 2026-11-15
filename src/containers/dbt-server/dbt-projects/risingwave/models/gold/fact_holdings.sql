WITH s1 AS (
  SELECT
    *
  FROM {{ ref('holdings_history') }}
)
SELECT
  ct.sk_trade_id AS sk_current_trade_id,
  pt.sk_trade_id,
  sk_customer_id,
  sk_account_id,
  sk_security_id,
  CAST(s1.create_timestamp AS DATE) AS sk_trade_date,
  s1.create_timestamp AS trade_timestamp,
  s1.trade_price AS current_price,
  s1.quantity AS current_holding,
  s1.bid_price AS current_bid_price,
  s1.fee AS current_fee,
  s1.commission AS current_commission
FROM s1
JOIN {{ ref('dim_trade') }} AS ct
  USING (trade_id)
JOIN {{ ref('dim_trade') }} AS pt
  ON s1.previous_trade_id = pt.trade_id
JOIN {{ ref('dim_account') }} AS a
  ON s1.account_id = a.account_id
  AND s1.create_timestamp BETWEEN a.effective_timestamp AND a.end_timestamp
JOIN {{ ref('dim_security') }} AS s
  ON s1.symbol = s.symbol
