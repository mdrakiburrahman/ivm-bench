WITH s1 AS (
  SELECT
    hh_t_id AS trade_id,
    hh_h_t_id AS previous_trade_id,
    hh_before_qty AS previous_quantity,
    hh_after_qty AS quantity
  FROM {{ ref('brokerage_holding_history') }}
)
SELECT
  s1.*,
  ct.account_id,
  ct.symbol,
  ct.create_timestamp,
  ct.close_timestamp,
  ct.trade_price,
  ct.bid_price,
  ct.fee,
  ct.commission
FROM s1
JOIN {{ ref('trades') }} AS ct
  USING (trade_id)
