WITH s1 AS (
  SELECT DISTINCT
    trade_id,
    account_id,
    trade_status,
    trade_type,
    transaction_type,
    symbol,
    executor_name,
    quantity,
    bid_price,
    trade_price,
    fee,
    commission,
    tax,
    MIN(effective_timestamp) OVER (PARTITION BY trade_id) AS create_timestamp,
    MAX(effective_timestamp) OVER (PARTITION BY trade_id) AS close_timestamp
  FROM {{ ref('trades_history') }}
)
SELECT
  *
FROM s1
