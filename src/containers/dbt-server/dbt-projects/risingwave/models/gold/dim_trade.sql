SELECT
  {{ dbt_utils.generate_surrogate_key(['trade_id', 't.effective_timestamp']) }} AS sk_trade_id,
  trade_id,
  trade_status AS status,
  transaction_type,
  trade_type AS type,
  executor_name AS executed_by,
  t.effective_timestamp,
  t.end_timestamp,
  t.is_current
FROM {{ ref('trades_history') }} AS t
