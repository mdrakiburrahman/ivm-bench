WITH s1 AS (
  SELECT
    w_c_id AS customer_id,
    w_s_symb AS symbol,
    w_dts AS watch_timestamp,
    CASE w_action WHEN 'ACTV' THEN 'Activate' WHEN 'CNCL' THEN 'Cancelled' ELSE NULL END AS action_type
  FROM {{ ref('brokerage_watch_history') }}
)
SELECT
  s1.*,
  s.company_id,
  s.company_name,
  s.exchange_id,
  s.status AS security_status
FROM s1
JOIN {{ ref('securities') }} AS s
  USING (symbol)
