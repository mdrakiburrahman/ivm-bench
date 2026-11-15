WITH t AS (
  SELECT
    ct_ca_id AS account_id,
    ct_dts AS transaction_timestamp,
    ct_amt AS amount,
    ct_name AS description
  FROM {{ ref('brokerage_cash_transaction') }}
)
SELECT
  a.customer_id,
  t.*
FROM t
JOIN {{ ref('accounts') }} AS a
  ON t.account_id = a.account_id
  AND t.transaction_timestamp BETWEEN a.effective_timestamp AND a.end_timestamp
