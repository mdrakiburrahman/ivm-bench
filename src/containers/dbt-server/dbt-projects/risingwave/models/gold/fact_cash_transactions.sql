WITH s1 AS (
  SELECT
    *,
    CAST(transaction_timestamp AS DATE) AS sk_transaction_date
  FROM {{ ref('cash_transactions') }}
)
SELECT
  sk_customer_id,
  sk_account_id,
  sk_transaction_date,
  transaction_timestamp,
  amount,
  description
FROM s1
JOIN {{ ref('dim_account') }} AS a
  ON s1.account_id = a.account_id
  AND s1.transaction_timestamp BETWEEN a.effective_timestamp AND a.end_timestamp
