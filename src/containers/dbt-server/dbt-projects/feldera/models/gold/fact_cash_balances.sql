WITH s1 AS (
  SELECT
    *
  FROM {{ ref('fact_cash_transactions') }}
)
SELECT
  sk_customer_id,
  sk_account_id,
  sk_transaction_date,
  SUM(amount) AS amount,
  description
FROM s1
GROUP BY
  sk_customer_id,
  sk_account_id,
  sk_transaction_date,
  description
