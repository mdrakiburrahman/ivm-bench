SELECT
  {{ dbt_utils.generate_surrogate_key(['account_id', 'a.effective_timestamp']) }} AS sk_account_id,
  a.account_id,
  sk_broker_id,
  sk_customer_id,
  a.status,
  account_desc,
  tax_status,
  a.effective_timestamp,
  a.end_timestamp,
  a.is_current
FROM {{ ref('accounts') }} AS a
JOIN {{ ref('dim_customer') }} AS c
  ON a.customer_id = c.customer_id
  AND a.effective_timestamp BETWEEN c.effective_timestamp AND c.end_timestamp
JOIN {{ ref('dim_broker') }} AS b
  USING (broker_id)
