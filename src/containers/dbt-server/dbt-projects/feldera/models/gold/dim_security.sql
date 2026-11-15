WITH s1 AS (
  SELECT
    symbol,
    issue_type AS issue,
    s.status,
    s.name,
    exchange_id,
    sk_company_id,
    shares_outstanding,
    first_trade_date,
    first_exchange_date,
    dividend,
    s.effective_timestamp,
    s.end_timestamp,
    s.is_current
  FROM {{ ref("securities") }} AS s
  JOIN {{ ref("dim_company") }} AS c
    ON s.company_id = c.company_id
    AND s.effective_timestamp BETWEEN c.effective_timestamp AND c.end_timestamp
)
SELECT
  {{ dbt_utils.generate_surrogate_key(['symbol', 'effective_timestamp']) }} AS sk_security_id,
  *
FROM s1
