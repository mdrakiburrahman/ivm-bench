/* Work: company financials pre-joined for fact_market_history */
SELECT
  f.company_id,
  sk_company_id,
  f.eps,
  f.revenue,
  f.effective_timestamp,
  f.end_timestamp,
  f.is_current
FROM {{ ref('financials') }} AS f
JOIN {{ ref('dim_company') }} AS c
  ON f.company_id = c.company_id
  AND f.effective_timestamp BETWEEN c.effective_timestamp AND c.end_timestamp
