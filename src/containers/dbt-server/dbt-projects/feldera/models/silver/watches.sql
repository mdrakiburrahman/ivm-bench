WITH s1 AS (
  SELECT
    customer_id,
    symbol,
    watch_timestamp,
    action_type,
    company_id,
    company_name,
    exchange_id,
    security_status,
    CASE action_type WHEN 'Activate' THEN watch_timestamp ELSE NULL END AS placed_timestamp,
    CASE action_type WHEN 'Cancelled' THEN watch_timestamp ELSE NULL END AS removed_timestamp
  FROM {{ ref('watches_history') }}
), s2 AS (
  SELECT
    customer_id,
    symbol,
    company_id,
    company_name,
    exchange_id,
    security_status,
    MIN(placed_timestamp) AS placed_timestamp,
    MAX(removed_timestamp) AS removed_timestamp
  FROM s1
  GROUP BY
    customer_id,
    symbol,
    company_id,
    company_name,
    exchange_id,
    security_status
)
SELECT
  *,
  CASE WHEN removed_timestamp IS NULL THEN 'Active' ELSE 'Inactive' END AS watch_status
FROM s2
