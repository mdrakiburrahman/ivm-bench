WITH workload_types AS (
  SELECT DISTINCT
    COALESCE(NULLIF(account_usage, ''), 'Unknown') AS account_usage
  FROM int_account
), ranked AS (
  SELECT
    TRY_CAST(ROW_NUMBER() OVER (
      ORDER BY CASE WHEN account_usage = 'Unknown' THEN 0 ELSE 1 END NULLS FIRST, account_usage NULLS FIRST
    ) AS INT) AS account_usage_key,
    account_usage
  FROM workload_types
)
SELECT
  account_usage_key,
  account_usage
FROM ranked
