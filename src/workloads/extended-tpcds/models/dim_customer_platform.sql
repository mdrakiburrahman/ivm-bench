WITH ranked AS (
  SELECT
    TRY_CAST(ROW_NUMBER() OVER (
      ORDER BY CASE WHEN platform_name = 'N/A' THEN 0 ELSE 1 END NULLS FIRST, platform_name NULLS FIRST
    ) AS INT) AS platform_key,
    platform_name
  FROM (
    SELECT DISTINCT
      COALESCE(platform_name, 'N/A') AS platform_name
    FROM stg_customer_profile_history
  ) AS dedup
)
SELECT
  platform_key,
  platform_name
FROM ranked
