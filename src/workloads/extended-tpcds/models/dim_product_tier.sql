WITH editions_deduped AS (
  SELECT DISTINCT
    product_tier
  FROM (
    SELECT
      'UNKNOWN' AS product_tier
    UNION ALL
    SELECT
      product_tier
    FROM stg_engagement_product_history
    WHERE
      NOT product_tier IS NULL
      AND product_tier <> 'UNKNOWN'
      AND product_tier IN ('BASIC', 'STANDARD', 'PREMIUM')
  ) AS combined
), ranked AS (
  SELECT
    TRY_CAST(ROW_NUMBER() OVER (
      ORDER BY CASE WHEN product_tier = 'UNKNOWN' THEN 0 ELSE 1 END NULLS FIRST, product_tier NULLS FIRST
    ) AS INT) AS product_tier_key,
    product_tier AS product_tier
  FROM editions_deduped
)
SELECT
  product_tier_key,
  product_tier
FROM ranked
