SELECT
  LOWER(TRIM(engagement_path)) AS engagement_path,
  UPPER(TRIM(product_version)) AS product_version,
  UPPER(TRIM(product_tier)) AS product_tier,
  effective_start
FROM ext_engagement_product_history
WHERE
  is_current = TRUE AND 1 = 1
