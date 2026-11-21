SELECT
  engagement_path_norm,
  product_version,
  product_tier
FROM (
  SELECT
    ivd.engagement_path AS engagement_path_norm,
    ivd.product_version,
    ivd.product_tier,
    ROW_NUMBER() OVER (PARTITION BY ivd.engagement_path ORDER BY ivd.effective_start DESC) AS rn
  FROM stg_engagement_product_history AS ivd
) AS ranked
WHERE
  rn = 1
