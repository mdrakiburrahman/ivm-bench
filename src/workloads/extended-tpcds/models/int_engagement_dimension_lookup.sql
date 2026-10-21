SELECT
  v.engagement_path,
  COALESCE(v.product_tier, 'UNKNOWN') AS product_tier,
  COALESCE(v.product_version, 'UNKNOWN') AS product_version,
  COALESCE(h.activity_mode, 'UNKNOWN') AS activity_mode,
  COALESCE(i.relationship_type, 'PURCHASE') AS relationship_type
FROM stg_engagement_product_history AS v
LEFT JOIN stg_engagement_activity_history AS h
  ON v.engagement_path = h.engagement_path
LEFT JOIN stg_engagement_capacity_history AS i
  ON v.engagement_path = i.engagement_path
