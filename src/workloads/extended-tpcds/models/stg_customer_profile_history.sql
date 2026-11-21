SELECT
  LOWER(TRIM(customer_path)) AS customer_path,
  platform_name,
  NULLIF(TRIM(reported_platform), '') AS reported_platform,
  parent_capacity,
  parent_limit,
  customer_segment,
  customer_segment_detail,
  store_active,
  catalog_active,
  web_active,
  ingestion_time,
  effective_start
FROM ext_customer_profile_history
WHERE
  is_current = TRUE AND 1 = 1
