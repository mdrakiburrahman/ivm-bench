SELECT
  assessment_key,
  LOWER(TRIM(store_eligibility)) AS store_eligibility,
  LOWER(TRIM(catalog_eligibility)) AS catalog_eligibility,
  LOWER(TRIM(web_eligibility)) AS web_eligibility,
  ingestion_time,
  effective_start
FROM ext_engagement_channel_history
WHERE
  is_current = TRUE AND 1 = 1
