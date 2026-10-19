SELECT
  LOWER(TRIM(engagement_path)) AS engagement_path,
  NULLIF(TRIM(service_role), '') AS service_role,
  effective_start
FROM ext_engagement_service_history
WHERE
  is_current = TRUE AND 1 = 1
