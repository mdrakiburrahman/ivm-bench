SELECT
  LOWER(TRIM(engagement_path)) AS engagement_path,
  LOWER(TRIM(activity_mode)) AS activity_mode,
  effective_start
FROM ext_engagement_activity_history
WHERE
  is_current = TRUE AND 1 = 1
