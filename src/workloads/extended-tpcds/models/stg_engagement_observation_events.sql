SELECT
  observation_time,
  LOWER(TRIM(engagement_path)) AS engagement_path,
  LOWER(TRIM(customer_path)) AS customer_path,
  TRY_CAST(reported_units AS INT) AS reported_units
FROM ext_engagement_observation_events
WHERE
  1 = 1
  AND CAST(CAST(TRY_STRPTIME(COALESCE(event_month, '') || '01', '%Y%m%d') AS TIMESTAMP) AS DATE) BETWEEN TRY_CAST('__REPORT_DATE__' AS DATE) + INTERVAL (-(
    365
  )) DAY AND TRY_CAST('__REPORT_DATE__' AS DATE)
