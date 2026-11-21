SELECT
  engagement_registry_key,
  event_date,
  observation_time,
  ingestion_time,
  assessment_key
FROM stg_engagement_status_events
WHERE
  event_date BETWEEN CAST(CAST(TRY_CAST('__REPORT_DATE__' AS DATE) + INTERVAL (-__history_months__) MONTH AS DATE) AS DATE) + INTERVAL (-27) DAY AND TRY_CAST('__REPORT_DATE__' AS DATE)
  OR TRY_CAST(observation_time AS DATE) BETWEEN CAST(CAST(TRY_CAST('__REPORT_DATE__' AS DATE) + INTERVAL (-__history_months__) MONTH AS DATE) AS DATE) + INTERVAL (-27) DAY AND TRY_CAST('__REPORT_DATE__' AS DATE)
