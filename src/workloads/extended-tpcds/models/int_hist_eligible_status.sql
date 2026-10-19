SELECT
  isf.engagement_registry_key,
  isf.event_date AS effective_date,
  isf.observation_time,
  isf.ingestion_time,
  isf.assessment_key
FROM int_hist_raw_engagement_status AS isf
INNER JOIN int_engagement_eligible_active AS ei
  ON isf.engagement_registry_key = ei.engagement_registry_key
WHERE
  ei.activity_mode = 'observed'
UNION ALL
SELECT
  isf.engagement_registry_key,
  TRY_CAST(isf.observation_time AS DATE) AS effective_date,
  isf.observation_time,
  isf.ingestion_time,
  isf.assessment_key
FROM int_hist_raw_engagement_status AS isf
INNER JOIN int_engagement_eligible_active AS ei
  ON isf.engagement_registry_key = ei.engagement_registry_key
WHERE
  ei.activity_mode = 'reported'
