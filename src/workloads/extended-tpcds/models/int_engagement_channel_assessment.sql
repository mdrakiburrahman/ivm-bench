SELECT
  lmk.engagement_path,
  CASE WHEN msl.store_eligibility IN ('ready', 'notready') THEN 1 ELSE 0 END AS store_evaluated,
  CASE WHEN msl.catalog_eligibility IN ('ready', 'notready') THEN 1 ELSE 0 END AS catalog_evaluated,
  CASE WHEN msl.web_eligibility IN ('ready', 'notready') THEN 1 ELSE 0 END AS web_evaluated,
  CASE WHEN msl.store_eligibility = 'ready' THEN 1 ELSE 0 END AS store_eligible,
  CASE WHEN msl.catalog_eligibility = 'ready' THEN 1 ELSE 0 END AS catalog_eligible,
  CASE WHEN msl.web_eligibility = 'ready' THEN 1 ELSE 0 END AS web_eligible
FROM int_engagement_assessment_key_latest AS lmk
LEFT JOIN int_channel_status_latest AS msl
  ON lmk.assessment_key = msl.assessment_key
