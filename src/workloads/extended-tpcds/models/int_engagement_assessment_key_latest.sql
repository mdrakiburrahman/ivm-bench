SELECT
  engagement_path,
  assessment_key
FROM (
  SELECT
    acd.entity_path AS engagement_path,
    isf.assessment_key,
    ROW_NUMBER() OVER (PARTITION BY acd.entity_path ORDER BY isf.observation_time DESC, isf.assessment_key DESC NULLS LAST) AS arm_rn
  FROM (
    SELECT
      engagement_registry_key,
      assessment_key,
      observation_time,
      ROW_NUMBER() OVER (PARTITION BY engagement_registry_key ORDER BY observation_time DESC, assessment_key DESC NULLS LAST) AS rn
    FROM stg_engagement_status_events
  ) AS isf
  INNER JOIN stg_entity_registry AS acd
    ON isf.engagement_registry_key = acd.registry_key AND acd.rn = 1
  WHERE
    isf.rn = 1
) AS latest
WHERE
  arm_rn = 1
