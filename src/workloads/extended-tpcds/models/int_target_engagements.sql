SELECT
  ac.registry_key AS engagement_registry_key,
  ac.entity_path AS engagement_path,
  ac.account_id,
  cs.household_id,
  ivc.allocated_units
FROM stg_entity_registry AS ac
LEFT JOIN (
  SELECT
    account_id,
    household_id
  FROM (
    SELECT
      account_id,
      household_id,
      ROW_NUMBER() OVER (PARTITION BY account_id ORDER BY effective_start DESC, ingestion_time DESC) AS rn
    FROM stg_account_profile
  ) AS t
  WHERE
    rn = 1
) AS cs
  ON ac.account_id = cs.account_id
INNER JOIN int_engagement_allocated_units AS ivc
  ON ac.entity_path = ivc.engagement_path_norm
WHERE
  ac.rn = 1 AND ac.entity_type = 'engagements'
AND ('__COHORT__' = 'all' OR ac.account_id IN (SELECT account_id FROM stg_account_profile WHERE household_type = '__COHORT__'))
