WITH latest_engagement_customer AS (
  SELECT
    iml.engagement_path,
    iml.customer_path AS customer_path
  FROM int_engagement_customer_lookup AS iml
  INNER JOIN int_target_engagements AS ti
    ON iml.engagement_path = ti.engagement_path
  WHERE
    NOT iml.customer_path IS NULL AND iml.customer_path <> LOWER('NOT APPLICABLE')
)
SELECT
  engagement_path,
  customer_path,
  customer_registry_key
FROM (
  SELECT
    lim.engagement_path,
    lim.customer_path,
    mac.registry_key AS customer_registry_key,
    ROW_NUMBER() OVER (PARTITION BY lim.engagement_path ORDER BY mac.registry_key DESC) AS rn
  FROM latest_engagement_customer AS lim
  LEFT JOIN stg_entity_registry AS mac
    ON mac.rn = 1 AND mac.entity_path = lim.customer_path
) AS x
WHERE
  rn = 1
