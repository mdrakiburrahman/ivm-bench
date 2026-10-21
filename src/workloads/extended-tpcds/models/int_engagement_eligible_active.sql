WITH latest_engagement_health AS (
  SELECT
    engagement_path,
    activity_mode
  FROM (
    SELECT
      ihd.engagement_path AS engagement_path,
      ihd.activity_mode,
      ROW_NUMBER() OVER (PARTITION BY ihd.engagement_path ORDER BY ihd.effective_start DESC) AS rn
    FROM stg_engagement_activity_history AS ihd
    INNER JOIN int_target_engagements AS ti
      ON ihd.engagement_path = ti.engagement_path
  ) AS x
  WHERE
    rn = 1
), latest_engagement_hadr AS (
  SELECT
    engagement_path,
    service_role
  FROM (
    SELECT
      ihd.engagement_path AS engagement_path,
      ihd.service_role,
      ROW_NUMBER() OVER (PARTITION BY ihd.engagement_path ORDER BY ihd.effective_start DESC) AS rn
    FROM stg_engagement_service_history AS ihd
    INNER JOIN int_target_engagements AS ti
      ON ihd.engagement_path = ti.engagement_path
  ) AS x
  WHERE
    rn = 1
), target_customer_collection_keys AS (
  SELECT DISTINCT
    customer_registry_key
  FROM int_engagement_customer_registry
  WHERE
    NOT customer_registry_key IS NULL
), latest_customer_status AS (
  SELECT
    customer_registry_key,
    customer_status
  FROM (
    SELECT
      msf.customer_registry_key,
      msf.customer_status,
      ROW_NUMBER() OVER (PARTITION BY msf.customer_registry_key ORDER BY msf.ingestion_time DESC, msf.customer_status DESC NULLS LAST) AS rn
    FROM stg_customer_status_events AS msf
    INNER JOIN target_customer_collection_keys AS tmck
      ON msf.customer_registry_key = tmck.customer_registry_key
  ) AS x
  WHERE
    rn = 1
)
SELECT
  ti.engagement_registry_key,
  ti.engagement_path,
  ti.household_id,
  ti.allocated_units,
  lih.activity_mode,
  lihd.service_role,
  lmc.customer_path,
  lmc.customer_registry_key
FROM int_target_engagements AS ti
LEFT JOIN latest_engagement_health AS lih
  ON ti.engagement_path = lih.engagement_path
LEFT JOIN latest_engagement_hadr AS lihd
  ON ti.engagement_path = lihd.engagement_path
LEFT JOIN int_engagement_customer_registry AS lmc
  ON ti.engagement_path = lmc.engagement_path
LEFT JOIN latest_customer_status AS lms
  ON lmc.customer_registry_key = lms.customer_registry_key
WHERE
  COALESCE(lms.customer_status, '') <> 'inactive'
