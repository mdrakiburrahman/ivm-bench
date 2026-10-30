SELECT
  silver.engagement_path,
  silver.customer_path,
  silver.engagement_reported_units
FROM (
  SELECT
    engagement_path AS engagement_path,
    COALESCE(customer_path, LOWER('NOT APPLICABLE')) AS customer_path,
    reported_units AS engagement_reported_units,
    ROW_NUMBER() OVER (PARTITION BY engagement_path ORDER BY observation_time DESC, reported_units DESC NULLS LAST, customer_path DESC NULLS LAST) AS rn
  FROM stg_engagement_observation_events
) AS silver
WHERE
  silver.rn = 1
  AND NOT EXISTS(
    SELECT
      1
    FROM int_engagement_primary_units AS gold
    WHERE
      silver.engagement_path = gold.engagement_path
  )
