WITH latest_customer_infrastructure AS (
  SELECT
    customer_path,
    platform_name,
    reported_platform,
    parent_capacity,
    parent_limit,
    customer_segment,
    customer_segment_detail
  FROM (
    SELECT
      mod.customer_path,
      mod.platform_name,
      mod.reported_platform,
      mod.parent_capacity,
      mod.parent_limit,
      mod.customer_segment,
      mod.customer_segment_detail,
      ROW_NUMBER() OVER (
        PARTITION BY mod.customer_path
        ORDER BY mod.ingestion_time DESC, mod.effective_start DESC
      ) AS rn
    FROM stg_customer_profile_history AS mod
  ) AS ranked
  WHERE
    rn = 1
), latest_customer_os AS (
  SELECT
    customer_path,
    preference_sku,
    preference_type
  FROM (
    SELECT
      mid.customer_path,
      mid.preference_sku,
      mid.preference_type,
      ROW_NUMBER() OVER (
        PARTITION BY mid.customer_path
        ORDER BY mid.ingestion_time DESC, mid.effective_start DESC
      ) AS rn
    FROM stg_customer_preference_history AS mid
  ) AS ranked
  WHERE
    rn = 1
), deduped AS (
  SELECT
    mod.customer_path,
    mod.platform_name,
    mod.reported_platform,
    mod.parent_capacity,
    mod.parent_limit,
    mod.customer_segment,
    mod.customer_segment_detail,
    mid.preference_sku,
    mid.preference_type
  FROM latest_customer_infrastructure AS mod
  INNER JOIN latest_customer_os AS mid
    ON mod.customer_path = mid.customer_path
)
SELECT
  customer_path,
  platform_name,
  reported_platform,
  parent_capacity,
  parent_limit,
  customer_segment,
  customer_segment_detail,
  preference_sku,
  preference_type
FROM deduped
