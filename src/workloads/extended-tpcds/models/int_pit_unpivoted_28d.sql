SELECT
  d.household_key,
  d.product_version_key,
  d.product_tier_key,
  d.preference_key,
  d.service_role,
  d.reported_platform,
  d.policy_type,
  d.engagement_registry_key,
  d.allocated_units,
  d.customer_path,
  d.parent_capacity,
  d.parent_limit,
  'store' AS channel,
  CASE
    WHEN d.store_eligibility = 'ready'
    THEN 'ready'
    WHEN d.store_eligibility = 'notready'
    THEN 'not ready'
    ELSE 'not assessed'
  END AS eligibility_status
FROM int_pit_active_28d AS d
UNION ALL
SELECT
  d.household_key,
  d.product_version_key,
  d.product_tier_key,
  d.preference_key,
  d.service_role,
  d.reported_platform,
  d.policy_type,
  d.engagement_registry_key,
  d.allocated_units,
  d.customer_path,
  d.parent_capacity,
  d.parent_limit,
  'catalog' AS channel,
  CASE
    WHEN d.catalog_eligibility = 'ready'
    THEN 'ready'
    WHEN d.catalog_eligibility = 'notready'
    THEN 'not ready'
    ELSE 'not assessed'
  END AS eligibility_status
FROM int_pit_active_28d AS d
UNION ALL
SELECT
  d.household_key,
  d.product_version_key,
  d.product_tier_key,
  d.preference_key,
  d.service_role,
  d.reported_platform,
  d.policy_type,
  d.engagement_registry_key,
  d.allocated_units,
  d.customer_path,
  d.parent_capacity,
  d.parent_limit,
  'web' AS channel,
  CASE
    WHEN d.web_eligibility = 'ready'
    THEN 'ready'
    WHEN d.web_eligibility = 'notready'
    THEN 'not ready'
    ELSE 'not assessed'
  END AS eligibility_status
FROM int_pit_active_28d AS d
