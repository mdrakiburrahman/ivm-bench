SELECT
  TRY_CAST(ROW_NUMBER() OVER (
    ORDER BY a.report_as_of_date NULLS FIRST, a.household_key NULLS FIRST, a.product_version_key NULLS FIRST, a.product_tier_key NULLS FIRST, a.preference_key NULLS FIRST, a.service_role NULLS FIRST, a.reported_platform NULLS FIRST, a.policy_type NULLS FIRST, a.channel NULLS FIRST, a.eligibility_status NULLS FIRST
  ) AS INT) AS primary_key,
  a.report_as_of_date,
  a.household_key,
  a.product_version_key,
  a.product_tier_key,
  a.preference_key,
  a.service_role,
  a.reported_platform,
  a.policy_type,
  a.channel,
  a.eligibility_status,
  TRY_CAST(a.engagement_count AS INT) AS engagement_count,
  TRY_CAST(a.allocated_unit_count AS DOUBLE) AS allocated_unit_count,
  TRY_CAST(COALESCE(m.parent_capacity_sum, 0) AS INT) AS parent_capacity_sum,
  TRY_CAST(COALESCE(m.parent_limit_sum, 0) AS INT) AS parent_limit_sum
FROM (
  SELECT
    TRY_CAST('__REPORT_DATE__' AS DATE) AS report_as_of_date,
    household_key,
    product_version_key,
    product_tier_key,
    preference_key,
    service_role,
    reported_platform,
    policy_type,
    channel,
    eligibility_status,
    COUNT(DISTINCT engagement_registry_key) AS engagement_count,
    SUM(allocated_units) AS allocated_unit_count
  FROM int_pit_unpivoted_7d
  GROUP BY
    household_key,
    product_version_key,
    product_tier_key,
    preference_key,
    service_role,
    reported_platform,
    policy_type,
    channel,
    eligibility_status
) AS a
LEFT JOIN (
  SELECT
    household_key,
    product_version_key,
    product_tier_key,
    preference_key,
    service_role,
    reported_platform,
    policy_type,
    channel,
    eligibility_status,
    SUM(parent_capacity) AS parent_capacity_sum,
    SUM(parent_limit) AS parent_limit_sum
  FROM (
    SELECT DISTINCT
      household_key,
      product_version_key,
      product_tier_key,
      preference_key,
      service_role,
      reported_platform,
      policy_type,
      channel,
      eligibility_status,
      customer_path,
      parent_capacity,
      parent_limit
    FROM int_pit_unpivoted_7d
    WHERE
      NOT customer_path IS NULL
  ) AS d
  GROUP BY
    household_key,
    product_version_key,
    product_tier_key,
    preference_key,
    service_role,
    reported_platform,
    policy_type,
    channel,
    eligibility_status
) AS m
  ON a.household_key = m.household_key
  AND a.product_version_key = m.product_version_key
  AND a.product_tier_key = m.product_tier_key
  AND a.preference_key = m.preference_key
  AND COALESCE(a.service_role, '') = COALESCE(m.service_role, '')
  AND COALESCE(a.reported_platform, '') = COALESCE(m.reported_platform, '')
  AND COALESCE(a.policy_type, '') = COALESCE(m.policy_type, '')
  AND a.channel = m.channel
  AND a.eligibility_status = m.eligibility_status
WHERE
  a.engagement_count > 0
