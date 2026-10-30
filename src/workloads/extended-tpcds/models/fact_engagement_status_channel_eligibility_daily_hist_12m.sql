SELECT
  MD5(
    CONCAT_WS(
      '-',
      COALESCE(TRY_CAST(a.report_as_of_date AS TEXT), ''),
      COALESCE(TRY_CAST(a.household_id AS TEXT), ''),
      COALESCE(TRY_CAST(a.household_key AS TEXT), ''),
      COALESCE(TRY_CAST(a.product_version_key AS TEXT), ''),
      COALESCE(TRY_CAST(a.product_tier_key AS TEXT), ''),
      COALESCE(TRY_CAST(a.preference_key AS TEXT), ''),
      COALESCE(TRY_CAST(a.service_role AS TEXT), ''),
      COALESCE(TRY_CAST(a.reported_platform AS TEXT), ''),
      COALESCE(TRY_CAST(a.policy_type AS TEXT), ''),
      COALESCE(TRY_CAST(a.channel AS TEXT), ''),
      COALESCE(TRY_CAST(a.eligibility_status AS TEXT), '')
    )
  ) AS primary_key,
  a.report_as_of_date,
  a.household_id,
  a.channel,
  a.eligibility_status,
  TRY_CAST(a.engagement_count AS INT) AS engagement_count,
  TRY_CAST(a.order_line_count AS INT) AS order_line_count,
  TRY_CAST(a.allocated_unit_count AS DOUBLE) AS allocated_unit_count,
  a.household_key,
  a.product_version_key,
  a.product_tier_key,
  a.preference_key,
  a.service_role,
  a.reported_platform,
  a.policy_type,
  TRY_CAST(COALESCE(m.parent_capacity_sum, 0) AS INT) AS parent_capacity_sum,
  TRY_CAST(COALESCE(m.parent_limit_sum, 0) AS INT) AS parent_limit_sum
FROM (
  SELECT
    report_as_of_date,
    household_id,
    channel,
    eligibility_status,
    household_key,
    product_version_key,
    product_tier_key,
    preference_key,
    service_role,
    reported_platform,
    policy_type,
    COUNT(DISTINCT engagement_registry_key) AS engagement_count,
    SUM(allocated_units) AS allocated_unit_count,
    SUM(order_line_count) AS order_line_count
  FROM int_hist_unpivoted_daily
  GROUP BY
    report_as_of_date,
    household_id,
    channel,
    eligibility_status,
    household_key,
    product_version_key,
    product_tier_key,
    preference_key,
    service_role,
    reported_platform,
    policy_type
) AS a
LEFT JOIN (
  SELECT
    report_as_of_date,
    household_id,
    channel,
    eligibility_status,
    household_key,
    product_version_key,
    product_tier_key,
    preference_key,
    service_role,
    reported_platform,
    policy_type,
    SUM(parent_capacity) AS parent_capacity_sum,
    SUM(parent_limit) AS parent_limit_sum
  FROM (
    SELECT DISTINCT
      report_as_of_date,
      household_id,
      channel,
      eligibility_status,
      household_key,
      product_version_key,
      product_tier_key,
      preference_key,
      service_role,
      reported_platform,
      policy_type,
      customer_path,
      parent_capacity,
      parent_limit
    FROM int_hist_unpivoted_daily
    WHERE
      NOT customer_path IS NULL
  ) AS distinct_customers
  GROUP BY
    report_as_of_date,
    household_id,
    channel,
    eligibility_status,
    household_key,
    product_version_key,
    product_tier_key,
    preference_key,
    service_role,
    reported_platform,
    policy_type
) AS m
  ON a.report_as_of_date = m.report_as_of_date
  AND a.household_id IS NOT DISTINCT FROM m.household_id
  AND a.channel = m.channel
  AND a.eligibility_status = m.eligibility_status
  AND a.household_key = m.household_key
  AND a.product_version_key = m.product_version_key
  AND a.product_tier_key = m.product_tier_key
  AND a.preference_key = m.preference_key
  AND COALESCE(a.service_role, '') = COALESCE(m.service_role, '')
  AND COALESCE(a.reported_platform, '') = COALESCE(m.reported_platform, '')
  AND COALESCE(a.policy_type, '') = COALESCE(m.policy_type, '')
