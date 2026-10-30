WITH latest AS (
  SELECT
    event_time,
    household_key,
    account_offer_key,
    account_usage_key,
    platform_key,
    preference_key,
    product_tier_key,
    product_version_key,
    engagement_type_key,
    mode_key,
    engagement_path,
    effective_units,
    dim_date_key,
    ROW_NUMBER() OVER (PARTITION BY engagement_path, dim_date_key ORDER BY event_time DESC) AS rn
  FROM (
    SELECT
      event_time,
      household_key,
      account_offer_key,
      account_usage_key,
      platform_key,
      preference_key,
      product_tier_key,
      product_version_key,
      engagement_type_key,
      mode_key,
      engagement_path,
      effective_units,
      TRY_CAST(YEAR(CAST(event_time AS DATE)) * 10000 + MONTH(CAST(event_time AS DATE)) * 100 + 1 AS INT) AS dim_date_key
    FROM int_engagement_status_transaction
  )
  WHERE
    event_time >= TRY_CAST('__REPORT_DATE__' AS DATE) + INTERVAL (-(
      365
    )) DAY
), deduped AS (
  SELECT
    event_time,
    household_key,
    account_offer_key,
    account_usage_key,
    platform_key,
    preference_key,
    product_tier_key,
    product_version_key,
    engagement_type_key,
    mode_key,
    engagement_path,
    effective_units,
    dim_date_key
  FROM latest
  WHERE
    rn = 1
), agg AS (
  SELECT
    dim_date_key,
    household_key,
    account_offer_key,
    account_usage_key,
    platform_key,
    preference_key,
    product_tier_key,
    product_version_key,
    engagement_type_key,
    mode_key,
    COUNT(DISTINCT engagement_path) AS engagement_total_count,
    SUM(effective_units) AS effective_units_count
  FROM deduped
  GROUP BY
    dim_date_key,
    household_key,
    account_offer_key,
    account_usage_key,
    platform_key,
    preference_key,
    product_tier_key,
    product_version_key,
    engagement_type_key,
    mode_key
)
SELECT
  TRY_CAST(ROW_NUMBER() OVER (
    ORDER BY dim_date_key NULLS FIRST, household_key NULLS FIRST, account_offer_key NULLS FIRST, account_usage_key NULLS FIRST, platform_key NULLS FIRST, preference_key NULLS FIRST, product_tier_key NULLS FIRST, product_version_key NULLS FIRST, engagement_type_key NULLS FIRST, mode_key NULLS FIRST
  ) AS INT) AS primary_key,
  dim_date_key,
  household_key,
  account_offer_key,
  account_usage_key,
  platform_key,
  preference_key,
  product_tier_key,
  product_version_key,
  engagement_type_key,
  mode_key,
  TRY_CAST(engagement_total_count AS INT) AS engagement_total_count,
  TRY_CAST(effective_units_count AS INT) AS effective_units_count
FROM agg
WHERE
  engagement_total_count > 0
