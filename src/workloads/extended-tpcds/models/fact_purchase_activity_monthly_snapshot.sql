WITH agg AS (
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
    COUNT(*) AS engagement_total_count
  FROM int_purchase_activity_monthly_distinct
  GROUP BY
    dim_date_key,
    household_key,
    account_offer_key,
    account_usage_key,
    platform_key,
    preference_key,
    product_tier_key,
    product_version_key,
    engagement_type_key
)
SELECT
  TRY_CAST(ROW_NUMBER() OVER (
    ORDER BY dim_date_key NULLS FIRST, household_key NULLS FIRST, account_offer_key NULLS FIRST, account_usage_key NULLS FIRST, platform_key NULLS FIRST, preference_key NULLS FIRST, product_tier_key NULLS FIRST, product_version_key NULLS FIRST, engagement_type_key NULLS FIRST
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
  TRY_CAST(engagement_total_count AS INT) AS engagement_total_count
FROM agg
