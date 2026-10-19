WITH agg AS (
  SELECT
    TRY_CAST(YEAR(CAST(event_time AS DATE)) * 10000 + MONTH(CAST(event_time AS DATE)) * 100 + 1 AS INT) AS dim_date_key,
    engagement_path,
    price_catalog_id,
    household_key,
    account_offer_key,
    account_usage_key,
    platform_key,
    preference_key,
    product_tier_key,
    product_version_key,
    engagement_type_key,
    price_catalog_key,
    SUM(purchase_quantity) AS purchase_quantity_count
  FROM int_purchase_usage_transaction
  WHERE
    event_time >= TRY_CAST('__REPORT_DATE__' AS DATE) + INTERVAL (-(
      365
    )) DAY
  GROUP BY
    TRY_CAST(YEAR(CAST(event_time AS DATE)) * 10000 + MONTH(CAST(event_time AS DATE)) * 100 + 1 AS INT),
    engagement_path,
    price_catalog_id,
    household_key,
    account_offer_key,
    account_usage_key,
    platform_key,
    preference_key,
    product_tier_key,
    product_version_key,
    engagement_type_key,
    price_catalog_key
)
SELECT
  TRY_CAST(ROW_NUMBER() OVER (
    ORDER BY dim_date_key NULLS FIRST, household_key NULLS FIRST, account_offer_key NULLS FIRST, account_usage_key NULLS FIRST, platform_key NULLS FIRST, preference_key NULLS FIRST, product_tier_key NULLS FIRST, product_version_key NULLS FIRST, engagement_type_key NULLS FIRST, price_catalog_key NULLS FIRST, engagement_path NULLS FIRST, price_catalog_id NULLS FIRST
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
  price_catalog_key,
  purchase_quantity_count
FROM agg
