SELECT DISTINCT
  TRY_CAST(YEAR(CAST(event_time AS DATE)) * 10000 + MONTH(CAST(event_time AS DATE)) * 100 + 1 AS INT) AS dim_date_key,
  household_key,
  account_offer_key,
  account_usage_key,
  platform_key,
  preference_key,
  product_tier_key,
  product_version_key,
  engagement_type_key,
  engagement_path
FROM int_purchase_usage_transaction
WHERE
  event_time >= TRY_CAST('__REPORT_DATE__' AS DATE) + INTERVAL (-(
    365
  )) DAY
