WITH latest AS (
  SELECT
    event_time,
    customer_path,
    household_key,
    account_offer_key,
    account_usage_key,
    platform_key,
    preference_key,
    store_active,
    catalog_active,
    web_active,
    parent_capacity,
    dim_date_key,
    ROW_NUMBER() OVER (PARTITION BY customer_path, dim_date_key ORDER BY event_time DESC) AS rn
  FROM (
    SELECT
      event_time,
      customer_path,
      household_key,
      account_offer_key,
      account_usage_key,
      platform_key,
      preference_key,
      store_active,
      catalog_active,
      web_active,
      parent_capacity,
      TRY_CAST(YEAR(CAST(event_time AS DATE)) * 10000 + MONTH(CAST(event_time AS DATE)) * 100 + 1 AS INT) AS dim_date_key
    FROM int_customer_status_transaction
  )
  WHERE
    event_time >= TRY_CAST('__REPORT_DATE__' AS DATE) + INTERVAL (-(
      365
    )) DAY
), deduped AS (
  SELECT
    event_time,
    customer_path,
    household_key,
    account_offer_key,
    account_usage_key,
    platform_key,
    preference_key,
    store_active,
    catalog_active,
    web_active,
    parent_capacity,
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
    SUM(CASE WHEN store_active = TRUE THEN 1 ELSE 0 END) AS store_active_count,
    SUM(CASE WHEN catalog_active = TRUE THEN 1 ELSE 0 END) AS catalog_active_count,
    SUM(CASE WHEN web_active = TRUE THEN 1 ELSE 0 END) AS web_active_count,
    COUNT(DISTINCT customer_path) AS customer_total_count,
    SUM(parent_capacity) AS parent_capacity_count
  FROM deduped
  GROUP BY
    dim_date_key,
    household_key,
    account_offer_key,
    account_usage_key,
    platform_key,
    preference_key
)
SELECT
  TRY_CAST(ROW_NUMBER() OVER (
    ORDER BY dim_date_key NULLS FIRST, household_key NULLS FIRST, account_offer_key NULLS FIRST, account_usage_key NULLS FIRST, platform_key NULLS FIRST, preference_key NULLS FIRST
  ) AS INT) AS primary_key,
  dim_date_key,
  household_key,
  account_offer_key,
  account_usage_key,
  platform_key,
  preference_key,
  TRY_CAST(store_active_count AS INT) AS store_active_count,
  TRY_CAST(catalog_active_count AS INT) AS catalog_active_count,
  TRY_CAST(web_active_count AS INT) AS web_active_count,
  TRY_CAST(customer_total_count AS INT) AS customer_total_count,
  TRY_CAST(parent_capacity_count AS INT) AS parent_capacity_count
FROM agg
WHERE
  customer_total_count > 0
