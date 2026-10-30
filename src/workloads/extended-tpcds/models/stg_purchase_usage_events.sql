SELECT
  STRPTIME(purchase_date_key, '%Y%m%d') + (
    TRY_CAST(purchase_second_key AS INT) * INTERVAL '1' SECOND
  ) AS event_time,
  registry_key,
  price_catalog_id,
  purchase_quantity
FROM ext_purchase_usage_events
WHERE
  CAST(CAST(TRY_STRPTIME(COALESCE(event_month, '') || '01', '%Y%m%d') AS TIMESTAMP) AS DATE) BETWEEN TRY_CAST('__REPORT_DATE__' AS DATE) + INTERVAL (-(
    365
  )) DAY AND TRY_CAST('__REPORT_DATE__' AS DATE)
  AND 1 = 1
