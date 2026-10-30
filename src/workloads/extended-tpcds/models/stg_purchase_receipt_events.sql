SELECT
  receipt_time AS receipt_time,
  LOWER(TRIM(engagement_path)) AS engagement_path,
  COALESCE(LOWER(receipt_payload ->> '$.CustomerPath'), LOWER('NOT APPLICABLE')) AS customer_path,
  TRY_CAST(receipt_payload ->> '$.ReportedUnits' AS INT) AS receipt_reported_units
FROM ext_purchase_receipt_events
WHERE
  1 = 1
  AND CAST(CAST(TRY_STRPTIME(COALESCE(event_month, '') || '01', '%Y%m%d') AS TIMESTAMP) AS DATE) BETWEEN TRY_CAST('__REPORT_DATE__' AS DATE) + INTERVAL (-(
    365
  )) DAY AND TRY_CAST('__REPORT_DATE__' AS DATE)
