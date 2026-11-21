SELECT
  customer_registry_key,
  STRPTIME(event_date_key, '%Y%m%d') + (
    TRY_CAST(event_second_key AS INT) * INTERVAL '1' SECOND
  ) AS event_time,
  LOWER(TRIM(customer_status)) AS customer_status,
  ingestion_time
FROM ext_customer_status_events
WHERE
  CAST(CAST(TRY_STRPTIME(COALESCE(event_month, '') || '01', '%Y%m%d') AS TIMESTAMP) AS DATE) BETWEEN TRY_CAST('__REPORT_DATE__' AS DATE) + INTERVAL (-(
    365
  )) DAY AND TRY_CAST('__REPORT_DATE__' AS DATE)
  AND 1 = 1
