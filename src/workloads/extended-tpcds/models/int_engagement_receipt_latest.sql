SELECT
  receipt_time,
  engagement_path,
  customer_path,
  receipt_reported_units
FROM (
  SELECT
    receipt_time,
    engagement_path AS engagement_path,
    customer_path,
    receipt_reported_units,
    ROW_NUMBER() OVER (PARTITION BY engagement_path, customer_path ORDER BY receipt_time DESC, receipt_reported_units DESC NULLS LAST) AS rn
  FROM stg_purchase_receipt_events
) AS ranked
WHERE
  rn = 1
