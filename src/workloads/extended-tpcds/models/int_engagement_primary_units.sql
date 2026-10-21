SELECT
  iid.engagement_path,
  iid.customer_path,
  iid.reported_units AS engagement_reported_units,
  mhmv.parent_capacity AS customer_reported_capacity,
  TRY_CAST(NULL AS INT) AS receipt_reported_units
FROM stg_engagement_capacity_history AS iid
LEFT JOIN int_customer_profile AS mhmv
  ON iid.customer_path = mhmv.customer_path
