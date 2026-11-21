SELECT
  lis.report_as_of_date,
  lis.engagement_registry_key,
  lis.assessment_key,
  COALESCE(dc.household_key, 1) AS household_key,
  COALESCE(dc.household_type, 'Unknown') AS household_type,
  COALESCE(ei.allocated_units, 0.0) AS allocated_units,
  COALESCE(die.product_tier_key, 1) AS product_tier_key,
  COALESCE(dv.product_version_key, 1) AS product_version_key,
  COALESCE(dos.preference_key, 1) AS preference_key,
  ei.service_role,
  mh.customer_path,
  COALESCE(mh.parent_capacity, 0) AS parent_capacity,
  COALESCE(mh.parent_limit, 0) AS parent_limit,
  mh.reported_platform,
  llc.policy_type,
  COALESCE(dbl.order_line_count, 0) AS order_line_count,
  ei.household_id,
  ms.store_eligibility,
  ms.catalog_eligibility,
  ms.web_eligibility,
  TRY_CAST(CASE
    WHEN ms.store_eligibility IN ('ready', 'notready')
    THEN 1
    WHEN ms.catalog_eligibility IN ('ready', 'notready')
    THEN 1
    WHEN ms.web_eligibility IN ('ready', 'notready')
    THEN 1
    ELSE 0
  END AS BOOLEAN) AS is_evaluated
FROM (
  SELECT
    report_as_of_date,
    engagement_registry_key,
    assessment_key
  FROM (
    SELECT
      csr.report_as_of_date,
      csr.engagement_registry_key,
      csr.assessment_key,
      ROW_NUMBER() OVER (
        PARTITION BY csr.report_as_of_date, csr.engagement_registry_key
        ORDER BY COALESCE(csr.observation_time, csr.ingestion_time) DESC, csr.assessment_key DESC
      ) AS rn
    FROM int_hist_active_candidates_daily AS csr
  ) AS ranked
  WHERE
    rn = 1
) AS lis
INNER JOIN int_engagement_eligible_active AS ei
  ON lis.engagement_registry_key = ei.engagement_registry_key
LEFT JOIN dim_household AS dc
  ON COALESCE(ei.household_id, -2147483648) = dc.household_id
LEFT JOIN int_engagement_version_latest AS iv
  ON ei.engagement_path = iv.engagement_path_norm
LEFT JOIN dim_product_tier AS die
  ON COALESCE(iv.product_tier, 'UNKNOWN') = die.product_tier
LEFT JOIN dim_product_version AS dv
  ON COALESCE(iv.product_version, 'UNKNOWN') = dv.product_version
LEFT JOIN int_customer_profile AS mh
  ON ei.customer_path = mh.customer_path
LEFT JOIN dim_customer_preference AS dos
  ON COALESCE(mh.preference_type, 'unknown') = dos.preference_type
  AND COALESCE(mh.preference_sku, 'Unknown') = dos.preference_sku
LEFT JOIN int_customer_policy_latest AS llc
  ON ei.customer_path = llc.customer_path_norm
LEFT JOIN int_engagement_order_line_counts AS dbl
  ON ei.engagement_path = dbl.engagement_path_norm
LEFT JOIN int_channel_status_latest AS ms
  ON lis.assessment_key = ms.assessment_key
