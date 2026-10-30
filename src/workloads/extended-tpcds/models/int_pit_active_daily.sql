SELECT
  lis.engagement_registry_key,
  lis.assessment_key,
  COALESCE(dc.household_key, 1) AS household_key,
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
    x.engagement_registry_key,
    x.assessment_key
  FROM (
    SELECT
      csr.engagement_registry_key,
      csr.assessment_key,
      ROW_NUMBER() OVER (
        PARTITION BY csr.engagement_registry_key
        ORDER BY COALESCE(csr.observation_time, csr.ingestion_time) DESC, csr.assessment_key DESC
      ) AS rn
    FROM (
      SELECT
        isf.engagement_registry_key,
        isf.observation_time,
        isf.ingestion_time,
        isf.assessment_key
      FROM stg_engagement_status_events AS isf
      INNER JOIN int_engagement_eligible_active AS ei
        ON isf.engagement_registry_key = ei.engagement_registry_key
      WHERE
        ei.activity_mode = 'observed'
        AND isf.event_date >= TRY_CAST('__REPORT_DATE__' AS DATE) + INTERVAL (-0) DAY
        AND isf.event_date <= TRY_CAST('__REPORT_DATE__' AS DATE)
      UNION ALL
      SELECT
        isf.engagement_registry_key,
        isf.observation_time,
        isf.ingestion_time,
        isf.assessment_key
      FROM stg_engagement_status_events AS isf
      INNER JOIN int_engagement_eligible_active AS ei
        ON isf.engagement_registry_key = ei.engagement_registry_key
      WHERE
        ei.activity_mode = 'reported'
        AND CAST(isf.observation_time AS DATE) >= TRY_CAST('__REPORT_DATE__' AS DATE) + INTERVAL (-0) DAY
        AND CAST(isf.observation_time AS DATE) <= TRY_CAST('__REPORT_DATE__' AS DATE)
    ) AS csr
  ) AS x
  WHERE
    x.rn = 1
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
LEFT JOIN int_channel_status_latest AS ms
  ON lis.assessment_key = ms.assessment_key
