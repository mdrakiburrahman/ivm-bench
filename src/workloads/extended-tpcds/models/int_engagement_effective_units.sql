WITH engagement_reported_units_silver AS (
  SELECT
    silver.engagement_path,
    silver.customer_path,
    silver.engagement_reported_units,
    NULL AS customer_reported_capacity,
    NULL AS receipt_reported_units
  FROM int_engagement_observation_latest AS silver
), receipt_reported_units_latest AS (
  SELECT
    receipt_time,
    engagement_path,
    customer_path,
    receipt_reported_units
  FROM int_engagement_receipt_latest
), engagement_customer_reported_units AS (
  SELECT
    engagement_path,
    customer_path,
    engagement_reported_units,
    customer_reported_capacity,
    NULL AS receipt_reported_units
  FROM int_engagement_primary_units
  UNION
  SELECT
    engagement_path,
    customer_path,
    engagement_reported_units,
    customer_reported_capacity,
    NULL AS receipt_reported_units
  FROM engagement_reported_units_silver
  UNION
  SELECT
    engagement_path,
    customer_path,
    NULL AS engagement_reported_units,
    NULL AS customer_reported_capacity,
    receipt_reported_units
  FROM receipt_reported_units_latest
), dedup AS (
  SELECT
    engagement_path,
    customer_path,
    engagement_reported_units,
    customer_reported_capacity,
    receipt_reported_units,
    ROW_NUMBER() OVER (
      PARTITION BY engagement_path, customer_path
      ORDER BY COALESCE(customer_reported_capacity, engagement_reported_units, receipt_reported_units, 0) DESC, customer_reported_capacity DESC NULLS LAST, engagement_reported_units DESC NULLS LAST, receipt_reported_units DESC NULLS LAST
    ) AS rn
  FROM engagement_customer_reported_units
), dedup_top AS (
  SELECT
    engagement_path,
    customer_path,
    engagement_reported_units,
    customer_reported_capacity,
    receipt_reported_units,
    CASE
      WHEN customer_path IS NULL
      THEN TRY_CAST(NULL AS BIGINT)
      ELSE SUM(CASE WHEN customer_path = LOWER('NOT APPLICABLE') THEN 0 ELSE 1 END) OVER (PARTITION BY customer_path)
    END AS engagements_per_customer
  FROM dedup
  WHERE
    rn = 1
), engagement_deduped AS (
  SELECT
    engagement_path,
    customer_path,
    engagement_reported_units,
    customer_reported_capacity,
    receipt_reported_units,
    engagements_per_customer,
    ROW_NUMBER() OVER (
      PARTITION BY engagement_path
      ORDER BY LENGTH(COALESCE(customer_path, '')) DESC, COALESCE(customer_reported_capacity, engagement_reported_units, receipt_reported_units, 0) DESC, customer_path ASC NULLS FIRST
    ) AS engagement_rn
  FROM dedup_top
), final AS (
  SELECT
    engagement_path,
    customer_path,
    engagement_reported_units,
    customer_reported_capacity,
    receipt_reported_units,
    engagements_per_customer,
    CASE
      WHEN NOT customer_reported_capacity IS NULL
      THEN TRY_CAST(customer_reported_capacity AS DOUBLE) / NULLIF(TRY_CAST(COALESCE(engagements_per_customer, 1) AS DOUBLE), 0)
      WHEN NOT engagement_reported_units IS NULL
      THEN TRY_CAST(engagement_reported_units AS DOUBLE)
      WHEN NOT receipt_reported_units IS NULL
      THEN TRY_CAST(receipt_reported_units AS DOUBLE)
      ELSE 0.0
    END AS effective_units,
    CASE
      WHEN NOT customer_reported_capacity IS NULL
      THEN 'customer_capacity_divided'
      WHEN NOT engagement_reported_units IS NULL
      THEN 'engagement_reported'
      WHEN NOT receipt_reported_units IS NULL
      THEN 'receipt_reported'
      ELSE 'no_report'
    END AS effective_units_source
  FROM engagement_deduped
  WHERE
    engagement_rn = 1
)
SELECT
  engagement_path,
  customer_path,
  engagement_reported_units,
  customer_reported_capacity,
  receipt_reported_units,
  engagements_per_customer,
  effective_units,
  effective_units_source
FROM final
