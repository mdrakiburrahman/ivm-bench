WITH infra AS (
  SELECT
    engagement_path,
    customer_path
  FROM (
    SELECT
      iid.engagement_path AS engagement_path,
      iid.customer_path AS customer_path,
      ROW_NUMBER() OVER (PARTITION BY iid.engagement_path ORDER BY iid.effective_start DESC) AS rn
    FROM stg_engagement_capacity_history AS iid
    WHERE
      iid.relationship_type = 'purchase'
  ) AS ranked
  WHERE
    rn = 1
), usage_ranked AS (
  SELECT
    engagement_path,
    customer_path,
    ROW_NUMBER() OVER (
      PARTITION BY engagement_path
      ORDER BY CASE WHEN customer_path <> LOWER('NOT APPLICABLE') THEN 0 ELSE 1 END NULLS FIRST, receipt_time DESC
    ) AS rn
  FROM int_engagement_receipt_latest
), final AS (
  SELECT
    COALESCE(i.engagement_path, u.engagement_path) AS engagement_path,
    COALESCE(i.customer_path, u.customer_path) AS customer_path
  FROM usage_ranked AS u
  FULL OUTER JOIN infra AS i
    ON u.engagement_path = i.engagement_path
  WHERE
    (
      u.rn = 1 OR u.rn IS NULL
    )
)
SELECT
  engagement_path,
  customer_path
FROM final
