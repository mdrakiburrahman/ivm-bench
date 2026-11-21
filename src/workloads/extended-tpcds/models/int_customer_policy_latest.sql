SELECT
  customer_path_norm,
  policy_type
FROM (
  SELECT
    ecd.customer_path AS customer_path_norm,
    ecd.policy_type AS policy_type,
    ROW_NUMBER() OVER (
      PARTITION BY ecd.customer_path
      ORDER BY ecd.ingestion_time DESC, ecd.effective_start DESC, COALESCE(ecd.policy_type, '') DESC
    ) AS rn
  FROM stg_customer_policy_history AS ecd
  WHERE
    NOT ecd.customer_path IS NULL
) AS ecd
WHERE
  rn = 1
