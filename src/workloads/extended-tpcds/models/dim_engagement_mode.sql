WITH statuses_deduped AS (
  SELECT DISTINCT
    activity_mode
  FROM (
    SELECT
      'UNKNOWN' AS activity_mode
    UNION ALL
    SELECT
      UPPER(activity_mode) AS activity_mode
    FROM stg_engagement_activity_history
    WHERE
      NOT activity_mode IS NULL
      AND UPPER(activity_mode) <> 'UNKNOWN'
      AND UPPER(activity_mode) IN ('OBSERVED', 'REPORTED', 'UNKNOWN')
  ) AS combined
), ranked AS (
  SELECT
    TRY_CAST(ROW_NUMBER() OVER (
      ORDER BY CASE WHEN activity_mode = 'UNKNOWN' THEN 0 ELSE 1 END NULLS FIRST, activity_mode NULLS FIRST
    ) AS INT) AS mode_key,
    activity_mode AS mode_name
  FROM statuses_deduped
)
SELECT
  mode_key,
  mode_name
FROM ranked
