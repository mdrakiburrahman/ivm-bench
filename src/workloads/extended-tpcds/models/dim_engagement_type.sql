WITH ranked AS (
  SELECT
    TRY_CAST(ROW_NUMBER() OVER (ORDER BY relationship_type NULLS FIRST) AS INT) AS engagement_type_key,
    relationship_type AS engagement_type_name
  FROM (
    SELECT DISTINCT
      UPPER(COALESCE(relationship_type, 'PURCHASE')) AS relationship_type
    FROM stg_engagement_capacity_history
  )
)
SELECT
  engagement_type_key,
  engagement_type_name
FROM ranked
