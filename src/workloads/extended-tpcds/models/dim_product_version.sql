WITH ranked AS (
  SELECT
    TRY_CAST(ROW_NUMBER() OVER (ORDER BY is_default DESC, product_version NULLS FIRST) AS INT) AS product_version_key,
    product_version
  FROM (
    SELECT
      TRUE AS is_default,
      'UNKNOWN' AS product_version
    UNION ALL
    SELECT
      FALSE AS is_default,
      product_version
    FROM (
      SELECT DISTINCT
        product_version
      FROM (
        SELECT
          product_version
        FROM stg_engagement_product_history
      )
      WHERE
        NOT product_version IS NULL
        AND product_version <> 'UNKNOWN'
        AND product_version IN ('V1', 'V2', 'V3')
    ) AS dedup
  ) AS combined
)
SELECT
  product_version_key,
  product_version
FROM ranked
