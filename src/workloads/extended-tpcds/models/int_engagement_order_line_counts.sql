SELECT
  engagement_path_norm,
  COUNT(DISTINCT entity_path_norm) AS order_line_count
FROM (
  SELECT
    entity_path_norm,
    CASE
      WHEN STRPOS(entity_path_norm, '/order-lines/') > 1
      THEN SUBSTRING(entity_path_norm, 1, STRPOS(entity_path_norm, '/order-lines/') - 1)
      ELSE NULL
    END AS engagement_path_norm,
    CASE
      WHEN STRPOS(entity_path_norm, '/order-lines/') > 0
      THEN SUBSTRING(
        entity_path_norm,
        STRPOS(entity_path_norm, '/order-lines/') + 13,
        LENGTH(entity_path_norm)
      )
      ELSE NULL
    END AS database_name
  FROM (
    SELECT
      ac.entity_path AS entity_path_norm
    FROM stg_entity_registry AS ac
    WHERE
      ac.rn = 1
      AND ac.entity_type = 'order_lines'
      AND STRPOS(ac.entity_path, '/order-lines/') > 1
  ) AS ac_norm
) AS safe_split
WHERE
  NOT database_name IS NULL
  AND NOT database_name IN ('cancelled', 'test', 'sample', 'internal')
  AND NOT engagement_path_norm IS NULL
GROUP BY
  engagement_path_norm
