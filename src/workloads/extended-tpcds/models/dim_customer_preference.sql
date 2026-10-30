WITH normalized AS (
  SELECT
    TRUE AS is_default,
    'unknown' AS preference_type,
    'Unknown' AS preference_sku,
    LOWER(REGEXP_REPLACE('unknown' || '_' || 'Unknown', '[^A-Za-z0-9_]+', '_', 'g')) AS preference_name
  UNION ALL
  SELECT
    FALSE AS is_default,
    clean_os.preference_type,
    clean_os.preference_sku,
    LOWER(
      REGEXP_REPLACE(
        COALESCE(clean_os.preference_type, '') || '_' || COALESCE(clean_os.preference_sku, ''),
        '[^A-Za-z0-9_]+',
        '_',
        'g'
      )
    ) AS preference_name
  FROM (
    SELECT
      COALESCE(NULLIF(TRIM(preference_type), ''), 'unknown') AS preference_type,
      COALESCE(NULLIF(TRIM(preference_sku), ''), 'Unknown') AS preference_sku
    FROM stg_customer_preference_history
  ) AS clean_os
), deduped AS (
  SELECT
    preference_name,
    preference_type AS preference_type,
    preference_sku AS preference_sku,
    is_default,
    ROW_NUMBER() OVER (
      PARTITION BY preference_name
      ORDER BY is_default DESC, preference_type NULLS FIRST, preference_sku NULLS FIRST
    ) AS rn
  FROM normalized
), final AS (
  SELECT
    TRY_CAST(ROW_NUMBER() OVER (
      ORDER BY is_default DESC, preference_type NULLS FIRST, preference_sku NULLS FIRST
    ) AS INT) AS preference_key,
    preference_name,
    preference_type,
    preference_sku
  FROM deduped
  WHERE
    rn = 1
)
SELECT
  preference_key,
  preference_name,
  preference_type,
  preference_sku
FROM final
