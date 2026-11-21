WITH unique_pk AS (
  SELECT
    account_id,
    offer_type,
    usage_type,
    ROW_NUMBER() OVER (PARTITION BY account_id ORDER BY effective_start DESC, ingestion_time DESC) AS rn
  FROM stg_account_profile
), cust AS (
  SELECT
    account_id,
    offer_type,
    CASE WHEN usage_type IN ('Blank', 'None') THEN 'Unknown' ELSE usage_type END AS usage_type,
    1 AS src_priority
  FROM unique_pk
  WHERE
    rn = 1
), arm AS (
  SELECT DISTINCT
    account_id,
    TRY_CAST(NULL AS TEXT) AS offer_type,
    TRY_CAST(NULL AS TEXT) AS usage_type,
    2 AS src_priority
  FROM stg_entity_registry
  WHERE
    NOT account_id IS NULL
), default_row AS (
  SELECT
    '00000000-0000-0000-0000-000000000000' AS account_id,
    TRY_CAST(NULL AS TEXT) AS offer_type,
    TRY_CAST(NULL AS TEXT) AS usage_type,
    3 AS src_priority
), unioned AS (
  SELECT
    account_id,
    offer_type,
    usage_type,
    src_priority
  FROM cust
  UNION ALL
  SELECT
    account_id,
    offer_type,
    usage_type,
    src_priority
  FROM arm
  UNION ALL
  SELECT
    account_id,
    offer_type,
    usage_type,
    src_priority
  FROM default_row
), picked AS (
  SELECT
    account_id,
    offer_type,
    usage_type,
    ROW_NUMBER() OVER (PARTITION BY account_id ORDER BY src_priority NULLS FIRST) AS rn
  FROM unioned
)
SELECT
  account_id,
  COALESCE(NULLIF(offer_type, ''), 'Unknown') AS account_offer,
  COALESCE(NULLIF(usage_type, ''), 'Unknown') AS account_usage
FROM picked
WHERE
  rn = 1
