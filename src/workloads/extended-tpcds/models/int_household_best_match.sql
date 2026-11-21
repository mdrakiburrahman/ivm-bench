WITH exact_best AS (
  SELECT
    account_id,
    loyalty_household_id,
    household_id
  FROM (
    SELECT
      account_id,
      loyalty_household_id,
      household_id,
      ROW_NUMBER() OVER (
        PARTITION BY account_id, loyalty_household_id
        ORDER BY effective_start DESC, ingestion_time DESC
      ) AS rn
    FROM stg_account_profile
  ) AS t
  WHERE
    rn = 1
), sub_best AS (
  SELECT
    account_id,
    household_id
  FROM (
    SELECT
      account_id,
      household_id,
      ROW_NUMBER() OVER (PARTITION BY account_id ORDER BY effective_start DESC, ingestion_time DESC) AS rn
    FROM stg_account_profile
  ) AS t
  WHERE
    rn = 1
), tenant_best AS (
  SELECT
    loyalty_household_id,
    household_id
  FROM (
    SELECT
      loyalty_household_id,
      household_id,
      ROW_NUMBER() OVER (
        PARTITION BY loyalty_household_id
        ORDER BY effective_start DESC, ingestion_time DESC
      ) AS rn
    FROM stg_account_profile
  ) AS t
  WHERE
    rn = 1
), arms AS (
  SELECT DISTINCT
    account_id,
    loyalty_household_id
  FROM stg_account_household
), joined AS (
  SELECT
    a.account_id,
    a.loyalty_household_id,
    COALESCE(e.household_id, s.household_id, t.household_id, -2147483648) AS household_id,
    ROW_NUMBER() OVER (
      PARTITION BY a.account_id
      ORDER BY CASE
        WHEN NOT e.household_id IS NULL
        THEN 1
        WHEN NOT s.household_id IS NULL
        THEN 2
        WHEN NOT t.household_id IS NULL
        THEN 3
        ELSE 4
      END NULLS FIRST, a.loyalty_household_id ASC NULLS FIRST
    ) AS rn
  FROM arms AS a
  LEFT JOIN exact_best AS e
    ON a.account_id = e.account_id AND a.loyalty_household_id = e.loyalty_household_id
  LEFT JOIN sub_best AS s
    ON a.account_id = s.account_id
  LEFT JOIN tenant_best AS t
    ON a.loyalty_household_id = t.loyalty_household_id
), deduped AS (
  SELECT
    account_id,
    loyalty_household_id,
    household_id
  FROM (
    SELECT
      account_id,
      loyalty_household_id,
      household_id,
      ROW_NUMBER() OVER (
        PARTITION BY account_id
        ORDER BY loyalty_household_id ASC NULLS FIRST, household_id ASC NULLS FIRST
      ) AS rn
    FROM joined
    WHERE
      rn = 1
  ) AS t
  WHERE
    rn = 1
)
SELECT
  account_id,
  loyalty_household_id,
  household_id
FROM deduped
