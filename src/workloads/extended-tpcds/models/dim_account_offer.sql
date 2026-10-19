WITH offer_types AS (
  SELECT DISTINCT
    COALESCE(NULLIF(account_offer, ''), 'Unknown') AS account_offer
  FROM int_account
), ranked AS (
  SELECT
    TRY_CAST(ROW_NUMBER() OVER (
      ORDER BY CASE WHEN account_offer = 'Unknown' THEN 0 ELSE 1 END NULLS FIRST, account_offer NULLS FIRST
    ) AS INT) AS account_offer_key,
    account_offer
  FROM offer_types
), subscription_offer_type_buckets AS (
  SELECT
    account_offer_key,
    account_offer,
    CASE
      WHEN account_offer IN (
        'Member',
        'Premium',
        'Family',
        'Business',
        'Annual',
        'Monthly',
        'Gift',
        'Student',
        'Corporate',
        'Partner',
        'Employee'
      )
      THEN 'Paid'
      WHEN account_offer IN ('Guest', 'Trial', 'Coupon', 'Coupon')
      THEN 'Free'
      ELSE 'Other'
    END AS account_offer_bucket
  FROM ranked
)
SELECT
  account_offer_key,
  account_offer,
  account_offer_bucket
FROM subscription_offer_type_buckets
