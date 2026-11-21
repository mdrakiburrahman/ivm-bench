WITH scored AS (
  SELECT
    household_id,
    household_display_name AS household_name,
    household_type,
    region_group,
    sales_area,
    country_name,
    household_category,
    household_segment,
    household_subcategory,
    household_vertical,
    membership_number,
    membership_name,
    is_priority,
    offer_type,
    effective_start,
    ingestion_time,
    COUNT(*) OVER (PARTITION BY household_id, household_display_name) AS cnt_name,
    COUNT(*) OVER (
      PARTITION BY household_id, household_display_name, household_type, region_group, sales_area, country_name, household_category, household_segment, household_subcategory, household_vertical, is_priority, membership_number, membership_name, offer_type
    ) AS cnt_combo
  FROM stg_account_profile
), customers AS (
  SELECT
    household_id,
    household_name,
    household_type,
    region_group,
    sales_area,
    country_name,
    household_category,
    household_segment,
    household_subcategory,
    household_vertical,
    membership_number,
    membership_name,
    is_priority,
    offer_type
  FROM (
    SELECT
      household_id,
      household_name,
      household_type,
      region_group,
      sales_area,
      country_name,
      household_category,
      household_segment,
      household_subcategory,
      household_vertical,
      membership_number,
      membership_name,
      is_priority,
      offer_type,
      ROW_NUMBER() OVER (
        PARTITION BY household_id
        ORDER BY cnt_name DESC, cnt_combo DESC, effective_start DESC, ingestion_time DESC, COALESCE(NULLIF(household_name, ''), 'Unknown') ASC NULLS FIRST, COALESCE(NULLIF(household_type, ''), 'Unknown') ASC NULLS FIRST, COALESCE(NULLIF(region_group, ''), 'Unknown') ASC NULLS FIRST, COALESCE(NULLIF(sales_area, ''), 'Unknown') ASC NULLS FIRST, COALESCE(NULLIF(country_name, ''), 'Unknown') ASC NULLS FIRST, COALESCE(NULLIF(household_category, ''), 'Unknown') ASC NULLS FIRST, COALESCE(NULLIF(household_segment, ''), 'Unknown') ASC NULLS FIRST, COALESCE(NULLIF(household_subcategory, ''), 'Unknown') ASC NULLS FIRST, COALESCE(NULLIF(household_vertical, ''), 'Unknown') ASC NULLS FIRST, COALESCE(NULLIF(membership_number, ''), 'Unknown') ASC NULLS FIRST, COALESCE(NULLIF(membership_name, ''), 'Unknown') ASC NULLS FIRST, COALESCE(NULLIF(offer_type, ''), 'Unknown') ASC NULLS FIRST
      ) AS rn
    FROM scored
  ) AS s
  WHERE
    rn = 1
), combined AS (
  SELECT
    household_id,
    household_name,
    household_type,
    region_group,
    sales_area,
    country_name,
    household_category,
    household_segment,
    household_subcategory,
    household_vertical,
    membership_number,
    membership_name,
    is_priority,
    offer_type,
    1 AS src_priority
  FROM customers
  UNION ALL
  SELECT
    TRY_CAST(-2147483648 AS INT) AS household_id,
    'Unknown' AS household_name,
    'Unknown' AS household_type,
    'Unknown' AS region_group,
    'Unknown' AS sales_area,
    'Unknown' AS country_name,
    'Unknown' AS household_category,
    'Unknown' AS household_segment,
    'Unknown' AS household_subcategory,
    'Unknown' AS household_vertical,
    'Unknown' AS membership_number,
    'Unknown' AS membership_name,
    FALSE AS is_priority,
    'Unknown' AS offer_type,
    2 AS src_priority
), deduped AS (
  SELECT
    household_id,
    household_name,
    household_type,
    region_group,
    sales_area,
    country_name,
    household_category,
    household_segment,
    household_subcategory,
    household_vertical,
    membership_number,
    membership_name,
    is_priority,
    offer_type,
    ROW_NUMBER() OVER (PARTITION BY household_id ORDER BY src_priority NULLS FIRST) AS rn
  FROM combined
)
SELECT
  TRY_CAST(ROW_NUMBER() OVER (ORDER BY household_id NULLS FIRST) AS INT) AS household_key,
  household_id,
  COALESCE(
    CASE
      WHEN household_id = -1
      THEN 'All unmanaged households'
      ELSE COALESCE(NULLIF(household_name, ''), 'Unknown')
    END,
    ''
  ) || ' (' || COALESCE(TRY_CAST(household_id AS TEXT), '') || ')' AS household_name,
  COALESCE(NULLIF(household_type, ''), 'Unknown') AS household_type,
  COALESCE(NULLIF(region_group, ''), 'Unknown') AS region_group,
  COALESCE(NULLIF(sales_area, ''), 'Unknown') AS sales_area,
  COALESCE(NULLIF(country_name, ''), 'Unknown') AS country_name,
  COALESCE(NULLIF(household_category, ''), 'Unknown') AS household_category,
  COALESCE(NULLIF(household_segment, ''), 'Unknown') AS household_segment,
  COALESCE(NULLIF(household_subcategory, ''), 'Unknown') AS household_subcategory,
  COALESCE(NULLIF(household_vertical, ''), 'Unknown') AS household_vertical,
  NULLIF(membership_number, '') AS membership_number,
  NULLIF(membership_name, '') AS membership_name,
  is_priority,
  CASE
    WHEN offer_type IS NULL OR offer_type IN ('', 'Blank')
    THEN 'Unknown'
    ELSE offer_type
  END AS offer_type
FROM deduped
WHERE
  rn = 1
