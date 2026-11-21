SELECT
  account_id,
  loyalty_household_id,
  household_id,
  effective_start,
  ingestion_time,
  offer_type,
  usage_type,
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
  COALESCE(
    NULLIF(household_name_translated, ''),
    NULLIF(household_name_raw, ''),
    NULLIF(household_name_secondary, ''),
    NULLIF(household_name, '')
  ) AS household_display_name
FROM ext_account_profile
WHERE
  is_current = TRUE
