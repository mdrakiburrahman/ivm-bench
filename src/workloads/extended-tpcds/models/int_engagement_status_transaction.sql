WITH base_filtered_fact AS (
  SELECT
    isf.event_time,
    acd.account_id AS account_id,
    acd.entity_path AS engagement_path,
    LOWER(COALESCE(imlmv.customer_path, 'NOT APPLICABLE')) AS customer_path,
    UPPER(COALESCE(idlmv.product_tier, 'UNKNOWN')) AS product_tier,
    UPPER(COALESCE(idlmv.product_version, 'UNKNOWN')) AS product_version,
    UPPER(COALESCE(idlmv.activity_mode, 'UNKNOWN')) AS mode_name,
    UPPER(COALESCE(idlmv.relationship_type, 'PURCHASE')) AS engagement_type_name
  FROM stg_engagement_status_events AS isf
  INNER JOIN (
    SELECT
      registry_key,
      account_id,
      entity_path
    FROM stg_entity_registry
    WHERE
      rn = 1
  ) AS acd
    ON isf.engagement_registry_key = acd.registry_key
  LEFT JOIN int_engagement_customer_lookup AS imlmv
    ON acd.entity_path = imlmv.engagement_path
  LEFT JOIN int_engagement_dimension_lookup AS idlmv
    ON acd.entity_path = idlmv.engagement_path
)
SELECT
  bff.event_time,
  COALESCE(customer.household_key, 1) AS household_key,
  COALESCE(sotd.account_offer_key, 1) AS account_offer_key,
  COALESCE(swtd.account_usage_key, 1) AS account_usage_key,
  COALESCE(infra.platform_key, 1) AS platform_key,
  COALESCE(os.preference_key, 1) AS preference_key,
  COALESCE(product_tier.product_tier_key, 1) AS product_tier_key,
  COALESCE(version.product_version_key, 1) AS product_version_key,
  COALESCE(type.engagement_type_key, 1) AS engagement_type_key,
  COALESCE(mode.mode_key, 1) AS mode_key,
  bff.engagement_path,
  bff.customer_path,
  COALESCE(reported_units.effective_units, 0.0) AS effective_units
FROM base_filtered_fact AS bff
LEFT JOIN int_customer_profile AS mhmv
  ON bff.customer_path = mhmv.customer_path
LEFT JOIN int_household_best_match AS tilmv
  ON bff.account_id = tilmv.account_id
LEFT JOIN int_account AS smv
  ON bff.account_id = smv.account_id
LEFT JOIN dim_customer_platform AS infra
  ON infra.platform_name = COALESCE(mhmv.platform_name, 'N/A')
LEFT JOIN dim_customer_preference AS os
  ON COALESCE(mhmv.preference_type, 'unknown') = os.preference_type
  AND COALESCE(mhmv.preference_sku, 'Unknown') = os.preference_sku
LEFT JOIN dim_product_tier AS product_tier
  ON bff.product_tier = product_tier.product_tier
LEFT JOIN dim_product_version AS version
  ON bff.product_version = version.product_version
LEFT JOIN dim_engagement_mode AS mode
  ON bff.mode_name = mode.mode_name
LEFT JOIN dim_engagement_type AS type
  ON bff.engagement_type_name = type.engagement_type_name
LEFT JOIN dim_household AS customer
  ON COALESCE(tilmv.household_id, -2147483648) = customer.household_id
LEFT JOIN dim_account_offer AS sotd
  ON COALESCE(smv.account_offer, 'Unknown') = sotd.account_offer
LEFT JOIN dim_account_usage AS swtd
  ON COALESCE(smv.account_usage, 'Unknown') = swtd.account_usage
LEFT JOIN int_engagement_effective_units AS reported_units
  ON bff.engagement_path = reported_units.engagement_path
