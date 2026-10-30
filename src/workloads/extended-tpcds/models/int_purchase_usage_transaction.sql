WITH base_filtered_fact AS (
  SELECT
    busf.event_time,
    acd.account_id,
    acd.entity_path AS engagement_path,
    LOWER(COALESCE(iml.customer_path, 'NOT APPLICABLE')) AS customer_path,
    busf.price_catalog_id,
    busf.purchase_quantity
  FROM stg_purchase_usage_events AS busf
  INNER JOIN (
    SELECT
      registry_key,
      account_id,
      entity_path
    FROM stg_entity_registry
    WHERE
      rn = 1
  ) AS acd
    ON busf.registry_key = acd.registry_key
  LEFT JOIN int_engagement_customer_lookup AS iml
    ON acd.entity_path = iml.engagement_path
)
SELECT
  bff.event_time,
  bff.engagement_path,
  bff.price_catalog_id,
  COALESCE(customer.household_key, 1) AS household_key,
  COALESCE(sotd.account_offer_key, 1) AS account_offer_key,
  COALESCE(swtd.account_usage_key, 1) AS account_usage_key,
  COALESCE(infra.platform_key, 1) AS platform_key,
  COALESCE(os.preference_key, 1) AS preference_key,
  COALESCE(product_tier.product_tier_key, 1) AS product_tier_key,
  COALESCE(version.product_version_key, 1) AS product_version_key,
  COALESCE(type.engagement_type_key, 1) AS engagement_type_key,
  COALESCE(meter.price_catalog_key, 1) AS price_catalog_key,
  bff.purchase_quantity
FROM base_filtered_fact AS bff
LEFT JOIN int_customer_profile AS mhmv
  ON bff.customer_path = mhmv.customer_path
LEFT JOIN int_household_best_match AS tilmv
  ON bff.account_id = tilmv.account_id
LEFT JOIN int_account AS smv
  ON bff.account_id = smv.account_id
LEFT JOIN stg_engagement_capacity_history AS iid
  ON bff.engagement_path = iid.engagement_path
LEFT JOIN stg_engagement_product_history AS ivd
  ON bff.engagement_path = ivd.engagement_path
LEFT JOIN dim_customer_platform AS infra
  ON infra.platform_name = COALESCE(mhmv.platform_name, 'N/A')
LEFT JOIN dim_customer_preference AS os
  ON COALESCE(mhmv.preference_type, 'unknown') = os.preference_type
  AND COALESCE(mhmv.preference_sku, 'Unknown') = os.preference_sku
LEFT JOIN dim_product_tier AS product_tier
  ON COALESCE(UPPER(ivd.product_tier), 'UNKNOWN') = product_tier.product_tier
LEFT JOIN dim_product_version AS version
  ON COALESCE(UPPER(ivd.product_version), 'UNKNOWN') = version.product_version
LEFT JOIN dim_engagement_type AS type
  ON COALESCE(UPPER(iid.relationship_type), 'PURCHASE') = type.engagement_type_name
LEFT JOIN dim_household AS customer
  ON COALESCE(tilmv.household_id, -2147483648) = customer.household_id
LEFT JOIN dim_account_offer AS sotd
  ON COALESCE(smv.account_offer, 'Unknown') = sotd.account_offer
LEFT JOIN dim_account_usage AS swtd
  ON COALESCE(smv.account_usage, 'Unknown') = swtd.account_usage
LEFT JOIN dim_price_catalog AS meter
  ON bff.price_catalog_id = meter.price_catalog_id
