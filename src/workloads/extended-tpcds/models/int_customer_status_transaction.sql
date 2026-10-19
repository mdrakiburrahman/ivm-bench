WITH base_filtered_fact AS (
  SELECT
    msf.event_time,
    acd.account_id AS account_id,
    acd.entity_path AS customer_path,
    COALESCE(hmv.platform_name, 'N/A') AS platform_name,
    COALESCE(hmv.preference_type, 'unknown') AS preference_type,
    COALESCE(hmv.preference_sku, 'Unknown') AS preference_sku,
    COALESCE(hmv.parent_capacity, 0) AS parent_capacity,
    COALESCE(dmv.store_active, FALSE) AS store_active,
    COALESCE(dmv.catalog_active, FALSE) AS catalog_active,
    COALESCE(dmv.web_active, FALSE) AS web_active
  FROM stg_customer_status_events AS msf
  INNER JOIN (
    SELECT
      registry_key,
      account_id,
      entity_path
    FROM stg_entity_registry
    WHERE
      rn = 1
  ) AS acd
    ON msf.customer_registry_key = acd.registry_key
  LEFT JOIN int_customer_profile AS hmv
    ON acd.entity_path = hmv.customer_path
  LEFT JOIN int_customer_channel_flags AS dmv
    ON acd.entity_path = dmv.customer_path
)
SELECT
  bff.event_time,
  bff.customer_path,
  COALESCE(customer.household_key, 1) AS household_key,
  COALESCE(sotd.account_offer_key, 1) AS account_offer_key,
  COALESCE(swtd.account_usage_key, 1) AS account_usage_key,
  COALESCE(infra.platform_key, 1) AS platform_key,
  COALESCE(os.preference_key, 1) AS preference_key,
  bff.store_active,
  bff.catalog_active,
  bff.web_active,
  COALESCE(bff.parent_capacity, 0) AS parent_capacity
FROM base_filtered_fact AS bff
LEFT JOIN dim_customer_platform AS infra
  ON bff.platform_name = infra.platform_name
LEFT JOIN dim_customer_preference AS os
  ON bff.preference_type = os.preference_type
  AND bff.preference_sku = os.preference_sku
LEFT JOIN int_household_best_match AS tilmv
  ON bff.account_id = tilmv.account_id
LEFT JOIN int_account AS smv
  ON bff.account_id = smv.account_id
LEFT JOIN dim_household AS customer
  ON COALESCE(tilmv.household_id, -2147483648) = customer.household_id
LEFT JOIN dim_account_offer AS sotd
  ON COALESCE(smv.account_offer, 'Unknown') = sotd.account_offer
LEFT JOIN dim_account_usage AS swtd
  ON COALESCE(smv.account_usage, 'Unknown') = swtd.account_usage
