WITH distinct_usage_meters AS (
  SELECT DISTINCT
    price_catalog_id
  FROM stg_purchase_usage_events
  WHERE
    NOT price_catalog_id IS NULL
), unmapped AS (
  SELECT
    dum.price_catalog_id,
    'n/a' AS catalog_service_category_name_service_level_1,
    'n/a' AS catalog_service_name_service_level_2,
    'n/a' AS catalog_product_category_name_service_level_3,
    'n/a' AS catalog_product_name_service_level_4,
    'n/a' AS catalog_sku_name_service_level_5,
    'n/a' AS catalog_database_engine,
    'n/a' AS catalog_billable_resource_category,
    'n/a' AS catalog_subservice_name,
    'n/a' AS catalog_sql_tier_name,
    TRY_CAST(0 AS INT) AS catalog_size,
    'n/a' AS catalog_type,
    'n/a' AS catalog_license_type,
    TRY_CAST(0.0 AS REAL) AS catalog_price,
    'n/a' AS catalog_currency,
    'n/a' AS catalog_unit_of_measure,
    TRY_CAST(0 AS REAL) AS catalog_consumption_multiplier,
    'n/a' AS catalog_availability_region,
    TRY_CAST(0.0 AS REAL) AS catalog_average_revenue_per_unit,
    'n/a' AS catalog_description
  FROM distinct_usage_meters AS dum
  WHERE
    NOT EXISTS(
      SELECT
        1
      FROM stg_price_catalog AS bmd
      WHERE
        bmd.price_catalog_id = dum.price_catalog_id
    )
), combined AS (
  SELECT
    1 AS is_unmapped,
    price_catalog_id,
    catalog_service_category_name_service_level_1,
    catalog_service_name_service_level_2,
    catalog_product_category_name_service_level_3,
    catalog_product_name_service_level_4,
    catalog_sku_name_service_level_5,
    catalog_database_engine,
    catalog_billable_resource_category,
    catalog_subservice_name,
    catalog_sql_tier_name,
    catalog_size,
    catalog_type,
    catalog_license_type,
    catalog_price,
    catalog_currency,
    catalog_unit_of_measure,
    catalog_consumption_multiplier,
    catalog_availability_region,
    catalog_average_revenue_per_unit,
    catalog_description
  FROM unmapped
  UNION ALL
  SELECT
    0 AS is_unmapped,
    price_catalog_id,
    COALESCE(NULLIF(catalog_service_category_name_service_level_1, ''), 'n/a') AS catalog_service_category_name_service_level_1,
    COALESCE(NULLIF(catalog_service_name_service_level_2, ''), 'n/a') AS catalog_service_name_service_level_2,
    COALESCE(NULLIF(catalog_product_category_name_service_level_3, ''), 'n/a') AS catalog_product_category_name_service_level_3,
    COALESCE(NULLIF(catalog_product_name_service_level_4, ''), 'n/a') AS catalog_product_name_service_level_4,
    COALESCE(NULLIF(catalog_sku_name_service_level_5, ''), 'n/a') AS catalog_sku_name_service_level_5,
    COALESCE(NULLIF(catalog_database_engine, ''), 'n/a') AS catalog_database_engine,
    COALESCE(NULLIF(catalog_billable_resource_category, ''), 'n/a') AS catalog_billable_resource_category,
    COALESCE(NULLIF(catalog_subservice_name, ''), 'n/a') AS catalog_subservice_name,
    COALESCE(NULLIF(catalog_sql_tier_name, ''), 'n/a') AS catalog_sql_tier_name,
    COALESCE(catalog_size, 0) AS catalog_size,
    COALESCE(NULLIF(catalog_type, ''), 'n/a') AS catalog_type,
    COALESCE(NULLIF(catalog_license_type, ''), 'n/a') AS catalog_license_type,
    COALESCE(catalog_price, 0.0) AS catalog_price,
    COALESCE(NULLIF(catalog_currency, ''), 'n/a') AS catalog_currency,
    COALESCE(NULLIF(catalog_unit_of_measure, ''), 'n/a') AS catalog_unit_of_measure,
    COALESCE(catalog_consumption_multiplier, 0) AS catalog_consumption_multiplier,
    COALESCE(NULLIF(catalog_availability_region, ''), 'n/a') AS catalog_availability_region,
    COALESCE(catalog_average_revenue_per_unit, 0.0) AS catalog_average_revenue_per_unit,
    COALESCE(NULLIF(catalog_description, ''), 'n/a') AS catalog_description
  FROM stg_price_catalog
)
SELECT
  TRY_CAST(ROW_NUMBER() OVER (ORDER BY is_unmapped DESC, price_catalog_id NULLS FIRST) AS INT) AS price_catalog_key,
  price_catalog_id,
  catalog_service_category_name_service_level_1,
  catalog_service_name_service_level_2,
  catalog_product_category_name_service_level_3,
  catalog_product_name_service_level_4,
  catalog_sku_name_service_level_5,
  catalog_database_engine,
  catalog_billable_resource_category,
  catalog_subservice_name,
  catalog_sql_tier_name,
  catalog_size,
  catalog_type,
  catalog_license_type,
  catalog_price,
  catalog_currency,
  catalog_unit_of_measure,
  catalog_consumption_multiplier,
  catalog_availability_region,
  catalog_average_revenue_per_unit,
  catalog_description
FROM combined
