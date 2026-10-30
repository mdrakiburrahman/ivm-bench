SELECT
  customer_path,
  COALESCE(store_active, FALSE) AS store_active,
  COALESCE(catalog_active, FALSE) AS catalog_active,
  COALESCE(web_active, FALSE) AS web_active
FROM stg_customer_profile_history
