SELECT DISTINCT
  imd.assessment_key,
  FIRST_VALUE(imd.store_eligibility IGNORE NULLS) OVER (
    PARTITION BY imd.assessment_key
    ORDER BY imd.ingestion_time DESC, imd.effective_start DESC, imd.store_eligibility DESC NULLS LAST, imd.catalog_eligibility DESC NULLS LAST, imd.web_eligibility DESC NULLS LAST
    ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
  ) AS store_eligibility,
  FIRST_VALUE(imd.catalog_eligibility IGNORE NULLS) OVER (
    PARTITION BY imd.assessment_key
    ORDER BY imd.ingestion_time DESC, imd.effective_start DESC, imd.store_eligibility DESC NULLS LAST, imd.catalog_eligibility DESC NULLS LAST, imd.web_eligibility DESC NULLS LAST
    ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
  ) AS catalog_eligibility,
  FIRST_VALUE(imd.web_eligibility IGNORE NULLS) OVER (
    PARTITION BY imd.assessment_key
    ORDER BY imd.ingestion_time DESC, imd.effective_start DESC, imd.store_eligibility DESC NULLS LAST, imd.catalog_eligibility DESC NULLS LAST, imd.web_eligibility DESC NULLS LAST
    ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
  ) AS web_eligibility
FROM stg_engagement_channel_history AS imd
