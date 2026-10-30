SELECT
  ds.report_as_of_date,
  b.engagement_registry_key,
  b.observation_time,
  b.ingestion_time,
  b.assessment_key
FROM int_hist_report_window_dates AS ds
INNER JOIN int_hist_eligible_status AS b
  ON b.effective_date = ds.effective_date
WHERE
  ds.window_lag_days < 1
