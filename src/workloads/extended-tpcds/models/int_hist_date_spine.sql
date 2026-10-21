SELECT
  c.date AS report_as_of_date
FROM dim_calendar AS c
WHERE
  c.date BETWEEN CAST(TRY_CAST('__REPORT_DATE__' AS DATE) + INTERVAL (-__history_months__) MONTH AS DATE) AND TRY_CAST('__REPORT_DATE__' AS DATE)
