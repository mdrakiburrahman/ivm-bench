SELECT
  TRY_CAST(0 AS INT) AS disabled,
  TRY_CAST(1 AS INT) AS schemaversion,
  TRY_CAST(TRY_CAST('__REPORT_DATE__ 00:00:00' AS TIMESTAMPTZ) AS TIMESTAMPTZ) AS processeddatetime,
  c.report_as_of_date AS date,
  'Retail' AS product,
  'Engagement Units' AS feature,
  c.household_id AS tpid,
  c.household_type AS customertype,
  'Eligible Units' AS metricname,
  TRY_CAST(SUM(c.metric_daily) AS BIGINT) AS metricvalue_daily,
  TRY_CAST(SUM(c.metric_7d) AS BIGINT) AS metricvalue_rolling7day,
  TRY_CAST(SUM(c.metric_28d) AS BIGINT) AS metricvalue_rolling28day,
  'extended-tpcds' AS serviceid
FROM (
  SELECT
    report_as_of_date,
    household_id,
    household_type,
    ready_vunits AS metric_daily,
    TRY_CAST(0 AS BIGINT) AS metric_7d,
    TRY_CAST(0 AS BIGINT) AS metric_28d
  FROM (
    SELECT
      report_as_of_date,
      household_id,
      household_type,
      TRY_CAST(SUM(
        CASE
          WHEN store_eligibility = 'ready'
          OR catalog_eligibility = 'ready'
          OR web_eligibility = 'ready'
          THEN allocated_units
          ELSE 0.0
        END
      ) AS BIGINT) AS ready_vunits
    FROM int_hist_active_daily
    GROUP BY
      report_as_of_date,
      household_id,
      household_type
  ) AS g
  UNION ALL
  SELECT
    report_as_of_date,
    household_id,
    household_type,
    TRY_CAST(0 AS BIGINT) AS metric_daily,
    ready_vunits AS metric_7d,
    TRY_CAST(0 AS BIGINT) AS metric_28d
  FROM (
    SELECT
      report_as_of_date,
      household_id,
      household_type,
      TRY_CAST(SUM(
        CASE
          WHEN store_eligibility = 'ready'
          OR catalog_eligibility = 'ready'
          OR web_eligibility = 'ready'
          THEN allocated_units
          ELSE 0.0
        END
      ) AS BIGINT) AS ready_vunits
    FROM int_hist_active_7d
    GROUP BY
      report_as_of_date,
      household_id,
      household_type
  ) AS g
  UNION ALL
  SELECT
    report_as_of_date,
    household_id,
    household_type,
    TRY_CAST(0 AS BIGINT) AS metric_daily,
    TRY_CAST(0 AS BIGINT) AS metric_7d,
    ready_vunits AS metric_28d
  FROM (
    SELECT
      report_as_of_date,
      household_id,
      household_type,
      TRY_CAST(SUM(
        CASE
          WHEN store_eligibility = 'ready'
          OR catalog_eligibility = 'ready'
          OR web_eligibility = 'ready'
          THEN allocated_units
          ELSE 0.0
        END
      ) AS BIGINT) AS ready_vunits
    FROM int_hist_active_28d
    GROUP BY
      report_as_of_date,
      household_id,
      household_type
  ) AS g
) AS c
GROUP BY
  c.report_as_of_date,
  c.household_id,
  c.household_type
