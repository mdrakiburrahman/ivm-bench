-- catalog-override: RisingWave needs a materialized per-symbol boundary before global rankings to avoid snapshot-backfill fanout.
WITH price_changes AS (
    SELECT
        dm_s_symb,
        dm_date,
        dm_close,
        dm_high,
        dm_low,
        dm_vol,
        LAG(dm_close) OVER (PARTITION BY dm_s_symb ORDER BY dm_date, dm_close, dm_vol) AS prev_close,
        LEAD(dm_close) OVER (PARTITION BY dm_s_symb ORDER BY dm_date, dm_close, dm_vol) AS next_close,
        dm_high - dm_low AS intraday_range
    FROM {{ ref('daily_market') }}
),

daily_returns AS (
    SELECT
        dm_s_symb,
        dm_date,
        dm_close,
        dm_high,
        dm_low,
        dm_vol,
        prev_close,
        next_close,
        intraday_range,
        CASE WHEN prev_close > 0
             THEN (dm_close - prev_close) / prev_close
             ELSE NULL
        END AS daily_return
    FROM price_changes
),

symbol_volatility AS (
    SELECT
        dm_s_symb,
        COUNT(*) AS trading_days,
        AVG(daily_return) AS avg_daily_return,
        STDDEV_SAMP(daily_return) AS return_volatility,
        AVG(CAST(intraday_range AS DOUBLE)) AS avg_intraday_range,
        MAX(intraday_range) AS max_intraday_range,
        SUM(CAST(dm_vol AS BIGINT)) AS total_volume,
        AVG(CAST(dm_vol AS DOUBLE)) AS avg_volume,
        STDDEV_SAMP(CAST(dm_vol AS DOUBLE)) AS volume_volatility,
        COUNT(DISTINCT dm_date) AS unique_trading_dates
    FROM daily_returns
    GROUP BY dm_s_symb
    HAVING COUNT(daily_return) >= 3
)

SELECT * FROM symbol_volatility
