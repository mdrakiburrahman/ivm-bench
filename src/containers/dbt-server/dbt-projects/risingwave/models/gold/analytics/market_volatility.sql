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
),

global_market AS (
    SELECT
        AVG(return_volatility) AS mkt_avg_volatility,
        STDDEV_SAMP(return_volatility) AS mkt_std_volatility,
        AVG(avg_daily_return) AS mkt_avg_return,
        SUM(total_volume) AS mkt_total_volume
    FROM symbol_volatility
),

scored AS (
    SELECT
        sv.dm_s_symb,
        sv.trading_days,
        ROUND(CAST(sv.avg_daily_return AS NUMERIC), 4) AS avg_daily_return,
        ROUND(CAST(sv.return_volatility AS NUMERIC), 4) AS return_volatility,
        ROUND(CAST(sv.avg_intraday_range AS NUMERIC), 4) AS avg_intraday_range,
        sv.max_intraday_range,
        sv.total_volume,
        ROUND(CAST(sv.avg_volume AS NUMERIC), 4) AS avg_volume,
        ROUND(CAST(sv.volume_volatility AS NUMERIC), 4) AS volume_volatility,
        sv.unique_trading_dates,
        ROUND(CAST((sv.return_volatility - gm.mkt_avg_volatility) / NULLIF(gm.mkt_std_volatility, 0) AS NUMERIC), 4) AS volatility_z_score,
        ROUND(CAST(sv.total_volume * 100.0 / NULLIF(gm.mkt_total_volume, 0) AS NUMERIC), 4) AS pct_market_volume,
        RANK() OVER (PARTITION BY 1 ORDER BY sv.return_volatility DESC, sv.dm_s_symb) AS rank_by_volatility,
        RANK() OVER (PARTITION BY 1 ORDER BY sv.total_volume DESC, sv.dm_s_symb) AS rank_by_volume
    FROM (SELECT *, 1 AS join_key FROM symbol_volatility) sv
    JOIN (SELECT *, 1 AS join_key FROM global_market) gm ON sv.join_key = gm.join_key
)

SELECT * FROM scored
