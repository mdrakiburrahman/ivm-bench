-- TODO: OpenIVM change needed from @ila to incrementalize this query and store as materialized_view
WITH price_changes AS (
    SELECT
        dm_s_symb,
        dm_date,
        dm_close,
        dm_high,
        dm_low,
        dm_vol,
        LAG(dm_close) OVER (PARTITION BY dm_s_symb ORDER BY dm_date) AS prev_close,
        LEAD(dm_close) OVER (PARTITION BY dm_s_symb ORDER BY dm_date) AS next_close,
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
        STDDEV(daily_return) AS return_volatility,
        AVG(CAST(intraday_range AS DOUBLE)) AS avg_intraday_range,
        MAX(intraday_range) AS max_intraday_range,
        SUM(CAST(dm_vol AS BIGINT)) AS total_volume,
        AVG(CAST(dm_vol AS DOUBLE)) AS avg_volume,
        STDDEV(CAST(dm_vol AS DOUBLE)) AS volume_volatility,
        COUNT(DISTINCT dm_date) AS unique_trading_dates
    FROM daily_returns
    GROUP BY dm_s_symb
    HAVING COUNT(daily_return) >= 3
),

global_market AS (
    SELECT
        AVG(return_volatility) AS mkt_avg_volatility,
        STDDEV(return_volatility) AS mkt_std_volatility,
        AVG(avg_daily_return) AS mkt_avg_return,
        SUM(total_volume) AS mkt_total_volume
    FROM symbol_volatility
),

scored AS (
    SELECT
        sv.dm_s_symb,
        sv.trading_days,
        sv.avg_daily_return,
        sv.return_volatility,
        sv.avg_intraday_range,
        sv.max_intraday_range,
        sv.total_volume,
        sv.avg_volume,
        sv.volume_volatility,
        sv.unique_trading_dates,
        ROUND(
            (sv.return_volatility - gm.mkt_avg_volatility) / NULLIF(gm.mkt_std_volatility, 0),
            4
        ) AS volatility_z_score,
        ROUND(
            sv.total_volume * 100.0 / NULLIF(gm.mkt_total_volume, 0),
            4
        ) AS pct_market_volume,
        RANK() OVER (ORDER BY sv.return_volatility DESC) AS rank_by_volatility,
        RANK() OVER (ORDER BY sv.total_volume DESC) AS rank_by_volume
    FROM symbol_volatility sv
    CROSS JOIN global_market gm
)

SELECT * FROM scored
