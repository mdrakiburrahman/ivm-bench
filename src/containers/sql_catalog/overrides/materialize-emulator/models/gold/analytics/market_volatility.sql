-- catalog-override: Materialize needs numeric ROUND, Spark NULL ordering and numeric accumulation to avoid float aggregate truncation (https://materialize.com/docs/sql/types/float/#aggregate-precision).
WITH price_changes AS (
    SELECT
        dm_s_symb,
        dm_date,
        dm_close,
        dm_high,
        dm_low,
        dm_vol,
        LAG(dm_close) OVER (PARTITION BY dm_s_symb ORDER BY dm_date NULLS FIRST, dm_close NULLS FIRST, dm_vol NULLS FIRST) AS prev_close,
        LEAD(dm_close) OVER (PARTITION BY dm_s_symb ORDER BY dm_date NULLS FIRST, dm_close NULLS FIRST, dm_vol NULLS FIRST) AS next_close,
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
             THEN (dm_close - prev_close) / NULLIF(prev_close, 0)
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
        AVG(CAST(intraday_range AS DOUBLE PRECISION)) AS avg_intraday_range,
        MAX(intraday_range) AS max_intraday_range,
        CAST(SUM(CAST(dm_vol AS BIGINT)) AS BIGINT) AS total_volume,
        AVG(CAST(dm_vol AS DOUBLE PRECISION)) AS avg_volume,
        STDDEV(CAST(dm_vol AS NUMERIC)) AS volume_volatility,
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
        CAST(SUM(total_volume) AS BIGINT) AS mkt_total_volume
    FROM symbol_volatility
),

scored AS (
    SELECT
        sv.dm_s_symb,
        sv.trading_days,
        CAST(ROUND(CAST(sv.avg_daily_return AS NUMERIC), 4) AS DOUBLE PRECISION) AS avg_daily_return,
        CAST(ROUND(CAST(sv.return_volatility AS NUMERIC), 4) AS DOUBLE PRECISION) AS return_volatility,
        CAST(ROUND(CAST(sv.avg_intraday_range AS NUMERIC), 4) AS DOUBLE PRECISION) AS avg_intraday_range,
        sv.max_intraday_range,
        sv.total_volume,
        CAST(ROUND(CAST(sv.avg_volume AS NUMERIC), 4) AS DOUBLE PRECISION) AS avg_volume,
        CAST(ROUND(sv.volume_volatility, 4) AS DOUBLE PRECISION) AS volume_volatility,
        sv.unique_trading_dates,
        CAST(ROUND(CAST(
            (sv.return_volatility - gm.mkt_avg_volatility) / NULLIF(gm.mkt_std_volatility, 0) AS NUMERIC),
            4
        ) AS DOUBLE PRECISION) AS volatility_z_score,
        CAST(ROUND(
            sv.total_volume * 100.0 / NULLIF(gm.mkt_total_volume, 0),
            4
        ) AS NUMERIC(29, 4)) AS pct_market_volume,
        CAST(RANK() OVER (ORDER BY sv.return_volatility DESC NULLS LAST, sv.dm_s_symb NULLS FIRST) AS INTEGER) AS rank_by_volatility,
        CAST(RANK() OVER (ORDER BY sv.total_volume DESC NULLS LAST, sv.dm_s_symb NULLS FIRST) AS INTEGER) AS rank_by_volume
    FROM symbol_volatility sv
    CROSS JOIN global_market gm
)

SELECT * FROM scored
