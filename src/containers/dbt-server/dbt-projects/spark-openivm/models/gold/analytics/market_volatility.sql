WITH symbol_volatility AS (
    SELECT
        dm.dm_s_symb,
        COUNT(*) AS trading_days,
        AVG(CAST(dm.dm_high AS DECIMAL(18, 6)) - CAST(dm.dm_low AS DECIMAL(18, 6))) AS avg_intraday_range,
        MAX(dm.dm_high - dm.dm_low) AS max_intraday_range,
        SUM(CAST(dm.dm_vol AS BIGINT)) AS total_volume,
        AVG(CAST(dm.dm_vol AS DECIMAL(28, 0))) AS avg_volume,
        STDDEV(CAST(dm.dm_vol AS DECIMAL(28, 0))) AS volume_volatility,
        MIN(dm.dm_low) AS min_low,
        MAX(dm.dm_high) AS max_high,
        AVG(CAST(dm.dm_close AS DECIMAL(18, 6))) AS avg_close,
        STDDEV(CAST(dm.dm_close AS DECIMAL(18, 6))) AS close_volatility,
        COUNT(DISTINCT dm.dm_date) AS unique_trading_dates
    FROM {{ ref('daily_market') }} dm
    GROUP BY dm.dm_s_symb
    HAVING COUNT(*) >= 3
)

SELECT
    dm_s_symb,
    trading_days,
    ROUND(avg_intraday_range, 4) AS avg_intraday_range,
    max_intraday_range,
    total_volume,
    ROUND(avg_volume, 4) AS avg_volume,
    ROUND(volume_volatility, 4) AS volume_volatility,
    min_low,
    max_high,
    ROUND(avg_close, 6) AS avg_close,
    ROUND(close_volatility, 6) AS close_volatility,
    unique_trading_dates
FROM symbol_volatility
