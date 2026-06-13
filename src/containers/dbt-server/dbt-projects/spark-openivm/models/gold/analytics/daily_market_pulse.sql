WITH daily_stats AS (
    SELECT
        dm.dm_date,
        COUNT(*) AS num_records,
        COUNT(DISTINCT dm.dm_s_symb) AS active_symbols,
        SUM(CAST(dm.dm_vol AS BIGINT)) AS total_volume,
        AVG(CAST(dm.dm_close AS DECIMAL(18, 6))) AS avg_close_price,
        STDDEV(CAST(dm.dm_close AS DECIMAL(18, 6))) AS close_dispersion,
        MIN(dm.dm_low) AS market_low,
        MAX(dm.dm_high) AS market_high,
        AVG(CAST(dm.dm_high AS DECIMAL(18, 6)) - CAST(dm.dm_low AS DECIMAL(18, 6))) AS avg_intraday_spread,
        SUM(CASE WHEN dm.dm_close >= dm.dm_low + (dm.dm_high - dm.dm_low) * 0.5
                 THEN 1 ELSE 0 END) AS closed_upper_half_count
    FROM {{ ref('daily_market') }} dm
    GROUP BY dm.dm_date
)

SELECT
    dm_date,
    num_records,
    active_symbols,
    total_volume,
    ROUND(avg_close_price, 6) AS avg_close_price,
    ROUND(close_dispersion, 6) AS close_dispersion,
    market_low,
    market_high,
    ROUND(avg_intraday_spread, 6) AS avg_intraday_spread,
    closed_upper_half_count,
    ROUND(
        closed_upper_half_count * 100.0 / NULLIF(CAST(num_records AS DECIMAL(18, 0)), 0),
        4
    ) AS upper_half_close_pct,
    ROUND(
        total_volume / NULLIF(CAST(active_symbols AS DECIMAL(18, 0)), 0),
        4
    ) AS avg_volume_per_symbol
FROM daily_stats
