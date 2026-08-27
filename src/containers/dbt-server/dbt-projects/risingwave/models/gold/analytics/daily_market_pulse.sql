WITH daily_stats AS (
    SELECT
        dm.dm_date,
        COUNT(*) AS num_records,
        COUNT(DISTINCT dm.dm_s_symb) AS active_symbols,
        SUM(CAST(dm.dm_vol AS BIGINT)) AS total_volume,
        ROUND(CAST(CAST(SUM(CAST(ROUND(CAST(dm.dm_close AS NUMERIC), 6) AS DECIMAL)) AS DOUBLE) / NULLIF(COUNT(dm.dm_close), 0) AS NUMERIC), 6) AS avg_close_price,
        ROUND(CAST(STDDEV_SAMP(CAST(dm.dm_close AS DOUBLE)) AS NUMERIC), 6) AS close_dispersion,
        MIN(dm.dm_low) AS market_low,
        MAX(dm.dm_high) AS market_high,
        ROUND(CAST(CAST(SUM(CAST(ROUND(CAST(dm.dm_high - dm.dm_low AS NUMERIC), 6) AS DECIMAL)) AS DOUBLE) / NULLIF(COUNT(dm.dm_high), 0) AS NUMERIC), 6) AS avg_intraday_spread,
        SUM(CASE WHEN dm.dm_close >= dm.dm_low + (dm.dm_high - dm.dm_low) * 0.5
                 THEN 1 ELSE 0 END) AS closed_upper_half_count
    FROM {{ ref('daily_market') }} dm
    GROUP BY dm.dm_date
),

no_trade_days AS (
    SELECT *
    FROM daily_stats ds
    WHERE NOT EXISTS (
        SELECT 1 FROM {{ ref('fact_trade') }} ft
        WHERE CAST(ft.create_timestamp AS DATE) = ds.dm_date
    )
),

with_lags AS (
    SELECT
        ntd.*,
        LAG(ntd.total_volume) OVER (PARTITION BY 1 ORDER BY ntd.dm_date) AS prev_volume,
        LAG(ntd.avg_close_price) OVER (PARTITION BY 1 ORDER BY ntd.dm_date) AS prev_avg_close,
        LAG(ntd.active_symbols) OVER (PARTITION BY 1 ORDER BY ntd.dm_date) AS prev_active_symbols
    FROM no_trade_days ntd
),

global_daily AS (
    SELECT
        AVG(CAST(total_volume AS DOUBLE)) AS avg_daily_volume,
        STDDEV_SAMP(CAST(total_volume AS DOUBLE)) AS std_daily_volume,
        AVG(CAST(active_symbols AS DOUBLE)) AS avg_daily_symbols,
        SUM(total_volume) AS grand_total_volume
    FROM daily_stats
),

scored AS (
    SELECT
        wl.dm_date,
        wl.num_records,
        wl.active_symbols,
        wl.total_volume,
        wl.avg_close_price,
        wl.close_dispersion,
        wl.market_low,
        wl.market_high,
        wl.avg_intraday_spread,
        wl.closed_upper_half_count,
        CASE WHEN wl.prev_volume > 0
             THEN ROUND(CAST((wl.total_volume - wl.prev_volume) * 100.0 / wl.prev_volume AS NUMERIC), 4)
             ELSE NULL
        END AS volume_change_pct,
        CASE WHEN wl.prev_avg_close > 0
             THEN ROUND(CAST((wl.avg_close_price - wl.prev_avg_close) * 100.0 / wl.prev_avg_close AS NUMERIC), 4)
             ELSE NULL
        END AS close_change_pct,
        CASE WHEN wl.prev_active_symbols IS NULL THEN NULL
             ELSE wl.active_symbols - wl.prev_active_symbols
        END AS symbol_count_change,
        ROUND(CAST((wl.total_volume - gd.avg_daily_volume) / NULLIF(gd.std_daily_volume, 0) AS NUMERIC), 4) AS volume_z_score,
        ROUND(CAST(wl.total_volume * 100.0 / NULLIF(gd.grand_total_volume, 0) AS NUMERIC), 4) AS pct_of_total_volume,
        RANK() OVER (PARTITION BY 1 ORDER BY wl.total_volume DESC) AS rank_by_volume,
        RANK() OVER (PARTITION BY 1 ORDER BY wl.close_dispersion DESC) AS rank_by_dispersion
    FROM (SELECT *, 1 AS join_key FROM with_lags) wl
    JOIN (SELECT *, 1 AS join_key FROM global_daily) gd ON wl.join_key = gd.join_key
)

SELECT * FROM scored
