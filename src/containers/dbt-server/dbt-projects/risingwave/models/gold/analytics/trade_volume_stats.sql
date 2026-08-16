WITH security_stats AS (
    SELECT
        s.symbol,
        s.sk_security_id,
        s.sk_company_id,
        COUNT(*) AS trade_count,
        COUNT(DISTINCT t.sk_account_id) AS unique_accounts,
        COUNT(DISTINCT t.sk_broker_id) AS unique_brokers,
        COUNT(DISTINCT CAST(t.create_timestamp AS DATE)) AS active_days,
        SUM(CAST(t.quantity AS BIGINT)) AS total_volume,
        ROUND(CAST(SUM(CAST(t.trade_price AS DOUBLE) * CAST(t.quantity AS DOUBLE)) AS NUMERIC), 4) AS total_notional,
        ROUND(CAST(CAST(SUM(CAST(ROUND(CAST(t.trade_price AS NUMERIC), 4) AS DECIMAL)) AS DOUBLE) / NULLIF(COUNT(t.trade_price), 0) AS NUMERIC), 4) AS avg_price,
        ROUND(CAST(STDDEV_SAMP(CAST(t.trade_price AS DOUBLE)) AS NUMERIC), 4) AS price_stddev,
        MIN(t.trade_price) AS min_price,
        MAX(t.trade_price) AS max_price,
        ROUND(CAST(CAST(SUM(CAST(ROUND(CAST(t.fee AS NUMERIC), 4) AS DECIMAL)) AS DOUBLE) / NULLIF(COUNT(t.fee), 0) AS NUMERIC), 4) AS avg_fee,
        ROUND(CAST(CAST(SUM(CAST(ROUND(CAST(t.commission AS NUMERIC), 4) AS DECIMAL)) AS DOUBLE) / NULLIF(COUNT(t.commission), 0) AS NUMERIC), 4) AS avg_commission,
        ROUND(CAST(SUM(CAST(t.fee AS DOUBLE) + CAST(t.commission AS DOUBLE)) AS NUMERIC), 4) AS total_cost
    FROM {{ ref('fact_trade') }} t
    JOIN {{ ref('dim_security') }} s
        ON t.sk_security_id = s.sk_security_id
    GROUP BY s.symbol, s.sk_security_id, s.sk_company_id
    HAVING COUNT(*) >= 2
),

unwatched_stats AS (
    SELECT *
    FROM security_stats ss
    WHERE NOT EXISTS (
        SELECT 1 FROM {{ ref('fact_watches') }} fw
        WHERE fw.sk_security_id = ss.sk_security_id
    )
),

global_stats AS (
    SELECT
        AVG(CAST(trade_count AS DOUBLE)) AS avg_trade_count,
        STDDEV_SAMP(CAST(trade_count AS DOUBLE)) AS std_trade_count,
        AVG(total_notional) AS avg_notional,
        STDDEV_SAMP(total_notional) AS std_notional,
        SUM(trade_count) AS global_total_trades,
        SUM(total_notional) AS global_total_notional
    FROM security_stats
),

scored AS (
    SELECT
        us.symbol,
        us.sk_security_id,
        us.sk_company_id,
        us.trade_count,
        us.unique_accounts,
        us.unique_brokers,
        us.active_days,
        us.total_volume,
        us.total_notional,
        us.avg_price,
        us.price_stddev,
        us.min_price,
        us.max_price,
        us.avg_fee,
        us.avg_commission,
        us.total_cost,
        ROUND(CAST(us.trade_count * 100.0 / NULLIF(gs.global_total_trades, 0) AS NUMERIC), 4) AS pct_of_trades,
        ROUND(CAST(us.total_notional * 100.0 / NULLIF(gs.global_total_notional, 0) AS NUMERIC), 4) AS pct_of_notional,
        ROUND(CAST((us.trade_count - gs.avg_trade_count) / NULLIF(gs.std_trade_count, 0) AS NUMERIC), 4) AS volume_z_score,
        ROUND(CAST((us.total_notional - gs.avg_notional) / NULLIF(gs.std_notional, 0) AS NUMERIC), 4) AS notional_z_score,
        RANK() OVER (PARTITION BY 1 ORDER BY us.total_notional DESC) AS rank_by_notional,
        RANK() OVER (PARTITION BY 1 ORDER BY us.trade_count DESC) AS rank_by_volume,
        DENSE_RANK() OVER (PARTITION BY 1 ORDER BY us.unique_accounts DESC) AS rank_by_diversity
    FROM (SELECT *, 1 AS join_key FROM unwatched_stats) us
    JOIN (SELECT *, 1 AS join_key FROM global_stats) gs ON us.join_key = gs.join_key
)

SELECT * FROM scored
