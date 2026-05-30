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
        ROUND(SUM(CAST(t.trade_price AS DOUBLE) * CAST(t.quantity AS DOUBLE)), 4) AS total_notional,
        ROUND(CAST(SUM(CAST(ROUND(t.trade_price, 4) AS DECIMAL(38, 4))) AS DOUBLE) / NULLIF(COUNT(t.trade_price), 0), 4) AS avg_price,
        ROUND(STDDEV(CAST(t.trade_price AS DOUBLE)), 4) AS price_stddev,
        MIN(t.trade_price) AS min_price,
        MAX(t.trade_price) AS max_price,
        ROUND(CAST(SUM(CAST(ROUND(t.fee, 4) AS DECIMAL(38, 4))) AS DOUBLE) / NULLIF(COUNT(t.fee), 0), 4) AS avg_fee,
        ROUND(CAST(SUM(CAST(ROUND(t.commission, 4) AS DECIMAL(38, 4))) AS DOUBLE) / NULLIF(COUNT(t.commission), 0), 4) AS avg_commission,
        ROUND(SUM(CAST(t.fee AS DOUBLE) + CAST(t.commission AS DOUBLE)), 4) AS total_cost
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
        STDDEV(CAST(trade_count AS DOUBLE)) AS std_trade_count,
        AVG(total_notional) AS avg_notional,
        STDDEV(total_notional) AS std_notional,
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
        ROUND(us.trade_count * 100.0 / NULLIF(gs.global_total_trades, 0), 4) AS pct_of_trades,
        ROUND(us.total_notional * 100.0 / NULLIF(gs.global_total_notional, 0), 4) AS pct_of_notional,
        ROUND((us.trade_count - gs.avg_trade_count) / NULLIF(gs.std_trade_count, 0), 4) AS volume_z_score,
        ROUND((us.total_notional - gs.avg_notional) / NULLIF(gs.std_notional, 0), 4) AS notional_z_score,
        RANK() OVER (ORDER BY us.total_notional DESC) AS rank_by_notional,
        RANK() OVER (ORDER BY us.trade_count DESC) AS rank_by_volume,
        DENSE_RANK() OVER (ORDER BY us.unique_accounts DESC) AS rank_by_diversity
    FROM unwatched_stats us
    CROSS JOIN global_stats gs
)

SELECT * FROM scored
