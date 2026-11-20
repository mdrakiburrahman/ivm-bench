-- catalog-override: Materialize needs numeric two-argument ROUND for doubles, bigint SUM casts and Spark's descending NULL placement; preserve explicit division guards.
WITH security_stats AS (
    SELECT
        s.symbol,
        s.sk_security_id,
        s.sk_company_id,
        COUNT(*) AS trade_count,
        COUNT(DISTINCT t.sk_account_id) AS unique_accounts,
        COUNT(DISTINCT t.sk_broker_id) AS unique_brokers,
        COUNT(DISTINCT CAST(t.create_timestamp AS DATE)) AS active_days,
        CAST(SUM(CAST(t.quantity AS BIGINT)) AS BIGINT) AS total_volume,
        CAST(ROUND(CAST(SUM(CAST(t.trade_price AS DOUBLE PRECISION) * CAST(t.quantity AS DOUBLE PRECISION)) AS NUMERIC), 4) AS DOUBLE PRECISION) AS total_notional,
        CAST(ROUND(CAST(CAST(SUM(CAST(CAST(ROUND(CAST(t.trade_price AS NUMERIC), 4) AS DOUBLE PRECISION) AS DECIMAL(38, 4))) AS DOUBLE PRECISION) / NULLIF(COUNT(t.trade_price), 0) AS NUMERIC), 4) AS DOUBLE PRECISION) AS avg_price,
        CAST(ROUND(CAST(STDDEV(CAST(t.trade_price AS DOUBLE PRECISION)) AS NUMERIC), 4) AS DOUBLE PRECISION) AS price_stddev,
        MIN(t.trade_price) AS min_price,
        MAX(t.trade_price) AS max_price,
        CAST(ROUND(CAST(CAST(SUM(CAST(CAST(ROUND(CAST(t.fee AS NUMERIC), 4) AS DOUBLE PRECISION) AS DECIMAL(38, 4))) AS DOUBLE PRECISION) / NULLIF(COUNT(t.fee), 0) AS NUMERIC), 4) AS DOUBLE PRECISION) AS avg_fee,
        CAST(ROUND(CAST(CAST(SUM(CAST(CAST(ROUND(CAST(t.commission AS NUMERIC), 4) AS DOUBLE PRECISION) AS DECIMAL(38, 4))) AS DOUBLE PRECISION) / NULLIF(COUNT(t.commission), 0) AS NUMERIC), 4) AS DOUBLE PRECISION) AS avg_commission,
        CAST(ROUND(CAST(SUM(CAST(t.fee AS DOUBLE PRECISION) + CAST(t.commission AS DOUBLE PRECISION)) AS NUMERIC), 4) AS DOUBLE PRECISION) AS total_cost
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
        AVG(CAST(trade_count AS DOUBLE PRECISION)) AS avg_trade_count,
        STDDEV(CAST(trade_count AS DOUBLE PRECISION)) AS std_trade_count,
        AVG(total_notional) AS avg_notional,
        STDDEV(total_notional) AS std_notional,
        CAST(SUM(trade_count) AS BIGINT) AS global_total_trades,
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
        CAST(ROUND(us.trade_count * 100.0 / NULLIF(gs.global_total_trades, 0), 4) AS NUMERIC(29, 4)) AS pct_of_trades,
        CAST(ROUND(CAST(us.total_notional * 100.0 / NULLIF(gs.global_total_notional, 0) AS NUMERIC), 4) AS DOUBLE PRECISION) AS pct_of_notional,
        CAST(ROUND(CAST((us.trade_count - gs.avg_trade_count) / NULLIF(gs.std_trade_count, 0) AS NUMERIC), 4) AS DOUBLE PRECISION) AS volume_z_score,
        CAST(ROUND(CAST((us.total_notional - gs.avg_notional) / NULLIF(gs.std_notional, 0) AS NUMERIC), 4) AS DOUBLE PRECISION) AS notional_z_score,
        CAST(RANK() OVER (ORDER BY us.total_notional DESC NULLS LAST) AS INTEGER) AS rank_by_notional,
        CAST(RANK() OVER (ORDER BY us.trade_count DESC NULLS LAST) AS INTEGER) AS rank_by_volume,
        CAST(DENSE_RANK() OVER (ORDER BY us.unique_accounts DESC NULLS LAST) AS INTEGER) AS rank_by_diversity
    FROM unwatched_stats us
    CROSS JOIN global_stats gs
)

SELECT * FROM scored
