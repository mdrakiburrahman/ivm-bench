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
        SUM(CAST(t.trade_price AS DECIMAL(18, 6)) * CAST(t.quantity AS DECIMAL(18, 0))) AS total_notional,
        AVG(CAST(t.trade_price AS DECIMAL(18, 6))) AS avg_price,
        STDDEV(CAST(t.trade_price AS DECIMAL(18, 6))) AS price_stddev,
        MIN(t.trade_price) AS min_price,
        MAX(t.trade_price) AS max_price,
        AVG(CAST(t.fee AS DECIMAL(18, 6))) AS avg_fee,
        AVG(CAST(t.commission AS DECIMAL(18, 6))) AS avg_commission,
        SUM(CAST(t.fee AS DECIMAL(18, 6)) + CAST(t.commission AS DECIMAL(18, 6))) AS total_cost
    FROM {{ ref('fact_trade') }} t
    JOIN {{ ref('dim_security') }} s
        ON t.sk_security_id = s.sk_security_id
    GROUP BY s.symbol, s.sk_security_id, s.sk_company_id
    HAVING COUNT(*) >= 2
)

SELECT
    symbol,
    sk_security_id,
    sk_company_id,
    trade_count,
    unique_accounts,
    unique_brokers,
    active_days,
    total_volume,
    ROUND(total_notional, 6) AS total_notional,
    ROUND(avg_price, 6) AS avg_price,
    ROUND(price_stddev, 6) AS price_stddev,
    min_price,
    max_price,
    ROUND(avg_fee, 6) AS avg_fee,
    ROUND(avg_commission, 6) AS avg_commission,
    ROUND(total_cost, 6) AS total_cost,
    ROUND(
        total_cost * 100.0 / NULLIF(total_notional, 0),
        4
    ) AS cost_pct_of_notional,
    ROUND(
        total_notional / NULLIF(CAST(trade_count AS DECIMAL(18, 0)), 0),
        6
    ) AS avg_notional_per_trade
FROM security_stats
