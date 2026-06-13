WITH broker_trades AS (
    SELECT
        ft.sk_broker_id,
        b.broker_id,
        b.first_name,
        b.last_name,
        COUNT(*) AS trade_count,
        COUNT(DISTINCT ft.sk_customer_id) AS unique_customers,
        COUNT(DISTINCT ft.sk_security_id) AS unique_securities,
        COUNT(DISTINCT ft.sk_account_id) AS unique_accounts,
        COUNT(DISTINCT CAST(ft.create_timestamp AS DATE)) AS active_days,
        SUM(CAST(ft.quantity AS BIGINT)) AS total_volume,
        SUM(CAST(ft.trade_price AS DECIMAL(18, 6)) * CAST(ft.quantity AS DECIMAL(18, 0))) AS total_notional,
        AVG(CAST(ft.trade_price AS DECIMAL(18, 6))) AS avg_trade_price,
        SUM(CAST(ft.commission AS DECIMAL(18, 6))) AS total_commission,
        SUM(CAST(ft.fee AS DECIMAL(18, 6))) AS total_fees,
        AVG(CAST(ft.commission AS DECIMAL(18, 6))) AS avg_commission,
        AVG(CAST(ft.fee AS DECIMAL(18, 6))) AS avg_fee
    FROM {{ ref('fact_trade') }} ft
    JOIN {{ ref('dim_broker') }} b
        ON ft.sk_broker_id = b.sk_broker_id
    GROUP BY ft.sk_broker_id, b.broker_id, b.first_name, b.last_name
    HAVING COUNT(*) >= 1
)

SELECT
    sk_broker_id,
    broker_id,
    first_name,
    last_name,
    trade_count,
    unique_customers,
    unique_securities,
    unique_accounts,
    active_days,
    total_volume,
    ROUND(total_notional, 6) AS total_notional,
    ROUND(avg_trade_price, 6) AS avg_trade_price,
    ROUND(total_commission, 6) AS total_commission,
    ROUND(total_fees, 6) AS total_fees,
    ROUND(avg_commission, 6) AS avg_commission,
    ROUND(avg_fee, 6) AS avg_fee,
    ROUND(
        (total_commission + total_fees) * 100.0 / NULLIF(total_notional, 0),
        4
    ) AS cost_pct_of_notional,
    ROUND(
        total_notional / NULLIF(CAST(trade_count AS DECIMAL(18, 0)), 0),
        6
    ) AS avg_notional_per_trade
FROM broker_trades
