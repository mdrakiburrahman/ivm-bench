-- catalog-override: Materialize needs numeric two-argument ROUND for doubles, bigint SUM casts, and Spark's descending NULL placement; retain explicit division guards instead of SQLGlot's rewritten guards.
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
        CAST(SUM(CAST(ft.quantity AS BIGINT)) AS BIGINT) AS total_volume,
        CAST(ROUND(CAST(SUM(CAST(ft.trade_price AS DOUBLE PRECISION) * CAST(ft.quantity AS DOUBLE PRECISION)) AS NUMERIC), 4) AS DOUBLE PRECISION) AS total_notional,
        CAST(ROUND(CAST(CAST(SUM(CAST(CAST(ROUND(CAST(ft.trade_price AS NUMERIC), 4) AS DOUBLE PRECISION) AS DECIMAL(38, 4))) AS DOUBLE PRECISION) / NULLIF(COUNT(ft.trade_price), 0) AS NUMERIC), 4) AS DOUBLE PRECISION) AS avg_trade_price,
        CAST(ROUND(CAST(SUM(ft.commission) AS NUMERIC), 4) AS DOUBLE PRECISION) AS total_commission,
        CAST(ROUND(CAST(SUM(ft.fee) AS NUMERIC), 4) AS DOUBLE PRECISION) AS total_fees,
        CAST(ROUND(CAST(CAST(SUM(CAST(CAST(ROUND(CAST(ft.commission AS NUMERIC), 4) AS DOUBLE PRECISION) AS DECIMAL(38, 4))) AS DOUBLE PRECISION) / NULLIF(COUNT(ft.commission), 0) AS NUMERIC), 4) AS DOUBLE PRECISION) AS avg_commission,
        CAST(ROUND(CAST(CAST(SUM(CAST(CAST(ROUND(CAST(ft.fee AS NUMERIC), 4) AS DOUBLE PRECISION) AS DECIMAL(38, 4))) AS DOUBLE PRECISION) / NULLIF(COUNT(ft.fee), 0) AS NUMERIC), 4) AS DOUBLE PRECISION) AS avg_fee
    FROM {{ ref('fact_trade') }} ft
    JOIN {{ ref('dim_broker') }} b
        ON ft.sk_broker_id = b.sk_broker_id
    GROUP BY ft.sk_broker_id, b.broker_id, b.first_name, b.last_name
    HAVING COUNT(*) >= 1
),

brokers_without_cash AS (
    SELECT *
    FROM broker_trades bt
    WHERE NOT EXISTS (
        SELECT 1
        FROM {{ ref('fact_trade') }} ft
        JOIN {{ ref('fact_cash_transactions') }} fct
            ON ft.sk_account_id = fct.sk_account_id
        WHERE ft.sk_broker_id = bt.sk_broker_id
    )
),

global_broker AS (
    SELECT
        CAST(SUM(trade_count) AS BIGINT) AS total_trades,
        SUM(total_notional) AS total_market_notional,
        AVG(total_notional) AS avg_broker_notional,
        STDDEV(total_notional) AS std_broker_notional,
        AVG(CAST(unique_customers AS DOUBLE PRECISION)) AS avg_customers_per_broker,
        STDDEV(CAST(unique_customers AS DOUBLE PRECISION)) AS std_customers_per_broker
    FROM broker_trades
),

scored AS (
    SELECT
        bw.sk_broker_id,
        bw.broker_id,
        bw.first_name,
        bw.last_name,
        bw.trade_count,
        bw.unique_customers,
        bw.unique_securities,
        bw.unique_accounts,
        bw.active_days,
        bw.total_volume,
        bw.total_notional,
        bw.avg_trade_price,
        bw.total_commission,
        bw.total_fees,
        bw.avg_commission,
        bw.avg_fee,
        CAST(ROUND(bw.trade_count * 100.0 / NULLIF(gb.total_trades, 0), 4) AS NUMERIC(29, 4)) AS pct_of_trades,
        CAST(ROUND(CAST(bw.total_notional * 100.0 / NULLIF(gb.total_market_notional, 0) AS NUMERIC), 4) AS DOUBLE PRECISION) AS pct_of_notional,
        CAST(ROUND(CAST(
            (bw.total_notional - gb.avg_broker_notional) / NULLIF(gb.std_broker_notional, 0) AS NUMERIC),
            4
        ) AS DOUBLE PRECISION) AS notional_z_score,
        CAST(ROUND(CAST(
            (bw.unique_customers - gb.avg_customers_per_broker) / NULLIF(gb.std_customers_per_broker, 0) AS NUMERIC),
            4
        ) AS DOUBLE PRECISION) AS customer_z_score,
        CAST(DENSE_RANK() OVER (ORDER BY bw.total_notional DESC NULLS LAST) AS INTEGER) AS rank_by_notional,
        CAST(DENSE_RANK() OVER (ORDER BY bw.unique_customers DESC NULLS LAST) AS INTEGER) AS rank_by_customers,
        CAST(DENSE_RANK() OVER (ORDER BY bw.total_commission DESC NULLS LAST) AS INTEGER) AS rank_by_commission
    FROM brokers_without_cash bw
    CROSS JOIN global_broker gb
)

SELECT * FROM scored
