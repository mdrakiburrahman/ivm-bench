WITH customer_positions AS (
    SELECT
        ft.sk_customer_id,
        ft.sk_account_id,
        ft.sk_security_id,
        s.symbol,
        COUNT(*) AS trade_count,
        SUM(CAST(ft.quantity AS BIGINT)) AS total_qty,
        SUM(CAST(ft.trade_price AS DECIMAL(18, 6)) * CAST(ft.quantity AS DECIMAL(18, 0))) AS position_value,
        SUM(CAST(ft.fee AS DECIMAL(18, 6)) + CAST(ft.commission AS DECIMAL(18, 6))) AS total_costs
    FROM {{ ref('fact_trade') }} ft
    JOIN {{ ref('dim_security') }} s
        ON ft.sk_security_id = s.sk_security_id
    GROUP BY ft.sk_customer_id, ft.sk_account_id, ft.sk_security_id, s.symbol
),

customer_totals_raw AS (
    SELECT
        sk_customer_id,
        COUNT(DISTINCT sk_account_id) AS num_accounts,
        COUNT(DISTINCT sk_security_id) AS num_securities,
        SUM(trade_count) AS total_trades,
        SUM(position_value) AS total_portfolio_value,
        SUM(total_costs) AS total_costs,
        MAX(position_value) AS largest_position
    FROM customer_positions
    GROUP BY sk_customer_id
),

customer_totals AS (
    SELECT * FROM customer_totals_raw WHERE total_trades >= 2
),

customer_totals_rounded AS (
    SELECT
        sk_customer_id,
        num_accounts,
        num_securities,
        total_trades,
        ROUND(total_portfolio_value, 2) AS total_portfolio_value,
        ROUND(total_costs, 2) AS total_costs,
        ROUND(largest_position, 2) AS largest_position
    FROM customer_totals
)

SELECT
    sk_customer_id,
    num_accounts,
    num_securities,
    total_trades,
    total_portfolio_value,
    total_costs,
    largest_position,
    ROUND(
        largest_position * 100.0 / NULLIF(total_portfolio_value, 0),
        2
    ) AS concentration_pct,
    ROUND(
        total_costs * 100.0 / NULLIF(total_portfolio_value, 0),
        2
    ) AS cost_pct_of_portfolio,
    ROUND(
        total_portfolio_value / NULLIF(CAST(total_trades AS DECIMAL(18, 0)), 0),
        2
    ) AS avg_value_per_trade
FROM customer_totals_rounded
