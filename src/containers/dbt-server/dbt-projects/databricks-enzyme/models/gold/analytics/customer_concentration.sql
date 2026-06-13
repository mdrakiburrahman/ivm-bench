WITH customer_positions AS (
    SELECT
        ft.sk_customer_id,
        ft.sk_account_id,
        ft.sk_security_id,
        s.symbol,
        COUNT(*) AS trade_count,
        SUM(CAST(ft.quantity AS BIGINT)) AS total_qty,
        SUM(CAST(ft.trade_price AS DECIMAL(18, 6)) * CAST(ft.quantity AS DECIMAL(18, 0))) AS position_value,
        AVG(CAST(ft.trade_price AS DECIMAL(18, 6))) AS avg_trade_price,
        SUM(CAST(ft.fee AS DECIMAL(18, 6)) + CAST(ft.commission AS DECIMAL(18, 6))) AS total_costs
    FROM {{ ref('fact_trade') }} ft
    JOIN {{ ref('dim_security') }} s
        ON ft.sk_security_id = s.sk_security_id
    GROUP BY ft.sk_customer_id, ft.sk_account_id, ft.sk_security_id, s.symbol
),

customer_totals AS (
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
    HAVING SUM(trade_count) >= 2
)

SELECT
    sk_customer_id,
    num_accounts,
    num_securities,
    total_trades,
    ROUND(total_portfolio_value, 6) AS total_portfolio_value,
    ROUND(total_costs, 6) AS total_costs,
    ROUND(largest_position, 6) AS largest_position,
    ROUND(
        largest_position * 100.0 / NULLIF(total_portfolio_value, 0),
        4
    ) AS concentration_pct,
    ROUND(
        total_costs * 100.0 / NULLIF(total_portfolio_value, 0),
        4
    ) AS cost_pct_of_portfolio,
    ROUND(
        total_portfolio_value / NULLIF(CAST(total_trades AS DECIMAL(18, 0)), 0),
        6
    ) AS avg_value_per_trade
FROM customer_totals
