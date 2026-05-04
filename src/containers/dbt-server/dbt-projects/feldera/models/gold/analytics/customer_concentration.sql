WITH customer_positions AS (
    SELECT
        ft.sk_customer_id,
        ft.sk_account_id,
        ft.sk_security_id,
        s.symbol,
        COUNT(*) AS trade_count,
        SUM(CAST(ft.quantity AS BIGINT)) AS total_qty,
        SUM(CAST(ft.trade_price AS DOUBLE) * CAST(ft.quantity AS DOUBLE)) AS position_value,
        AVG(ft.trade_price) AS avg_trade_price,
        SUM(CAST(ft.fee AS DOUBLE) + CAST(ft.commission AS DOUBLE)) AS total_costs
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
),

unwatched_customers AS (
    SELECT *
    FROM customer_totals ct
    WHERE NOT EXISTS (
        SELECT 1 FROM {{ ref('fact_watches') }} fw
        WHERE fw.sk_customer_id = ct.sk_customer_id
    )
),

global_customer AS (
    SELECT
        AVG(total_portfolio_value) AS avg_portfolio,
        STDDEV(total_portfolio_value) AS std_portfolio,
        AVG(CAST(num_securities AS DOUBLE)) AS avg_securities,
        SUM(total_portfolio_value) AS market_total
    FROM customer_totals
),

scored AS (
    SELECT
        uc.sk_customer_id,
        uc.num_accounts,
        uc.num_securities,
        uc.total_trades,
        uc.total_portfolio_value,
        uc.total_costs,
        uc.largest_position,
        ROUND(
            uc.largest_position * 100.0 / NULLIF(uc.total_portfolio_value, 0),
            4
        ) AS concentration_pct,
        ROUND(
            (uc.total_portfolio_value - gc.avg_portfolio) / NULLIF(gc.std_portfolio, 0),
            4
        ) AS portfolio_z_score,
        ROUND(
            uc.total_portfolio_value * 100.0 / NULLIF(gc.market_total, 0),
            4
        ) AS pct_of_market,
        DENSE_RANK() OVER (ORDER BY uc.total_portfolio_value DESC) AS rank_by_portfolio,
        DENSE_RANK() OVER (ORDER BY uc.num_securities DESC) AS rank_by_diversity
    FROM unwatched_customers uc
    CROSS JOIN global_customer gc
)

SELECT * FROM scored
