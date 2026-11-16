-- catalog-override: Materialize needs numeric ROUND, Spark NULL ordering and numeric accumulation to avoid float aggregate truncation (https://materialize.com/docs/sql/types/float/#aggregate-precision).
WITH customer_positions AS (
    SELECT
        ft.sk_customer_id,
        ft.sk_account_id,
        ft.sk_security_id,
        s.symbol,
        COUNT(*) AS trade_count,
        CAST(SUM(CAST(ft.quantity AS BIGINT)) AS BIGINT) AS total_qty,
        CAST(SUM(CAST(CAST(ft.trade_price AS DOUBLE PRECISION) * CAST(ft.quantity AS DOUBLE PRECISION) AS NUMERIC)) AS DOUBLE PRECISION) AS position_value,
        CAST(AVG(CAST(ft.trade_price AS NUMERIC)) AS DOUBLE PRECISION) AS avg_trade_price,
        CAST(SUM(CAST(CAST(ft.fee AS DOUBLE PRECISION) + CAST(ft.commission AS DOUBLE PRECISION) AS NUMERIC)) AS DOUBLE PRECISION) AS total_costs
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
        CAST(SUM(trade_count) AS BIGINT) AS total_trades,
        CAST(SUM(CAST(position_value AS NUMERIC)) AS DOUBLE PRECISION) AS total_portfolio_value,
        CAST(SUM(CAST(total_costs AS NUMERIC)) AS DOUBLE PRECISION) AS total_costs,
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
        CAST(AVG(CAST(total_portfolio_value AS NUMERIC)) AS DOUBLE PRECISION) AS avg_portfolio,
        CAST(STDDEV(CAST(total_portfolio_value AS NUMERIC)) AS DOUBLE PRECISION) AS std_portfolio,
        CAST(AVG(CAST(num_securities AS NUMERIC)) AS DOUBLE PRECISION) AS avg_securities,
        CAST(SUM(CAST(total_portfolio_value AS NUMERIC)) AS DOUBLE PRECISION) AS market_total
    FROM customer_totals
),

scored AS (
    SELECT
        uc.sk_customer_id,
        uc.num_accounts,
        uc.num_securities,
        uc.total_trades,
        CAST(ROUND(CAST(uc.total_portfolio_value AS NUMERIC), 6) AS DOUBLE PRECISION) AS total_portfolio_value,
        CAST(ROUND(CAST(uc.total_costs AS NUMERIC), 6) AS DOUBLE PRECISION) AS total_costs,
        CAST(ROUND(CAST(uc.largest_position AS NUMERIC), 6) AS DOUBLE PRECISION) AS largest_position,
        CAST(ROUND(CAST(
            uc.largest_position * 100.0 / NULLIF(uc.total_portfolio_value, 0) AS NUMERIC),
            4
        ) AS DOUBLE PRECISION) AS concentration_pct,
        CAST(ROUND(CAST(
            (uc.total_portfolio_value - gc.avg_portfolio) / NULLIF(gc.std_portfolio, 0) AS NUMERIC),
            4
        ) AS DOUBLE PRECISION) AS portfolio_z_score,
        CAST(ROUND(CAST(
            uc.total_portfolio_value * 100.0 / NULLIF(gc.market_total, 0) AS NUMERIC),
            4
        ) AS DOUBLE PRECISION) AS pct_of_market,
        -- Use displayed precision for ties, not sub-precision floating-point differences.
        CAST(DENSE_RANK() OVER (ORDER BY CAST(ROUND(CAST(uc.total_portfolio_value AS NUMERIC), 6) AS DOUBLE PRECISION) DESC NULLS LAST) AS INTEGER) AS rank_by_portfolio,
        CAST(DENSE_RANK() OVER (ORDER BY uc.num_securities DESC NULLS LAST) AS INTEGER) AS rank_by_diversity
    FROM unwatched_customers uc
    CROSS JOIN global_customer gc
)

SELECT * FROM scored
