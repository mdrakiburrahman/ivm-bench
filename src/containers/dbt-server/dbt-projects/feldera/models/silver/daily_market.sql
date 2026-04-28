-- Silver: daily_market with 52-week highs/lows
-- Rewritten as self-join (Feldera does not support ROWS BETWEEN window frames)
with
    s1 as (
        select a.dm_date,
               a.dm_s_symb,
               a.dm_close,
               a.dm_high,
               a.dm_low,
               a.dm_vol,
               MIN(b.dm_low) as fifty_two_week_low,
               MAX(b.dm_high) as fifty_two_week_high
        from {{ ref('brokerage_daily_market') }} a
        join {{ ref('brokerage_daily_market') }} b
            on a.dm_s_symb = b.dm_s_symb
            and b.dm_date between a.dm_date - INTERVAL '365' DAY and a.dm_date
        GROUP BY a.dm_date, a.dm_s_symb, a.dm_close, a.dm_high, a.dm_low, a.dm_vol
    ),
    s2 as (
        select a.dm_date,
               a.dm_s_symb,
               a.dm_close,
               a.dm_high,
               a.dm_low,
               a.dm_vol,
               a.fifty_two_week_low,
               a.fifty_two_week_high,
               MIN(b.dm_date) as fifty_two_week_low_date,
               MIN(c.dm_date) as fifty_two_week_high_date
        from s1 a
        join {{ ref('brokerage_daily_market') }} b
            on a.dm_s_symb = b.dm_s_symb
            and a.fifty_two_week_low = b.dm_low
            and b.dm_date between a.dm_date - INTERVAL '365' DAY and a.dm_date
        join {{ ref('brokerage_daily_market') }} c
            on a.dm_s_symb = c.dm_s_symb
            and a.fifty_two_week_high = c.dm_high
            and c.dm_date between a.dm_date - INTERVAL '365' DAY and a.dm_date
        GROUP BY a.dm_date, a.dm_s_symb, a.dm_close, a.dm_high, a.dm_low, a.dm_vol,
                 a.fifty_two_week_low, a.fifty_two_week_high
    )
select *
from s2
