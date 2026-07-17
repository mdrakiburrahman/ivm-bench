-- Silver: daily_market with cumulative highs/lows
with cumulative as (
    select
        dm_date, dm_s_symb, dm_close, dm_high, dm_low, dm_vol,
        min(dm_low) over w as fifty_two_week_low,
        max(dm_high) over w as fifty_two_week_high
    from {{ ref('brokerage_daily_market') }}
    window w as (partition by dm_s_symb order by dm_date)
),
flagged as (
    select *,
        case when dm_low = fifty_two_week_low then dm_date else null end as low_date_flag,
        case when dm_high = fifty_two_week_high then dm_date else null end as high_date_flag
    from cumulative
)
select
    dm_date, dm_s_symb, dm_close, dm_high, dm_low, dm_vol,
    fifty_two_week_low,
    fifty_two_week_high,
    max(low_date_flag) over (partition by dm_s_symb order by dm_date) as fifty_two_week_low_date,
    max(high_date_flag) over (partition by dm_s_symb order by dm_date) as fifty_two_week_high_date
from flagged
