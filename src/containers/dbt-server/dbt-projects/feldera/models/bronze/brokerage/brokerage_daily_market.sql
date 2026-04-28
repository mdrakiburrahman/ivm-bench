-- Bronze: union daily_market across all batches
select
    dm_date,
    dm_s_symb,
    dm_close,
    dm_high,
    dm_low,
    dm_vol
from {{ ref('batch1_daily_market') }}

union all

select
    dm_date,
    dm_s_symb,
    dm_close,
    dm_high,
    dm_low,
    dm_vol
from {{ ref('batch2_daily_market') }}

union all

select
    dm_date,
    dm_s_symb,
    dm_close,
    dm_high,
    dm_low,
    dm_vol
from {{ ref('batch3_daily_market') }}
