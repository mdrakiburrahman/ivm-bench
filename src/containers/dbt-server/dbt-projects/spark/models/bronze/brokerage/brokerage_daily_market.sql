-- Bronze: union daily_market across all batches
select
    dm_date,
    dm_s_symb,
    dm_close,
    dm_high,
    dm_low,
    dm_vol
from {{ source('tpcdi', 'batch1_daily_market') }}

union all

select
    dm_date,
    dm_s_symb,
    dm_close,
    dm_high,
    dm_low,
    dm_vol
from {{ source('tpcdi', 'batch2_daily_market') }}

union all

select
    dm_date,
    dm_s_symb,
    dm_close,
    dm_high,
    dm_low,
    dm_vol
from {{ source('tpcdi', 'batch3_daily_market') }}
