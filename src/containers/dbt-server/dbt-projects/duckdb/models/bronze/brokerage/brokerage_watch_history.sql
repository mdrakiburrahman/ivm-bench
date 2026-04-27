-- Bronze: union watch_history across all batches
select
    w_c_id,
    w_s_symb,
    w_dts,
    w_action
from {{ source('tpcdi', 'batch1_watch_history') }}

union all

select
    w_c_id,
    w_s_symb,
    w_dts,
    w_action
from {{ source('tpcdi', 'batch2_watch_history') }}

union all

select
    w_c_id,
    w_s_symb,
    w_dts,
    w_action
from {{ source('tpcdi', 'batch3_watch_history') }}
