-- Bronze: union holding_history across all batches
select
    hh_h_t_id,
    hh_t_id,
    hh_before_qty,
    hh_after_qty
from {{ source('tpcdi', 'batch1_holding_history') }}

union all

select
    hh_h_t_id,
    hh_t_id,
    hh_before_qty,
    hh_after_qty
from {{ source('tpcdi', 'batch2_holding_history') }}

union all

select
    hh_h_t_id,
    hh_t_id,
    hh_before_qty,
    hh_after_qty
from {{ source('tpcdi', 'batch3_holding_history') }}
