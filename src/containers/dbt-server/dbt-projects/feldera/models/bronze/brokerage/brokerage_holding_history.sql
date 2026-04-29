-- Bronze: read holding_history from staging table (all loaded batches)
select
    hh_h_t_id,
    hh_t_id,
    hh_before_qty,
    hh_after_qty
from {{ ref('staging_holding_history') }}
