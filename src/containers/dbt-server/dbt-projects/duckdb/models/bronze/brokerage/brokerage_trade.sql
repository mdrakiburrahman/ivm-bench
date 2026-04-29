-- Bronze: read trade from staging table (all loaded batches)
select
    t_id,
    t_dts,
    t_st_id,
    t_tt_id,
    t_is_cash,
    t_s_symb,
    t_qty,
    t_bid_price,
    t_ca_id,
    t_exec_name,
    t_trade_price,
    t_chrg,
    t_comm,
    t_tax
from {{ source('tpcdi', 'staging_trade') }}
