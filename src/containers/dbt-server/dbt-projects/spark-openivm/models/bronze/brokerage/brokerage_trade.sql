-- Bronze: read trade from staging table (all loaded batches). The augmented
-- feed contains one row per status transition, so expose only the latest
-- state per trade while brokerage_trade_history retains every transition.
with trade_rows as (
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
)
{% if env_var('TPCDI_BATCH_2_DAYS', '0') | int > 0 %}
, latest_events as (
    select t_id, max(t_dts) as latest_dts
    from trade_rows
    group by t_id
)
select t.*
from trade_rows t
join latest_events l
    on t.t_id = l.t_id and t.t_dts = l.latest_dts
{% else %}
select * from trade_rows
{% endif %}
