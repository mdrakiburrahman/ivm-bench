with s1 as (
    select
        hh_t_id as trade_id,
        hh_h_t_id as previous_trade_id,
        hh_before_qty as previous_quantity,
        hh_after_qty as quantity
    from {{ ref('brokerage_holding_history') }}
)
select s1.*,
       ct.account_id,
       ct.symbol,
       ct.create_timestamp,
       ct.close_timestamp,
       ct.trade_price,
       ct.bid_price,
       ct.fee,
       ct.commission
from s1
join {{ ref('trades') }} ct
    using (trade_id)
