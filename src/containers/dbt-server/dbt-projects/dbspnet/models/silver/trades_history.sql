select
    t_id as trade_id,
    t_dts as trade_timestamp,
    t_ca_id as account_id,
    ts.st_name as trade_status,
    tt_name as trade_type,
    case
        when t_is_cash = 1 then 'Cash'
        when t_is_cash = 0 then 'Margin'
    end as transaction_type,
    t_s_symb as symbol,
    t_exec_name as executor_name,
    t_qty as quantity,
    t_bid_price as bid_price,
    t_trade_price as trade_price,
    t_chrg as fee,
    t_comm as commission,
    t_tax as tax,
    us.st_name as update_status,
    th_dts as effective_timestamp,
    coalesce(
        lag(th_dts) over (
            partition by t_id
            order by th_dts desc, t_dts desc, t_st_id desc, th_st_id desc
        ) - INTERVAL '0.001' SECOND,
        TIMESTAMP '9999-12-31 23:59:59.999'
    ) as end_timestamp,
    case
        when lag(t_id) over (
            partition by t_id
            order by th_dts desc, t_dts desc, t_st_id desc, th_st_id desc
        ) is null then true
        else false
    end as is_current
from
    {{ ref('brokerage_trade') }}
join
    {{ ref('brokerage_trade_history') }}
on
    t_id = th_t_id
join
    {{ ref('reference_trade_type') }}
on
    t_tt_id = tt_id
join
    {{ ref('reference_status_type') }} ts
on
    t_st_id = ts.st_id
join
    {{ ref('reference_status_type') }} us
on
    th_st_id = us.st_id
