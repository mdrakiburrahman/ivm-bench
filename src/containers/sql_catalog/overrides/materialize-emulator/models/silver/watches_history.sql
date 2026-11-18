-- catalog-override: Spark places the USING key first in s1.*; explicitly preserve that output order.
with s1 as (
    select
        w_c_id as customer_id,
        w_s_symb as symbol,
        w_dts as watch_timestamp,
        case w_action
            when 'ACTV' then 'Activate'
            when 'CNCL' then 'Cancelled'
            else null
        end as action_type
    from {{ ref('brokerage_watch_history') }}
)
select
    s1.symbol,
    s1.customer_id,
    s1.watch_timestamp,
    s1.action_type,
    s.company_id,
    s.company_name,
    s.exchange_id,
    s.status as security_status
from s1
join {{ ref('securities') }} s using (symbol)
