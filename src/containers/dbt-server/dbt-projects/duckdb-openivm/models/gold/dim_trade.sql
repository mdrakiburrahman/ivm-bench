select
    {{ dbt_utils.generate_surrogate_key(['trade_id', 't.effective_timestamp']) }} as sk_trade_id,
    trade_id,
    trade_status as status,
    transaction_type,
    trade_type as type,
    executor_name as executed_by,
    t.effective_timestamp,
    t.end_timestamp,
    t.is_current
from
    {{ ref('trades_history') }} t
