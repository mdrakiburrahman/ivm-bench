-- catalog-override: Spark casts timestamps to UTC text without a timezone suffix when generating surrogate keys.
select
    {{ dbt_utils.generate_surrogate_key(['trade_id', "(t.effective_timestamp at time zone 'UTC')"]) }} as sk_trade_id,
    trade_id,
    trade_status as status,
    transaction_type,
    trade_type as type,
    executor_name as executed_by,
    t.effective_timestamp,
    t.end_timestamp,
    t.is_current
from {{ ref('trades_history') }} t
