-- Gold: dim_account
-- RisingWave variant: `using (broker_id)` cannot be used here. The two sides
-- disagree on type — accounts.broker_id comes from the customer_mgmt XML as
-- BIGINT, dim_broker.broker_id comes from the HR file as VARCHAR — and where
-- DuckDB coerces silently, RisingWave refuses with
-- 'function equal(bigint, character varying) does not exist'. The join is
-- therefore written out with both sides cast to VARCHAR. `using` also collapses
-- the duplicate column, but nothing downstream selects broker_id (only
-- sk_broker_id), so the explicit ON changes nothing else.
select
    {{ dbt_utils.generate_surrogate_key(['account_id', 'a.effective_timestamp']) }} as sk_account_id,
    a.account_id,
    sk_broker_id,
    sk_customer_id,
    a.status,
    account_desc,
    tax_status,
    a.effective_timestamp,
    a.end_timestamp,
    a.is_current
from
    {{ ref('accounts') }} a
join
    {{ ref('dim_customer') }} c
    on a.customer_id = c.customer_id
    and a.effective_timestamp between c.effective_timestamp and c.end_timestamp
join
    {{ ref('dim_broker') }} b
    on cast(a.broker_id as varchar) = cast(b.broker_id as varchar)
