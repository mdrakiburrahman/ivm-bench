-- catalog-override: Feldera uses CAST(timestamp AS DATE) because Spark's one-argument TO_DATE conversion is not supported in this form
with s1 as (
    select
        *,
        CAST(transaction_timestamp AS DATE) as sk_transaction_date
    from
        {{ ref('cash_transactions') }}
)
select
    sk_customer_id,
    sk_account_id,
    sk_transaction_date,
    transaction_timestamp,
    amount,
    description
from
    s1
join
    {{ ref('dim_account') }} a
on
    s1.account_id = a.account_id
and
    s1.transaction_timestamp between a.effective_timestamp and a.end_timestamp
