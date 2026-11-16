-- catalog-override: Materialize float SUM truncates each input to 2^-24; accumulate as NUMERIC and retain the Spark DOUBLE output (https://materialize.com/docs/sql/types/float/#aggregate-precision).
with s1 as (
    select *
    from {{ ref('fact_cash_transactions') }}
)
select
    sk_customer_id,
    sk_account_id,
    sk_transaction_date,
    cast(sum(cast(amount as numeric)) as double precision) as amount,
    description
from s1
group by
    sk_customer_id,
    sk_account_id,
    sk_transaction_date,
    description
