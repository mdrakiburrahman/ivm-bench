-- Bronze: union cash_transaction across all batches
select
    ct_ca_id,
    ct_dts,
    ct_amt,
    ct_name
from {{ ref('batch1_cash_transaction') }}

union all

select
    ct_ca_id,
    ct_dts,
    ct_amt,
    ct_name
from {{ ref('batch2_cash_transaction') }}

union all

select
    ct_ca_id,
    ct_dts,
    ct_amt,
    ct_name
from {{ ref('batch3_cash_transaction') }}
