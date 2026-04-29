-- Bronze: read cash_transaction from staging table (all loaded batches)
select
    ct_ca_id,
    ct_dts,
    ct_amt,
    ct_name
from {{ ref('staging_cash_transaction') }}
