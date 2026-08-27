select *
from {{ source('tpcdi', 'batch1_trade_type') }}
