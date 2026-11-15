select *
from {{ source('tpcdi', 'batch1_tax_rate') }}
