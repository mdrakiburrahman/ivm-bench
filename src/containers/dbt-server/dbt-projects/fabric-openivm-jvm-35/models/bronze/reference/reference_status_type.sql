select *
from {{ source('tpcdi', 'batch1_status_type') }}
