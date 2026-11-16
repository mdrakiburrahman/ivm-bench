select *
from {{ source('tpcdi', 'batch1_industry') }}
