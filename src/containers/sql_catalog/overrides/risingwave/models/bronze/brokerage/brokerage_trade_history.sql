-- catalog-override: RisingWave cannot transpile the conditional dbt Jinja source branch for staging trade-history rows
select *
from {{ source('tpcdi', 'batch1_trade_history') }}
