-- catalog-override: Feldera's bronze inputs are in-project dbt models, so ref() must replace the external source() relation.
-- Bronze: read daily_market from staging table (all loaded batches)
select
    dm_date,
    dm_s_symb,
    dm_close,
    dm_high,
    dm_low,
    dm_vol
from {{ ref('staging_daily_market') }}
