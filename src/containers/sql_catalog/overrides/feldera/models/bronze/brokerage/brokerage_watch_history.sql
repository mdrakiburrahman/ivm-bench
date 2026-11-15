-- catalog-override: Feldera's bronze inputs are in-project dbt models, so ref() must replace the external source() relation.
-- Bronze: read watch_history from staging table (all loaded batches)
select
    w_c_id,
    w_s_symb,
    w_dts,
    w_action
from {{ ref('staging_watch_history') }}
