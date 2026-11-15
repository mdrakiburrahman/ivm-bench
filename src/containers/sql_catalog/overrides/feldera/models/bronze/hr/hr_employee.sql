-- catalog-override: Feldera's bronze inputs are in-project dbt models, so ref() must replace the external source() relation.
select *
from {{ ref('batch1_hr') }}
