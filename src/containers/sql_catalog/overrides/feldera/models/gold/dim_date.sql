-- catalog-override: Feldera's engine-only silver_date model is not named date; preserve the existing ref target
select *
from {{ ref('silver_date') }}
