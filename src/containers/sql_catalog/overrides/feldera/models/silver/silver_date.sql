-- catalog-override: engine-only: Feldera exposes the reference-date projection at the unique silver_date path.
select *
from {{ ref('reference_date') }}
