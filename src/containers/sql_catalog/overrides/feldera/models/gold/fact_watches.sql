-- catalog-override: Feldera uses CAST(timestamp AS DATE) because Spark's one-argument TO_DATE conversion is not supported in this form
select
    sk_customer_id,
    sk_security_id,
    CAST(placed_timestamp AS DATE) as sk_date_placed,
    CAST(removed_timestamp AS DATE) as sk_date_removed,
    1 as watch_cnt
from
    {{ ref('watches') }} w
join
    {{ ref('dim_customer') }} c
on
    w.customer_id = c.customer_id
and
    placed_timestamp between c.effective_timestamp and c.end_timestamp
join
    {{ ref('dim_security') }} s
on
    w.symbol = s.symbol
and
    placed_timestamp between s.effective_timestamp and s.end_timestamp
