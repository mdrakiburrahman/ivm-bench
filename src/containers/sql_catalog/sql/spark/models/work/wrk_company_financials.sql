-- Work: company financials pre-joined for fact_market_history
select
    f.company_id,
    sk_company_id,
    f.eps,
    f.revenue,
    f.effective_timestamp,
    f.end_timestamp,
    f.is_current
from {{ ref('financials') }} f
join {{ ref('dim_company') }} c
    on f.company_id = c.company_id
    and f.effective_timestamp between c.effective_timestamp and c.end_timestamp
