select
    cik as company_id,
    st.st_name as status,
    company_name as name,
    ind.in_name as industry,
    ceo_name as ceo,
    address_line1,
    address_line2,
    postal_code,
    city,
    state_province,
    country,
    description,
    founding_date,
    sp_rating,
    pts as effective_timestamp,
    coalesce(
        lag(pts) over (
            partition by cik
            order by pts desc, company_name, status, industry_id, ceo_name
        ) - interval 1 millisecond,
        to_timestamp('9999-12-31 23:59:59.999')
    ) as end_timestamp,
    case
        when row_number() over (
            partition by cik
            order by pts desc, company_name, status, industry_id, ceo_name
        ) = 1 then true
        else false
    end as is_current
from {{ ref("finwire_company") }} cmp
join {{ ref("reference_status_type") }} st on cmp.status = st.st_id
join {{ ref("reference_industry") }} ind on cmp.industry_id = ind.in_id
