with s1 as (
    select
        year,
        quarter,
        quarter_start_date,
        posting_date,
        revenue,
        earnings,
        eps,
        diluted_eps,
        margin,
        inventory,
        assets,
        liabilities,
        sh_out,
        diluted_sh_out,
        coalesce(c1.name, c2.name) as company_name,
        coalesce(c1.company_id, c2.company_id) as company_id,
        pts as effective_timestamp
    from {{ ref('finwire_financial') }} s
    left join {{ ref('companies') }} c1
        on cast(s.cik as string) = cast(c1.company_id as string)
        and pts between c1.effective_timestamp and c1.end_timestamp
    left join {{ ref('companies') }} c2
        on s.company_name = c2.name
        and pts between c2.effective_timestamp and c2.end_timestamp
)
select
    *,
    coalesce(
        lag(effective_timestamp) over (
            partition by company_id
            order by effective_timestamp desc
        ) - interval 1 millisecond,
        to_timestamp('9999-12-31 23:59:59.999')
    ) as end_timestamp,
    case
        when row_number() over (
            partition by company_id
            order by effective_timestamp desc
        ) = 1 then true
        else false
    end as is_current
from s1
