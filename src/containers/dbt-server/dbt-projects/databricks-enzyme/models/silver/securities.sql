select
    symbol,
    issue_type,
    case s.status
        when 'ACTV' then 'Active'
        when 'INAC' then 'Inactive'
        else null
    end as status,
    s.name,
    ex_id as exchange_id,
    sh_out as shares_outstanding,
    first_trade_date,
    first_exchange_date,
    dividend,
    coalesce(c1.name, c2.name) as company_name,
    coalesce(c1.company_id, c2.company_id) as company_id,
    pts as effective_timestamp,
    coalesce(
        lag(pts) over (
            partition by symbol
            order by pts desc, s.issue_type, s.ex_id, s.sh_out, s.status
        ) - interval 1 millisecond,
        to_timestamp('9999-12-31 23:59:59.999')
    ) as end_timestamp,
    case
        when row_number() over (
            partition by symbol
            order by pts desc, s.issue_type, s.ex_id, s.sh_out, s.status
        ) = 1 then true
        else false
    end as is_current
from {{ ref('finwire_security') }} s
left join {{ ref('companies') }} c1
    on cast(s.cik as string) = cast(c1.company_id as string)
    and pts between c1.effective_timestamp and c1.end_timestamp
left join {{ ref('companies') }} c2
    on s.company_name = c2.name
    and pts between c2.effective_timestamp and c2.end_timestamp
