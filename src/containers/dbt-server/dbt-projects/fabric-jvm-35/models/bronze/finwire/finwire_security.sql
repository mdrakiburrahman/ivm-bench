-- Bronze: parse SEC records from finwire fixed-width lines
with s1 as (
    select
        pts,
        trim(substring(line, 19, 15)) as symbol,
        trim(substring(line, 34, 6)) as issue_type,
        trim(substring(line, 40, 4)) as status,
        trim(substring(line, 44, 70)) as name,
        trim(substring(line, 114, 6)) as ex_id,
        trim(substring(line, 120, 13)) as sh_out,
        trim(substring(line, 133, 8)) as first_trade_date,
        trim(substring(line, 141, 8)) as first_exchange_date,
        trim(substring(line, 149, 12)) as dividend,
        trim(substring(line, 161, 60)) as co_name_or_cik,
        cast(
            case
                when regexp_like(trim(substring(line, 161, 60)), '^[0-9]+$')
                then trim(substring(line, 161, 60))
                else null
            end as bigint
        ) as try_cik
    from {{ source('tpcdi', 'batch1_finwire') }}
    where rec_type = 'SEC'
)
select
    pts,
    symbol,
    issue_type,
    status,
    name,
    ex_id,
    cast(sh_out as bigint) as sh_out,
    to_date(first_trade_date, 'yyyyMMdd') as first_trade_date,
    to_date(first_exchange_date, 'yyyyMMdd') as first_exchange_date,
    cast(dividend as double) as dividend,
    try_cik as cik,
    case when try_cik is null then co_name_or_cik else null end as company_name
from s1
