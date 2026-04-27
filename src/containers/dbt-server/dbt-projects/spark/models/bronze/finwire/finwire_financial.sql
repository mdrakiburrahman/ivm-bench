-- Bronze: parse FIN records from finwire fixed-width lines
with s1 as (
    select
        pts,
        trim(substring(line, 19, 4)) as year,
        trim(substring(line, 23, 1)) as quarter,
        trim(substring(line, 24, 8)) as quarter_start_date,
        trim(substring(line, 32, 8)) as posting_date,
        trim(substring(line, 40, 17)) as revenue,
        trim(substring(line, 57, 17)) as earnings,
        trim(substring(line, 74, 12)) as eps,
        trim(substring(line, 86, 12)) as diluted_eps,
        trim(substring(line, 98, 12)) as margin,
        trim(substring(line, 110, 17)) as inventory,
        trim(substring(line, 127, 17)) as assets,
        trim(substring(line, 144, 17)) as liabilities,
        trim(substring(line, 161, 13)) as sh_out,
        trim(substring(line, 174, 13)) as diluted_sh_out,
        trim(substring(line, 187, 60)) as co_name_or_cik,
        cast(
            case
                when regexp_like(trim(substring(line, 187, 60)), '^[0-9]+$')
                then trim(substring(line, 187, 60))
                else null
            end as bigint
        ) as try_cik
    from {{ source('tpcdi', 'batch1_finwire') }}
    where rec_type = 'FIN'
)
select
    pts,
    cast(year as int) as year,
    cast(quarter as int) as quarter,
    to_date(quarter_start_date, 'yyyyMMdd') as quarter_start_date,
    to_date(posting_date, 'yyyyMMdd') as posting_date,
    cast(revenue as double) as revenue,
    cast(earnings as double) as earnings,
    cast(eps as double) as eps,
    cast(diluted_eps as double) as diluted_eps,
    cast(margin as double) as margin,
    cast(inventory as double) as inventory,
    cast(assets as double) as assets,
    cast(liabilities as double) as liabilities,
    cast(sh_out as bigint) as sh_out,
    cast(diluted_sh_out as bigint) as diluted_sh_out,
    try_cik as cik,
    case when try_cik is null then co_name_or_cik else null end as company_name
from s1
