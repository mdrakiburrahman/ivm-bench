-- catalog-override: Spark's division returns NULL for a zero close price, whereas Materialize errors; retain the financials left join and its multiplicity.
select
    s.sk_security_id,
    s.sk_company_id,
    dmh.dm_date as sk_date_id,
    (s.dividend / NULLIF(dmh.dm_close, 0)) / 100 as yield,
    fifty_two_week_high,
    fifty_two_week_high_date as sk_fifty_two_week_high_date,
    fifty_two_week_low,
    fifty_two_week_low_date as sk_fifty_two_week_low_date,
    dm_close as closeprice,
    dm_high as dayhigh,
    dm_low as daylow,
    dm_vol as volume
from {{ ref("daily_market") }} dmh
join {{ ref("dim_security") }} s
    on s.symbol = dmh.dm_s_symb
    and dmh.dm_date between s.effective_timestamp and s.end_timestamp
left join {{ ref("wrk_company_financials") }} f
    using (sk_company_id)
