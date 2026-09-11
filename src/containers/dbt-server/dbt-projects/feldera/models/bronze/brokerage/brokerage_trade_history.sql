select *
from {{ ref('batch1_trade_history') }}
{% if env_var('TPCDI_BATCH_2_DAYS', '0') | int > 0 %}
union all
select t_id as th_t_id, t_dts as th_dts, t_st_id as th_st_id
from {{ ref('staging_trade') }}
where cdc_flag is not null
{% endif %}
