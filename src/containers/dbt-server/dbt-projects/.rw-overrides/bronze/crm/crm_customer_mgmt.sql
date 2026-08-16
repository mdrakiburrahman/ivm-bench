-- Bronze: flatten customer_mgmt XML struct + union with staging customer/account data
-- RisingWave variant: the batch1 customer_mgmt Delta table arrives from spark-xml
-- as one deeply nested struct. services/risingwave_sources.py flattens that
-- struct at load time (RisingWave has STRUCT, but building nested struct
-- literals for every INSERT is fragile), so this model reads flat columns where
-- the DuckDB/Feldera versions walk dotted paths. The output schema is identical.
-- Staging customer/account tables accumulate batch2/3 flat CDC rows.

with batch1_xml as (
    select
        action_ts,
        action_type,
        c_id,
        c_tax_id,
        c_gndr,
        cast(c_tier as int) as c_tier,
        c_dob,
        c_l_name,
        c_f_name,
        c_m_name,
        c_adline1,
        c_adline2,
        c_zipcode,
        c_city,
        c_state_prov,
        c_ctry,
        c_prim_email,
        c_alt_email,
        concat_ws('-',
            cast(c_phone_1_ctry as varchar),
            cast(c_phone_1_area as varchar),
            c_phone_1_local,
            cast(c_phone_1_ext as varchar)
        ) as c_phone_1,
        concat_ws('-',
            cast(c_phone_2_ctry as varchar),
            cast(c_phone_2_area as varchar),
            c_phone_2_local,
            cast(c_phone_2_ext as varchar)
        ) as c_phone_2,
        concat_ws('-',
            cast(c_phone_3_ctry as varchar),
            cast(c_phone_3_area as varchar),
            c_phone_3_local,
            cast(c_phone_3_ext as varchar)
        ) as c_phone_3,
        c_lcl_tx_id,
        c_nat_tx_id,
        ca_id,
        cast(ca_tax_st as int) as ca_tax_st,
        ca_b_id,
        ca_name
    from {{ source('tpcdi', 'batch1_customer_mgmt') }}
),
-- Staging customer table accumulates batch2/3 CDC rows
staging_customers as (
    select
        to_timestamp(cdc_dsn) as action_ts,
        case cdc_flag when 'I' then 'NEW' when 'U' then 'UPDCUST' end as action_type,
        cast(customerid as bigint) as c_id,
        taxid as c_tax_id,
        gender as c_gndr,
        cast(tier as int) as c_tier,
        dob as c_dob,
        lastname as c_l_name,
        firstname as c_f_name,
        middleinitial as c_m_name,
        addressline1 as c_adline1,
        addressline2 as c_adline2,
        postalcode as c_zipcode,
        city as c_city,
        stateprov as c_state_prov,
        country as c_ctry,
        email1 as c_prim_email,
        email2 as c_alt_email,
        concat_ws('-', c_ctry_1, c_area_1, c_local_1, c_ext_1) as c_phone_1,
        concat_ws('-', c_ctry_2, c_area_2, c_local_2, c_ext_2) as c_phone_2,
        concat_ws('-', c_ctry_3, c_area_3, c_local_3, c_ext_3) as c_phone_3,
        lcl_tx_id as c_lcl_tx_id,
        nat_tx_id as c_nat_tx_id,
        cast(null as bigint) as ca_id,
        cast(null as int) as ca_tax_st,
        cast(null as bigint) as ca_b_id,
        cast(null as varchar) as ca_name
    from {{ source('tpcdi', 'staging_customer') }}
    where cdc_flag in ('I', 'U')
),
-- Staging account table accumulates batch2/3 CDC rows
staging_accounts as (
    select
        to_timestamp(cdc_dsn) as action_ts,
        case cdc_flag when 'I' then 'ADDACCT' when 'U' then 'UPDACCT' end as action_type,
        cast(ca_c_id as bigint) as c_id,
        cast(null as varchar) as c_tax_id,
        cast(null as varchar) as c_gndr,
        cast(null as int) as c_tier,
        cast(null as date) as c_dob,
        cast(null as varchar) as c_l_name,
        cast(null as varchar) as c_f_name,
        cast(null as varchar) as c_m_name,
        cast(null as varchar) as c_adline1,
        cast(null as varchar) as c_adline2,
        cast(null as varchar) as c_zipcode,
        cast(null as varchar) as c_city,
        cast(null as varchar) as c_state_prov,
        cast(null as varchar) as c_ctry,
        cast(null as varchar) as c_prim_email,
        cast(null as varchar) as c_alt_email,
        cast(null as varchar) as c_phone_1,
        cast(null as varchar) as c_phone_2,
        cast(null as varchar) as c_phone_3,
        cast(null as varchar) as c_lcl_tx_id,
        cast(null as varchar) as c_nat_tx_id,
        cast(accountid as bigint) as ca_id,
        cast(taxstatus as int) as ca_tax_st,
        ca_b_id as ca_b_id,
        accountdesc as ca_name
    from {{ source('tpcdi', 'staging_account') }}
    where cdc_flag in ('I', 'U')
)

select * from batch1_xml
union all
select * from staging_customers
union all
select * from staging_accounts
