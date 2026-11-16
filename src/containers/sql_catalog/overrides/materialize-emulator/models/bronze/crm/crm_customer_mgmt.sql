-- catalog-override: Materialize stores the XML-derived nested Customer struct as JSONB.
with batch1_xml as (
    select
        _actionts as action_ts,
        _actiontype as action_type,
        cast(customer ->> '_C_ID' as bigint) as c_id,
        customer ->> '_C_TAX_ID' as c_tax_id,
        customer ->> '_C_GNDR' as c_gndr,
        cast(customer ->> '_C_TIER' as int) as c_tier,
        try_cast(customer ->> '_C_DOB' as date) as c_dob,
        customer -> 'Name' ->> 'C_L_NAME' as c_l_name,
        customer -> 'Name' ->> 'C_F_NAME' as c_f_name,
        customer -> 'Name' ->> 'C_M_NAME' as c_m_name,
        customer -> 'Address' ->> 'C_ADLINE1' as c_adline1,
        customer -> 'Address' ->> 'C_ADLINE2' as c_adline2,
        customer -> 'Address' ->> 'C_ZIPCODE' as c_zipcode,
        customer -> 'Address' ->> 'C_CITY' as c_city,
        customer -> 'Address' ->> 'C_STATE_PROV' as c_state_prov,
        customer -> 'Address' ->> 'C_CTRY' as c_ctry,
        customer -> 'ContactInfo' ->> 'C_PRIM_EMAIL' as c_prim_email,
        customer -> 'ContactInfo' ->> 'C_ALT_EMAIL' as c_alt_email,
        concat_ws('-',
            customer -> 'ContactInfo' -> 'C_PHONE_1' ->> 'C_CTRY_CODE',
            customer -> 'ContactInfo' -> 'C_PHONE_1' ->> 'C_AREA_CODE',
            customer -> 'ContactInfo' -> 'C_PHONE_1' ->> 'C_LOCAL',
            customer -> 'ContactInfo' -> 'C_PHONE_1' ->> 'C_EXT'
        ) as c_phone_1,
        concat_ws('-',
            customer -> 'ContactInfo' -> 'C_PHONE_2' ->> 'C_CTRY_CODE',
            customer -> 'ContactInfo' -> 'C_PHONE_2' ->> 'C_AREA_CODE',
            customer -> 'ContactInfo' -> 'C_PHONE_2' ->> 'C_LOCAL',
            customer -> 'ContactInfo' -> 'C_PHONE_2' ->> 'C_EXT'
        ) as c_phone_2,
        concat_ws('-',
            customer -> 'ContactInfo' -> 'C_PHONE_3' ->> 'C_CTRY_CODE',
            customer -> 'ContactInfo' -> 'C_PHONE_3' ->> 'C_AREA_CODE',
            customer -> 'ContactInfo' -> 'C_PHONE_3' ->> 'C_LOCAL',
            customer -> 'ContactInfo' -> 'C_PHONE_3' ->> 'C_EXT'
        ) as c_phone_3,
        customer -> 'TaxInfo' ->> 'C_LCL_TX_ID' as c_lcl_tx_id,
        customer -> 'TaxInfo' ->> 'C_NAT_TX_ID' as c_nat_tx_id,
        cast(customer -> 'Account' ->> '_CA_ID' as bigint) as ca_id,
        cast(customer -> 'Account' ->> '_CA_TAX_ST' as int) as ca_tax_st,
        cast(customer -> 'Account' ->> 'CA_B_ID' as bigint) as ca_b_id,
        customer -> 'Account' ->> 'CA_NAME' as ca_name
    from {{ source('tpcdi', 'batch1_customer_mgmt') }}
),
staging_customers as (
    select
        to_timestamp(cdc_dsn) as action_ts,
        case when cdc_flag = 'I' then 'NEW'
             when cdc_flag = 'U' and status = 'INAC' then 'INACT'
             when cdc_flag = 'U' then 'UPDCUST' end as action_type,
        cast(customerid as bigint) as c_id,
        taxid as c_tax_id, gender as c_gndr, cast(tier as int) as c_tier, dob as c_dob,
        lastname as c_l_name, firstname as c_f_name, middleinitial as c_m_name,
        addressline1 as c_adline1, addressline2 as c_adline2, postalcode as c_zipcode,
        city as c_city, stateprov as c_state_prov, country as c_ctry,
        email1 as c_prim_email, email2 as c_alt_email,
        concat_ws('-', c_ctry_1, c_area_1, c_local_1, c_ext_1) as c_phone_1,
        concat_ws('-', c_ctry_2, c_area_2, c_local_2, c_ext_2) as c_phone_2,
        concat_ws('-', c_ctry_3, c_area_3, c_local_3, c_ext_3) as c_phone_3,
        lcl_tx_id as c_lcl_tx_id, nat_tx_id as c_nat_tx_id,
        cast(null as bigint) as ca_id, cast(null as int) as ca_tax_st,
        cast(null as bigint) as ca_b_id, cast(null as text) as ca_name
    from {{ source('tpcdi', 'staging_customer') }}
    where cdc_flag in ('I', 'U')
),
staging_accounts as (
    select
        to_timestamp(cdc_dsn) as action_ts,
        case when cdc_flag = 'I' then 'ADDACCT'
             when cdc_flag = 'U' and ca_st_id = 'INAC' then 'CLOSEACCT'
             when cdc_flag = 'U' then 'UPDACCT' end as action_type,
        cast(ca_c_id as bigint) as c_id,
        cast(null as text) as c_tax_id, cast(null as text) as c_gndr,
        cast(null as int) as c_tier, cast(null as date) as c_dob,
        cast(null as text) as c_l_name, cast(null as text) as c_f_name,
        cast(null as text) as c_m_name, cast(null as text) as c_adline1,
        cast(null as text) as c_adline2, cast(null as text) as c_zipcode,
        cast(null as text) as c_city, cast(null as text) as c_state_prov,
        cast(null as text) as c_ctry, cast(null as text) as c_prim_email,
        cast(null as text) as c_alt_email, cast(null as text) as c_phone_1,
        cast(null as text) as c_phone_2, cast(null as text) as c_phone_3,
        cast(null as text) as c_lcl_tx_id, cast(null as text) as c_nat_tx_id,
        cast(accountid as bigint) as ca_id, cast(taxstatus as int) as ca_tax_st,
        ca_b_id, accountdesc as ca_name
    from {{ source('tpcdi', 'staging_account') }}
    where cdc_flag in ('I', 'U')
)
select * from batch1_xml
union all select * from staging_customers
union all select * from staging_accounts
