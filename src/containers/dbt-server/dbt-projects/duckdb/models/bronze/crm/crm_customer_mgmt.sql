-- Bronze: flatten customer_mgmt XML struct + union with staging customer/account data
-- The batch1 customer_mgmt Delta table has nested structs from spark-xml parsing.
-- Staging customer/account tables accumulate batch2/3 flat CDC rows.

with batch1_xml as (
    select
        _ActionTS as action_ts,
        _ActionType as action_type,
        Customer._C_ID as c_id,
        Customer._C_TAX_ID as c_tax_id,
        Customer._C_GNDR as c_gndr,
        cast(Customer._C_TIER as int) as c_tier,
        Customer._C_DOB as c_dob,
        Customer.Name.C_L_NAME as c_l_name,
        Customer.Name.C_F_NAME as c_f_name,
        Customer.Name.C_M_NAME as c_m_name,
        Customer.Address.C_ADLINE1 as c_adline1,
        Customer.Address.C_ADLINE2 as c_adline2,
        Customer.Address.C_ZIPCODE as c_zipcode,
        Customer.Address.C_CITY as c_city,
        Customer.Address.C_STATE_PROV as c_state_prov,
        Customer.Address.C_CTRY as c_ctry,
        Customer.ContactInfo.C_PRIM_EMAIL as c_prim_email,
        Customer.ContactInfo.C_ALT_EMAIL as c_alt_email,
        concat_ws('-',
            cast(Customer.ContactInfo.C_PHONE_1.C_CTRY_CODE as varchar),
            cast(Customer.ContactInfo.C_PHONE_1.C_AREA_CODE as varchar),
            Customer.ContactInfo.C_PHONE_1.C_LOCAL,
            cast(Customer.ContactInfo.C_PHONE_1.C_EXT as varchar)
        ) as c_phone_1,
        concat_ws('-',
            cast(Customer.ContactInfo.C_PHONE_2.C_CTRY_CODE as varchar),
            cast(Customer.ContactInfo.C_PHONE_2.C_AREA_CODE as varchar),
            Customer.ContactInfo.C_PHONE_2.C_LOCAL,
            cast(Customer.ContactInfo.C_PHONE_2.C_EXT as varchar)
        ) as c_phone_2,
        concat_ws('-',
            cast(Customer.ContactInfo.C_PHONE_3.C_CTRY_CODE as varchar),
            cast(Customer.ContactInfo.C_PHONE_3.C_AREA_CODE as varchar),
            Customer.ContactInfo.C_PHONE_3.C_LOCAL,
            cast(Customer.ContactInfo.C_PHONE_3.C_EXT as varchar)
        ) as c_phone_3,
        Customer.TaxInfo.C_LCL_TX_ID as c_lcl_tx_id,
        Customer.TaxInfo.C_NAT_TX_ID as c_nat_tx_id,
        Customer.Account._CA_ID as ca_id,
        cast(Customer.Account._CA_TAX_ST as int) as ca_tax_st,
        Customer.Account.CA_B_ID as ca_b_id,
        Customer.Account.CA_NAME as ca_name
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
