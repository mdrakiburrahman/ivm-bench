-- Bronze: flatten customer_mgmt XML struct + union with batch2/3 flat customer/account data
-- batch1 customer_mgmt Delta table has nested structs (ROW types from spark-xml).
-- Access nested fields via table.column.field dot notation per Feldera docs.

with batch1_xml as (
    select
        cm._ActionTS as action_ts,
        cm._ActionType as action_type,
        cm.Customer._C_ID as c_id,
        cm.Customer._C_TAX_ID as c_tax_id,
        cm.Customer._C_GNDR as c_gndr,
        CAST(cm.Customer._C_TIER AS INTEGER) as c_tier,
        cm.Customer._C_DOB as c_dob,
        cm.Customer.Name.C_L_NAME as c_l_name,
        cm.Customer.Name.C_F_NAME as c_f_name,
        cm.Customer.Name.C_M_NAME as c_m_name,
        cm.Customer.Address.C_ADLINE1 as c_adline1,
        cm.Customer.Address.C_ADLINE2 as c_adline2,
        cm.Customer.Address.C_ZIPCODE as c_zipcode,
        cm.Customer.Address.C_CITY as c_city,
        cm.Customer.Address.C_STATE_PROV as c_state_prov,
        cm.Customer.Address.C_CTRY as c_ctry,
        cm.Customer.ContactInfo.C_PRIM_EMAIL as c_prim_email,
        cm.Customer.ContactInfo.C_ALT_EMAIL as c_alt_email,
        CONCAT_WS('-',
            CAST(cm.Customer.ContactInfo.C_PHONE_1.C_CTRY_CODE AS VARCHAR),
            CAST(cm.Customer.ContactInfo.C_PHONE_1.C_AREA_CODE AS VARCHAR),
            cm.Customer.ContactInfo.C_PHONE_1.C_LOCAL,
            CAST(cm.Customer.ContactInfo.C_PHONE_1.C_EXT AS VARCHAR)
        ) as c_phone_1,
        CONCAT_WS('-',
            CAST(cm.Customer.ContactInfo.C_PHONE_2.C_CTRY_CODE AS VARCHAR),
            CAST(cm.Customer.ContactInfo.C_PHONE_2.C_AREA_CODE AS VARCHAR),
            cm.Customer.ContactInfo.C_PHONE_2.C_LOCAL,
            CAST(cm.Customer.ContactInfo.C_PHONE_2.C_EXT AS VARCHAR)
        ) as c_phone_2,
        CONCAT_WS('-',
            CAST(cm.Customer.ContactInfo.C_PHONE_3.C_CTRY_CODE AS VARCHAR),
            CAST(cm.Customer.ContactInfo.C_PHONE_3.C_AREA_CODE AS VARCHAR),
            cm.Customer.ContactInfo.C_PHONE_3.C_LOCAL,
            CAST(cm.Customer.ContactInfo.C_PHONE_3.C_EXT AS VARCHAR)
        ) as c_phone_3,
        cm.Customer.TaxInfo.C_LCL_TX_ID as c_lcl_tx_id,
        cm.Customer.TaxInfo.C_NAT_TX_ID as c_nat_tx_id,
        cm.Customer.Account._CA_ID as ca_id,
        CAST(cm.Customer.Account._CA_TAX_ST AS INTEGER) as ca_tax_st,
        cm.Customer.Account.CA_B_ID as ca_b_id,
        cm.Customer.Account.CA_NAME as ca_name
    from {{ ref('batch1_customer_mgmt') }} cm
),
-- Batch2/3 customer files have flat columns with CDC prefix
batch2_customers as (
    select
        CAST(cdc_dsn AS TIMESTAMP) as action_ts,
        case cdc_flag when 'I' then 'NEW' when 'U' then 'UPDCUST' end as action_type,
        CAST(customerid AS BIGINT) as c_id,
        taxid as c_tax_id,
        gender as c_gndr,
        CAST(tier AS INTEGER) as c_tier,
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
        CONCAT_WS('-', c_ctry_1, c_area_1, c_local_1, c_ext_1) as c_phone_1,
        CONCAT_WS('-', c_ctry_2, c_area_2, c_local_2, c_ext_2) as c_phone_2,
        CONCAT_WS('-', c_ctry_3, c_area_3, c_local_3, c_ext_3) as c_phone_3,
        lcl_tx_id as c_lcl_tx_id,
        nat_tx_id as c_nat_tx_id,
        CAST(null AS BIGINT) as ca_id,
        CAST(null AS INTEGER) as ca_tax_st,
        CAST(null AS BIGINT) as ca_b_id,
        CAST(null AS VARCHAR) as ca_name
    from {{ ref('batch2_customer') }}
    where cdc_flag in ('I', 'U')
),
batch3_customers as (
    select
        CAST(cdc_dsn AS TIMESTAMP) as action_ts,
        case cdc_flag when 'I' then 'NEW' when 'U' then 'UPDCUST' end as action_type,
        CAST(customerid AS BIGINT) as c_id,
        taxid as c_tax_id,
        gender as c_gndr,
        CAST(tier AS INTEGER) as c_tier,
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
        CONCAT_WS('-', c_ctry_1, c_area_1, c_local_1, c_ext_1) as c_phone_1,
        CONCAT_WS('-', c_ctry_2, c_area_2, c_local_2, c_ext_2) as c_phone_2,
        CONCAT_WS('-', c_ctry_3, c_area_3, c_local_3, c_ext_3) as c_phone_3,
        lcl_tx_id as c_lcl_tx_id,
        nat_tx_id as c_nat_tx_id,
        CAST(null AS BIGINT) as ca_id,
        CAST(null AS INTEGER) as ca_tax_st,
        CAST(null AS BIGINT) as ca_b_id,
        CAST(null AS VARCHAR) as ca_name
    from {{ ref('batch3_customer') }}
    where cdc_flag in ('I', 'U')
),
-- Batch2/3 account files
batch2_accounts as (
    select
        CAST(cdc_dsn AS TIMESTAMP) as action_ts,
        case cdc_flag when 'I' then 'ADDACCT' when 'U' then 'UPDACCT' end as action_type,
        CAST(ca_c_id AS BIGINT) as c_id,
        CAST(null AS VARCHAR) as c_tax_id,
        CAST(null AS VARCHAR) as c_gndr,
        CAST(null AS INTEGER) as c_tier,
        CAST(null AS DATE) as c_dob,
        CAST(null AS VARCHAR) as c_l_name,
        CAST(null AS VARCHAR) as c_f_name,
        CAST(null AS VARCHAR) as c_m_name,
        CAST(null AS VARCHAR) as c_adline1,
        CAST(null AS VARCHAR) as c_adline2,
        CAST(null AS VARCHAR) as c_zipcode,
        CAST(null AS VARCHAR) as c_city,
        CAST(null AS VARCHAR) as c_state_prov,
        CAST(null AS VARCHAR) as c_ctry,
        CAST(null AS VARCHAR) as c_prim_email,
        CAST(null AS VARCHAR) as c_alt_email,
        CAST(null AS VARCHAR) as c_phone_1,
        CAST(null AS VARCHAR) as c_phone_2,
        CAST(null AS VARCHAR) as c_phone_3,
        CAST(null AS VARCHAR) as c_lcl_tx_id,
        CAST(null AS VARCHAR) as c_nat_tx_id,
        CAST(accountid AS BIGINT) as ca_id,
        CAST(taxstatus AS INTEGER) as ca_tax_st,
        ca_b_id as ca_b_id,
        accountdesc as ca_name
    from {{ ref('batch2_account') }}
    where cdc_flag in ('I', 'U')
),
batch3_accounts as (
    select
        CAST(cdc_dsn AS TIMESTAMP) as action_ts,
        case cdc_flag when 'I' then 'ADDACCT' when 'U' then 'UPDACCT' end as action_type,
        CAST(ca_c_id AS BIGINT) as c_id,
        CAST(null AS VARCHAR) as c_tax_id,
        CAST(null AS VARCHAR) as c_gndr,
        CAST(null AS INTEGER) as c_tier,
        CAST(null AS DATE) as c_dob,
        CAST(null AS VARCHAR) as c_l_name,
        CAST(null AS VARCHAR) as c_f_name,
        CAST(null AS VARCHAR) as c_m_name,
        CAST(null AS VARCHAR) as c_adline1,
        CAST(null AS VARCHAR) as c_adline2,
        CAST(null AS VARCHAR) as c_zipcode,
        CAST(null AS VARCHAR) as c_city,
        CAST(null AS VARCHAR) as c_state_prov,
        CAST(null AS VARCHAR) as c_ctry,
        CAST(null AS VARCHAR) as c_prim_email,
        CAST(null AS VARCHAR) as c_alt_email,
        CAST(null AS VARCHAR) as c_phone_1,
        CAST(null AS VARCHAR) as c_phone_2,
        CAST(null AS VARCHAR) as c_phone_3,
        CAST(null AS VARCHAR) as c_lcl_tx_id,
        CAST(null AS VARCHAR) as c_nat_tx_id,
        CAST(accountid AS BIGINT) as ca_id,
        CAST(taxstatus AS INTEGER) as ca_tax_st,
        ca_b_id as ca_b_id,
        accountdesc as ca_name
    from {{ ref('batch3_account') }}
    where cdc_flag in ('I', 'U')
)

select * from batch1_xml
union all
select * from batch2_customers
union all
select * from batch3_customers
union all
select * from batch2_accounts
union all
select * from batch3_accounts
