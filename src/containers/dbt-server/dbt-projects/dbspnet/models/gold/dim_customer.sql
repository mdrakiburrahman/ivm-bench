-- Gold: dim_customer with forward-fill and prospect enrichment
-- Forward-fill uses cumulative grouping pattern (MAX of non-null group + MAX value per group)
-- because Feldera does not support IGNORE NULLS in LAST_VALUE/FIRST_VALUE.

with s1 as (
    select c.*,
           p.agency_id,
           p.credit_rating,
           p.net_worth
    from {{ ref('customers') }} c
    left join {{ ref('syndicated_prospect') }} p
        on c.first_name = p.first_name
        and c.last_name = p.last_name
        and c.postal_code = p.postal_code
        and c.address_line1 = p.address_line1
        and coalesce(c.address_line2, '') = coalesce(p.address_line2, '')
),
-- Assign group numbers: each non-null value starts a new group
s2_groups as (
    select s1.*,
        COUNT(tax_id) OVER w as tax_id_grp,
        COUNT(last_name) OVER w as last_name_grp,
        COUNT(first_name) OVER w as first_name_grp,
        COUNT(middle_name) OVER w as middle_name_grp,
        COUNT(gender) OVER w as gender_grp,
        COUNT(tier) OVER w as tier_grp,
        COUNT(dob) OVER w as dob_grp,
        COUNT(address_line1) OVER w as address_line1_grp,
        COUNT(address_line2) OVER w as address_line2_grp,
        COUNT(postal_code) OVER w as postal_code_grp,
        COUNT(city) OVER w as city_grp,
        COUNT(state_province) OVER w as state_province_grp,
        COUNT(country) OVER w as country_grp,
        COUNT(phone1) OVER w as phone1_grp,
        COUNT(phone2) OVER w as phone2_grp,
        COUNT(phone3) OVER w as phone3_grp,
        COUNT(primary_email) OVER w as primary_email_grp,
        COUNT(alternate_email) OVER w as alternate_email_grp,
        COUNT(local_tax_rate_name) OVER w as local_tax_rate_name_grp,
        COUNT(local_tax_rate) OVER w as local_tax_rate_grp,
        COUNT(national_tax_rate_name) OVER w as national_tax_rate_name_grp,
        COUNT(national_tax_rate) OVER w as national_tax_rate_grp
    from s1
    WINDOW w AS (PARTITION BY customer_id ORDER BY effective_timestamp)
),
-- Forward-fill: MAX within each group carries the last non-null value forward
s3 as (
    select
        {{ dbt_utils.generate_surrogate_key(['customer_id', 'effective_timestamp']) }} as sk_customer_id,
        customer_id,
        coalesce(tax_id, MAX(tax_id) OVER (PARTITION BY customer_id, tax_id_grp)) as tax_id,
        status,
        coalesce(last_name, MAX(last_name) OVER (PARTITION BY customer_id, last_name_grp)) as last_name,
        coalesce(first_name, MAX(first_name) OVER (PARTITION BY customer_id, first_name_grp)) as first_name,
        coalesce(middle_name, MAX(middle_name) OVER (PARTITION BY customer_id, middle_name_grp)) as middleinitial,
        coalesce(gender, MAX(gender) OVER (PARTITION BY customer_id, gender_grp)) as gender,
        coalesce(tier, MAX(tier) OVER (PARTITION BY customer_id, tier_grp)) as tier,
        coalesce(dob, MAX(dob) OVER (PARTITION BY customer_id, dob_grp)) as dob,
        coalesce(address_line1, MAX(address_line1) OVER (PARTITION BY customer_id, address_line1_grp)) as address_line1,
        coalesce(address_line2, MAX(address_line2) OVER (PARTITION BY customer_id, address_line2_grp)) as address_line2,
        coalesce(postal_code, MAX(postal_code) OVER (PARTITION BY customer_id, postal_code_grp)) as postal_code,
        coalesce(city, MAX(city) OVER (PARTITION BY customer_id, city_grp)) as city,
        coalesce(state_province, MAX(state_province) OVER (PARTITION BY customer_id, state_province_grp)) as state_province,
        coalesce(country, MAX(country) OVER (PARTITION BY customer_id, country_grp)) as country,
        coalesce(phone1, MAX(phone1) OVER (PARTITION BY customer_id, phone1_grp)) as phone1,
        coalesce(phone2, MAX(phone2) OVER (PARTITION BY customer_id, phone2_grp)) as phone2,
        coalesce(phone3, MAX(phone3) OVER (PARTITION BY customer_id, phone3_grp)) as phone3,
        coalesce(primary_email, MAX(primary_email) OVER (PARTITION BY customer_id, primary_email_grp)) as primary_email,
        coalesce(alternate_email, MAX(alternate_email) OVER (PARTITION BY customer_id, alternate_email_grp)) as alternate_email,
        coalesce(local_tax_rate_name, MAX(local_tax_rate_name) OVER (PARTITION BY customer_id, local_tax_rate_name_grp)) as local_tax_rate_name,
        coalesce(local_tax_rate, MAX(local_tax_rate) OVER (PARTITION BY customer_id, local_tax_rate_grp)) as local_tax_rate,
        coalesce(national_tax_rate_name, MAX(national_tax_rate_name) OVER (PARTITION BY customer_id, national_tax_rate_name_grp)) as national_tax_rate_name,
        coalesce(national_tax_rate, MAX(national_tax_rate) OVER (PARTITION BY customer_id, national_tax_rate_grp)) as national_tax_rate,
        agency_id,
        credit_rating,
        net_worth,
        effective_timestamp,
        end_timestamp,
        is_current
    from s2_groups
)
select *
from s3
