-- catalog-override: Materialize does not support LAST_VALUE IGNORE NULLS; filtered window arrays preserve the ordered forward-fill.
with s1 as (
    select c.*, p.agency_id, p.credit_rating, p.net_worth
    from {{ ref('customers') }} c
    left join {{ ref('syndicated_prospect') }} p
        on c.first_name = p.first_name
        and c.last_name = p.last_name
        and c.postal_code = p.postal_code
        and c.address_line1 = p.address_line1
        and coalesce(c.address_line2, '') = coalesce(p.address_line2, '')
),
filled as (
    select s1.*,
        {{ materialize_forward_fill('tax_id') }} as tax_id_values,
        {{ materialize_forward_fill('last_name') }} as last_name_values,
        {{ materialize_forward_fill('first_name') }} as first_name_values,
        {{ materialize_forward_fill('middle_name') }} as middle_name_values,
        {{ materialize_forward_fill('gender') }} as gender_values,
        {{ materialize_forward_fill('tier') }} as tier_values,
        {{ materialize_forward_fill('dob') }} as dob_values,
        {{ materialize_forward_fill('address_line1') }} as address_line1_values,
        {{ materialize_forward_fill('address_line2') }} as address_line2_values,
        {{ materialize_forward_fill('postal_code') }} as postal_code_values,
        {{ materialize_forward_fill('city') }} as city_values,
        {{ materialize_forward_fill('state_province') }} as state_province_values,
        {{ materialize_forward_fill('country') }} as country_values,
        {{ materialize_forward_fill('phone1') }} as phone1_values,
        {{ materialize_forward_fill('phone2') }} as phone2_values,
        {{ materialize_forward_fill('phone3') }} as phone3_values,
        {{ materialize_forward_fill('primary_email') }} as primary_email_values,
        {{ materialize_forward_fill('alternate_email') }} as alternate_email_values,
        {{ materialize_forward_fill('local_tax_rate_name') }} as local_tax_rate_name_values,
        {{ materialize_forward_fill('local_tax_rate') }} as local_tax_rate_values,
        {{ materialize_forward_fill('national_tax_rate_name') }} as national_tax_rate_name_values,
        {{ materialize_forward_fill('national_tax_rate') }} as national_tax_rate_values
    from s1
)
select
    {{ dbt_utils.generate_surrogate_key(['customer_id', "(effective_timestamp at time zone 'UTC')"]) }} as sk_customer_id,
    customer_id,
    coalesce(tax_id, tax_id_values[array_length(tax_id_values, 1)]) as tax_id,
    status,
    coalesce(last_name, last_name_values[array_length(last_name_values, 1)]) as last_name,
    coalesce(first_name, first_name_values[array_length(first_name_values, 1)]) as first_name,
    coalesce(middle_name, middle_name_values[array_length(middle_name_values, 1)]) as middleinitial,
    coalesce(gender, gender_values[array_length(gender_values, 1)]) as gender,
    coalesce(tier, tier_values[array_length(tier_values, 1)]) as tier,
    coalesce(dob, dob_values[array_length(dob_values, 1)]) as dob,
    coalesce(address_line1, address_line1_values[array_length(address_line1_values, 1)]) as address_line1,
    coalesce(address_line2, address_line2_values[array_length(address_line2_values, 1)]) as address_line2,
    coalesce(postal_code, postal_code_values[array_length(postal_code_values, 1)]) as postal_code,
    coalesce(city, city_values[array_length(city_values, 1)]) as city,
    coalesce(state_province, state_province_values[array_length(state_province_values, 1)]) as state_province,
    coalesce(country, country_values[array_length(country_values, 1)]) as country,
    coalesce(phone1, phone1_values[array_length(phone1_values, 1)]) as phone1,
    coalesce(phone2, phone2_values[array_length(phone2_values, 1)]) as phone2,
    coalesce(phone3, phone3_values[array_length(phone3_values, 1)]) as phone3,
    coalesce(primary_email, primary_email_values[array_length(primary_email_values, 1)]) as primary_email,
    coalesce(alternate_email, alternate_email_values[array_length(alternate_email_values, 1)]) as alternate_email,
    coalesce(local_tax_rate_name, local_tax_rate_name_values[array_length(local_tax_rate_name_values, 1)]) as local_tax_rate_name,
    coalesce(local_tax_rate, local_tax_rate_values[array_length(local_tax_rate_values, 1)]) as local_tax_rate,
    coalesce(national_tax_rate_name, national_tax_rate_name_values[array_length(national_tax_rate_name_values, 1)]) as national_tax_rate_name,
    coalesce(national_tax_rate, national_tax_rate_values[array_length(national_tax_rate_values, 1)]) as national_tax_rate,
    agency_id, credit_rating, net_worth, effective_timestamp, end_timestamp, is_current
from filled
