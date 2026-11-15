SELECT
  {{ dbt_utils.generate_surrogate_key(['company_id', 'effective_timestamp']) }} AS sk_company_id,
  company_id,
  status,
  name,
  industry,
  ceo,
  address_line1,
  address_line2,
  postal_code,
  city,
  state_province,
  country,
  description,
  founding_date,
  sp_rating,
  CASE
    WHEN sp_rating IN ('BB', 'B', 'CCC', 'CC', 'C', 'D', 'BB+', 'B+', 'CCC+', 'BB-', 'B-', 'CCC-')
    THEN TRUE
    ELSE FALSE
  END AS is_lowgrade,
  effective_timestamp,
  end_timestamp,
  is_current
FROM {{ ref("companies") }}
