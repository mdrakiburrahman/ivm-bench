select
    {{ dbt_utils.generate_surrogate_key(['employee_id']) }} as sk_broker_id,
    employee_id as broker_id,
    manager_id,
    first_name,
    last_name,
    middle_initial,
    job_code,
    branch,
    office,
    phone
from
    {{ ref('employees') }}
