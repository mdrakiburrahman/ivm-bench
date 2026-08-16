select
    employeeid as employee_id,
    managerid as manager_id,
    employeefirstname as first_name,
    employeelastname as last_name,
    employeemi as middle_initial,
    employeejobcode as job_code,
    employeebranch as branch,
    employeeoffice as office,
    employeephone as phone
from {{ ref('hr_employee') }}
