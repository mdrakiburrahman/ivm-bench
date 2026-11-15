SELECT
  employeeid AS employee_id,
  managerid AS manager_id,
  employeefirstname AS first_name,
  employeelastname AS last_name,
  employeemi AS middle_initial,
  employeejobcode AS job_code,
  employeebranch AS branch,
  employeeoffice AS office,
  employeephone AS phone
FROM {{ ref('hr_employee') }}
