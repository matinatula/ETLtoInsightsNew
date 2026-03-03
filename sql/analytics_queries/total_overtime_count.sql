-- Overtime Count

SELECT
    client_employee_id,
    COUNT(*) AS overtime_days
FROM timesheet_derived
WHERE overtime_flag = 1
GROUP BY client_employee_id
ORDER BY overtime_days DESC;