-- Late Arrival Frequency

SELECT
    client_employee_id,
    COUNT(*) AS late_arrivals
FROM timesheet_derived
WHERE late_flag = 1
GROUP BY client_employee_id
ORDER BY late_arrivals DESC;