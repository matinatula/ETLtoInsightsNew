-- Average Daily Hours per Employee

SELECT
    client_employee_id,
    AVG(hours_worked) AS avg_daily_hours
FROM timesheet
GROUP BY client_employee_id
ORDER BY avg_daily_hours DESC;