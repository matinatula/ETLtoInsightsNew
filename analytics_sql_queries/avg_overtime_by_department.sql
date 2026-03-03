-- Average Overtime Hours by Department

SELECT
    t.department_id,
    t.department_name,
    AVG(t.hours_worked) AS avg_hours_worked
FROM timesheet_derived t
WHERE overtime_flag = 1
GROUP BY t.department_id, t.department_name
ORDER BY avg_hours_worked DESC;