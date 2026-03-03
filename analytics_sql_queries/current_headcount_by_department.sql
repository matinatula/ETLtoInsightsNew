-- Current Active Headcount by Department

SELECT
    department_id,
    department_name,
    COUNT(*) AS active_headcount
FROM employee
WHERE term_date IS NULL
GROUP BY department_id, department_name
ORDER BY active_headcount DESC;