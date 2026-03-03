-- Average Tenure (in days) by Department

SELECT
    department_id,
    department_name,
    AVG(COALESCE(term_date, CURRENT_DATE) - hire_date) AS avg_tenure_days
FROM employee
GROUP BY department_id, department_name
ORDER BY avg_tenure_days DESC;