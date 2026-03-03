-- Active Headcount Over Time
-- Counts employees active on each day based on hire_date and term_date

SELECT
    d::date AS date,
    COUNT(e.client_employee_id) AS active_headcount
FROM generate_series(
        (SELECT MIN(hire_date) FROM employee),
        CURRENT_DATE,
        INTERVAL '1 day'
     ) d
LEFT JOIN employee e
    ON d::date BETWEEN e.hire_date
    AND COALESCE(e.term_date, CURRENT_DATE)
GROUP BY d
ORDER BY d;