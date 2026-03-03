-- Monthly Hiring Trend

SELECT
    DATE_TRUNC('month', hire_date) AS month,
    COUNT(*) AS hires
FROM employee
GROUP BY month
ORDER BY month;