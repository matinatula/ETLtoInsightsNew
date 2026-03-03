-- Monthly Turnover Trend

SELECT
    DATE_TRUNC('month', term_date) AS month,
    COUNT(*) AS terminations
FROM employee
WHERE term_date IS NOT NULL
GROUP BY month
ORDER BY month;