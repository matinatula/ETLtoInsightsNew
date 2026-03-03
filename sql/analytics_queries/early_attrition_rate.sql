-- Early Attrition Rate (within 90 days)

SELECT
    COUNT(*) FILTER (
        WHERE term_date IS NOT NULL
        AND term_date <= hire_date + INTERVAL '90 days'
    ) * 1.0 / COUNT(*) AS early_attrition_rate
FROM employee;