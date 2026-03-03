-- 7-Day Rolling Average Working Hours

SELECT
    client_employee_id,
    punch_apply_date,
    AVG(hours_worked) OVER (
        PARTITION BY client_employee_id
        ORDER BY punch_apply_date
        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    ) AS rolling_avg_7day_hours
FROM timesheet
ORDER BY client_employee_id, punch_apply_date;