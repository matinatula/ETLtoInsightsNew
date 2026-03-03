-- attendance_irregularity_score.sql
SELECT
    client_employee_id,
    SUM(late_flag) + SUM(early_departure_flag) AS attendance_irregularities
FROM timesheet_derived
GROUP BY client_employee_id
ORDER BY attendance_irregularities DESC;