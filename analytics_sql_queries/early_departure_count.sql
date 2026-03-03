-- Early Departure Count

SELECT
    client_employee_id,
    COUNT(*) AS early_departures
FROM timesheet_derived
WHERE early_departure_flag = 1
GROUP BY client_employee_id
ORDER BY early_departures DESC;