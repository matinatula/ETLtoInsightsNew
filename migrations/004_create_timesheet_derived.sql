-- 004_create_timesheet_derived.sql
CREATE TABLE IF NOT EXISTS timesheet_derived (
    timesheet_id BIGSERIAL PRIMARY KEY,
    client_employee_id VARCHAR(50) NOT NULL,
    punch_apply_date DATE NOT NULL,
    late_flag INT,
    early_departure_flag INT,
    overtime_flag INT,
    hours_worked NUMERIC,
    department_id VARCHAR(100),
    department_name VARCHAR(255),
    scheduled_start_datetime TIMESTAMP,
    scheduled_end_datetime TIMESTAMP
);