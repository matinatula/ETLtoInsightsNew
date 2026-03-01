-- 002_create_timesheet.sql
-- Timesheet table (fact table)
CREATE TABLE IF NOT EXISTS timesheet (
    timesheet_id BIGSERIAL PRIMARY KEY,
    client_employee_id VARCHAR(50) NOT NULL,
    department_id VARCHAR(100),
    department_name VARCHAR(255),
    home_department_id VARCHAR(100),
    home_department_name VARCHAR(255),
    pay_code VARCHAR(100),
    punch_in_comment VARCHAR(255),
    punch_out_comment VARCHAR(255),
    hours_worked NUMERIC CHECK (hours_worked >= 0),
    punch_apply_date DATE NOT NULL,
    punch_in_datetime TIMESTAMP NOT NULL,
    punch_out_datetime TIMESTAMP NOT NULL,
    scheduled_start_datetime TIMESTAMP,
    scheduled_end_datetime TIMESTAMP,
    CONSTRAINT fk_employee
        FOREIGN KEY (client_employee_id)
        REFERENCES employee(client_employee_id)
        ON DELETE CASCADE
);