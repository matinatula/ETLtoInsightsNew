-- 003_indexes.sql
-- Add indexes for performance
CREATE INDEX IF NOT EXISTS idx_timesheet_employee
ON timesheet(client_employee_id);

CREATE INDEX IF NOT EXISTS idx_timesheet_date
ON timesheet(punch_apply_date);

CREATE INDEX IF NOT EXISTS idx_employee_department
ON employee(department_id);