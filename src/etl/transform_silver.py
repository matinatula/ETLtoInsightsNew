# Transform raw to clean tables (Silver)
# staging_employee -> employee
# staging_timesheet -> timesheet

"""
Transform Silver Layer (ETL step)

- Extract: staging_employee / staging_timesheet
- Transform: clean, deduplicate, convert types
- Load: employee / timesheet tables in ETL DB
"""

# src/etl/transform_silver.py
import pandas as pd
from etl.utils import get_engine
from airflow.utils.log.logging_mixin import LoggingMixin

log = LoggingMixin().log
engine = get_engine()  # ETL DB connection


def transform_employee():
    try:
        df = pd.read_sql("SELECT * FROM staging_employee", engine)
        if df.empty:
            log.warning("staging_employee is empty")
            return

        for col in df.select_dtypes(include="object"):
            df[col] = df[col].astype(str).str.strip()

        df.drop_duplicates(subset="client_employee_id", inplace=True)
        df.dropna(axis=1, how='all', inplace=True)
        df.dropna(axis=0, how='all', inplace=True)
        df = df[df['client_employee_id'].notna()]

        date_cols = ['dob', 'hire_date', 'recent_hire_date',
                     'anniversary_date', 'term_date', 'job_start_date']
        for col in date_cols:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], errors='coerce')

        num_cols = ['years_of_experience',
                    'scheduled_weekly_hour', 'active_status']
        for col in num_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')

        df.to_sql("employee", engine, if_exists="replace", index=False)
        log.info(f"Transformed employee loaded -> employee ({len(df)} rows)")
    except Exception as e:
        log.exception(f"Failed to transform employee: {e}")


def transform_timesheet():
    try:
        df = pd.read_sql("SELECT * FROM staging_timesheet", engine)
        if df.empty:
            log.warning("staging_timesheet is empty")
            return

        for col in df.select_dtypes(include="object"):
            df[col] = df[col].astype(str).str.strip()

        df.drop_duplicates(inplace=True)
        df.dropna(axis=1, how='all', inplace=True)
        df.dropna(axis=0, how='all', inplace=True)
        df = df[df['client_employee_id'].notna()]

        date_cols = ['punch_apply_date', 'punch_in_datetime', 'punch_out_datetime',
                     'scheduled_start_datetime', 'scheduled_end_datetime']
        for col in date_cols:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], errors='coerce')

        num_cols = ['hours_worked', 'scheduled_weekly_hour']
        for col in num_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')

        df.to_sql("timesheet", engine, if_exists="replace", index=False)
        log.info(f"Transformed timesheet loaded -> timesheet ({len(df)} rows)")
    except Exception as e:
        log.exception(f"Failed to transform timesheet: {e}")
