# src/etl/transform_silver.py

# Transform raw to clean tables (Silver)
# staging_employee -> employee
# staging_timesheet -> timesheet

"""
Transform Silver Layer (ETL step)

- Extract: staging_employee / staging_timesheet
- Transform: clean, deduplicate, convert types
- Load: employee / timesheet tables in ETL DB
"""

import pandas as pd
import logging
from etl.utils import get_engine

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)

engine = get_engine()  # ETL DB connection

# Employee Transform Function


def transform_employee():
    """
    Transform staging_employee → employee table (Silver layer)
    """
    try:
        df = pd.read_sql("SELECT * FROM staging_employee", engine)
        if df.empty:
            logging.warning("staging_employee is empty")
            return

        # Trim all string columns
        for col in df.select_dtypes(include="object"):
            df[col] = df[col].astype(str).str.strip()

        # Drop duplicates
        df.drop_duplicates(subset="client_employee_id", inplace=True)

        # Drop columns/rows with all NULL
        df.dropna(axis=1, how='all', inplace=True)
        df.dropna(axis=0, how='all', inplace=True)

        # Ensure client_employee_id is not null
        df = df[df['client_employee_id'].notna()]

        # Convert dates
        date_cols = ['dob', 'hire_date', 'recent_hire_date',
                     'anniversary_date', 'term_date', 'job_start_date']
        for col in date_cols:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], errors='coerce')

        # Convert numeric columns
        num_cols = ['years_of_experience',
                    'scheduled_weekly_hour', 'active_status']
        for col in num_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')

        # Load into Silver table
        df.to_sql("employee", engine, if_exists="replace", index=False)
        logging.info(
            f"Transformed employee data loaded -> employee ({len(df)} rows)")

    except Exception as e:
        logging.error(f"Failed to transform employee: {e}")



# Timesheet Transform Function

def transform_timesheet():
    """
    Transform staging_timesheet → timesheet table (Silver layer)
    """
    try:
        df = pd.read_sql("SELECT * FROM staging_timesheet", engine)
        if df.empty:
            logging.warning("staging_timesheet is empty")
            return

        # Trim string columns
        for col in df.select_dtypes(include="object"):
            df[col] = df[col].astype(str).str.strip()

        # Drop duplicates
        df.drop_duplicates(inplace=True)

        # Drop columns/rows with all NULL
        df.dropna(axis=1, how='all', inplace=True)
        df.dropna(axis=0, how='all', inplace=True)

        # Ensure client_employee_id is not null
        df = df[df['client_employee_id'].notna()]

        # Convert dates/timestamps
        date_cols = ['punch_apply_date', 'punch_in_datetime', 'punch_out_datetime',
                     'scheduled_start_datetime', 'scheduled_end_datetime']
        for col in date_cols:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], errors='coerce')

        # Convert numeric columns
        num_cols = ['hours_worked', 'scheduled_weekly_hour']
        for col in num_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')

        # Load into Silver table
        df.to_sql("timesheet", engine, if_exists="replace", index=False)
        logging.info(
            f"Transformed timesheet data loaded → timesheet ({len(df)} rows)")

    except Exception as e:
        logging.error(f"Failed to transform timesheet: {e}")
