"""
Transform Silver Layer (ETL step)

- Extract: staging_employee / staging_timesheet
- Transform: clean, deduplicate, convert types
- Load: employee / timesheet tables in ETL DB
"""

# src/etl/transform_silver.py
"""
Transform Silver Layer (ETL step)

- staging_employee -> employee
- staging_timesheet -> timesheet
- Preserves schema and uses chunks for large datasets
"""

from etl.db import get_engine
import pandas as pd
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)

engine = get_engine()  # ETL DB connection

CHUNKSIZE = 5000  # rows per chunk


def transform_employee():
    """
    Transform staging_employee → employee table (Silver layer)
    """
    try:
        # Read full staging table
        query = "SELECT * FROM staging_employee"
        df_iter = pd.read_sql(query, engine, chunksize=CHUNKSIZE)

        # Truncate target table first (preserves schema)
        with engine.begin() as conn:
            conn.execute("TRUNCATE TABLE employee RESTART IDENTITY CASCADE")

        total_rows = 0
        for chunk in df_iter:
            # Trim all string columns
            for col in chunk.select_dtypes(include="object"):
                chunk[col] = chunk[col].astype(str).str.strip()

            # Drop duplicates
            chunk.drop_duplicates(subset="client_employee_id", inplace=True)

            # Drop columns/rows with all NULL
            chunk.dropna(axis=1, how='all', inplace=True)
            chunk.dropna(axis=0, how='all', inplace=True)

            # Ensure client_employee_id is not null
            chunk = chunk[chunk['client_employee_id'].notna()]

            # Convert dates
            date_cols = ['dob', 'hire_date', 'recent_hire_date',
                         'anniversary_date', 'term_date', 'job_start_date']
            for col in date_cols:
                if col in chunk.columns:
                    chunk[col] = pd.to_datetime(chunk[col], errors='coerce')

            # Convert numeric columns
            num_cols = ['years_of_experience',
                        'scheduled_weekly_hour', 'active_status', 'job_code']
            for col in num_cols:
                if col in chunk.columns:
                    chunk[col] = pd.to_numeric(chunk[col], errors='coerce')

            # Append to Silver table
            chunk.to_sql("employee", engine, if_exists="append", index=False)
            total_rows += len(chunk)
            logging.info(f"Processed {len(chunk)} rows for employee")

        logging.info(f"Total rows loaded into employee: {total_rows}")

    except Exception as e:
        logging.error(f"Failed to transform employee: {e}")


def transform_timesheet():
    """
    Transform staging_timesheet -> timesheet table (Silver layer)
    """
    try:
        query = "SELECT * FROM staging_timesheet"
        df_iter = pd.read_sql(query, engine, chunksize=CHUNKSIZE)

        # Load valid employee IDs once
        employee_df = pd.read_sql(
            "SELECT client_employee_id FROM employee",
            engine
        )
        valid_employee_ids = set(
            employee_df["client_employee_id"].astype(str)
        )

        with engine.begin() as conn:
            conn.execute("TRUNCATE TABLE timesheet RESTART IDENTITY CASCADE")

        total_inserted = 0
        total_skipped = 0

        for chunk in df_iter:

            # Trim string columns
            for col in chunk.select_dtypes(include="object"):
                chunk[col] = chunk[col].astype(str).str.strip()

            # Drop duplicates
            chunk.drop_duplicates(inplace=True)

            # Drop empty rows/columns
            chunk.dropna(axis=1, how='all', inplace=True)
            chunk.dropna(axis=0, how='all', inplace=True)

            # Ensure employee id not null
            chunk = chunk[chunk['client_employee_id'].notna()]

            # Convert dates
            date_cols = [
                'punch_apply_date',
                'punch_in_datetime',
                'punch_out_datetime',
                'scheduled_start_datetime',
                'scheduled_end_datetime'
            ]
            for col in date_cols:
                if col in chunk.columns:
                    chunk[col] = pd.to_datetime(chunk[col], errors='coerce')

            # Convert numeric
            if 'hours_worked' in chunk.columns:
                chunk['hours_worked'] = pd.to_numeric(
                    chunk['hours_worked'],
                    errors='coerce'
                )

            # FILTER INVALID EMPLOYEES (THIS FIXES FK ERROR)
            before_filter = len(chunk)

            chunk = chunk[
                chunk["client_employee_id"].astype(
                    str).isin(valid_employee_ids)
            ]

            skipped = before_filter - len(chunk)
            total_skipped += skipped

            if len(chunk) == 0:
                continue

            # Insert valid rows
            chunk.to_sql(
                "timesheet",
                engine,
                if_exists="append",
                index=False,
                method="multi"
            )

            total_inserted += len(chunk)
            logging.info(f"Inserted {len(chunk)} rows")

        logging.info(f"Total inserted: {total_inserted}")
        logging.warning(f"Total skipped (missing employee): {total_skipped}")

    except Exception as e:
        logging.error(f"Failed to transform timesheet: {e}")
        raise  # IMPORTANT → makes Airflow fail properly
