# src/etl/extract_bronze.py
"""
Extract raw CSVs -> staging tables (Bronze layer)
- CSV -> staging_employee / staging_timesheet
- Preserves table types (use append after TRUNCATE)
"""
# src/etl/extract_bronze.py
"""
Extract raw CSVs -> staging tables (Bronze)
Supports large files via chunksize
"""

import glob
import os
import pandas as pd
from etl.db import get_engine
from airflow.utils.log.logging_mixin import LoggingMixin
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

log = LoggingMixin().log  # Airflow logger

# File paths from .env
EMPLOYEE_CSV = os.environ.get("EMPLOYEE_CSV")
TIMESHEETS_FOLDER = os.environ.get("TIMESHEETS_FOLDER")

engine = get_engine()  # ETL DB connection

CHUNKSIZE = 5000  # rows per chunk


def extract_employee(file_path=EMPLOYEE_CSV):
    """
    Extract employee CSV into staging_employee table (Bronze layer)
    """
    log.info(f"Starting employee extraction from: {file_path}")

    if not os.path.exists(file_path):
        log.error(f"Employee file not found: {file_path}")
        return

    try:
        # Truncate staging table first
        with engine.begin() as conn:
            conn.execute("TRUNCATE TABLE staging_employee RESTART IDENTITY")

        total_rows = 0
        for chunk in pd.read_csv(file_path, chunksize=CHUNKSIZE, sep="|"):
            chunk.to_sql("staging_employee", engine,
                         if_exists="append", index=False)
            total_rows += len(chunk)
            log.info(f"Loaded {len(chunk)} rows to staging_employee")

        log.info(f"Total employee rows loaded: {total_rows}")

    except Exception as e:
        log.exception(f"Failed to extract employee: {e}")


def extract_timesheets(folder_path=TIMESHEETS_FOLDER):
    """
    Extract all timesheet CSVs from folder -> staging_timesheet table (Bronze layer)
    """
    log.info(f"Starting timesheet extraction from folder: {folder_path}")

    if not os.path.exists(folder_path):
        log.error(f"Timesheet folder not found: {folder_path}")
        return

    all_files = glob.glob(os.path.join(folder_path, "*.csv"))
    if not all_files:
        log.warning(f"No CSV files found in {folder_path}")
        return

    # Truncate staging table first
    with engine.begin() as conn:
        conn.execute("TRUNCATE TABLE staging_timesheet RESTART IDENTITY")

    total_rows = 0
    for file in all_files:
        try:
            for chunk in pd.read_csv(file, chunksize=CHUNKSIZE, sep="|"):
                chunk.to_sql("staging_timesheet", engine,
                             if_exists="append", index=False)
                total_rows += len(chunk)
                log.info(
                    f"Loaded {len(chunk)} rows from {file} to staging_timesheet")
        except Exception as e:
            log.exception(f"Failed to extract {file}: {e}")

    log.info(f"Total timesheet rows loaded: {total_rows}")
