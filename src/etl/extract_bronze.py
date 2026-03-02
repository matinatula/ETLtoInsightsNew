# src/etl/extract_bronze.py

# Extract raw CSVs -> staging tables (Bronze)
# CSV -> staging_employee / staging_timesheet

# src/etl/extract_bronze.py
import os
import glob
import pandas as pd
from etl.utils import get_engine
from dotenv import load_dotenv
from airflow.utils.log.logging_mixin import LoggingMixin

# Load env
load_dotenv()
log = LoggingMixin().log

EMPLOYEE_CSV = os.environ.get("EMPLOYEE_CSV")
TIMESHEETS_FOLDER = os.environ.get("TIMESHEETS_FOLDER")

engine = get_engine()  # ETL DB connection


def extract_employee(file_path=EMPLOYEE_CSV):
    log.info(f"Starting employee extraction from: {file_path}")
    if not os.path.exists(file_path):
        log.error(f"Employee file not found: {file_path}")
        return

    try:
        df = pd.read_csv(file_path)
        df.to_sql("staging_employee", engine, if_exists="replace", index=False)
        log.info(
            f"Employee data loaded into staging_employee ({len(df)} rows)")
    except Exception as e:
        log.exception(f"Failed to extract employee: {e}")


def extract_timesheets(folder_path=TIMESHEETS_FOLDER):
    log.info(f"Starting timesheet extraction from folder: {folder_path}")
    if not os.path.exists(folder_path):
        log.error(f"Timesheet folder not found: {folder_path}")
        return

    all_files = glob.glob(os.path.join(folder_path, "*.csv"))
    if not all_files:
        log.warning(f"No CSV files found in {folder_path}")
        return

    total_rows = 0
    for file in all_files:
        try:
            df = pd.read_csv(file)
            df.to_sql("staging_timesheet", engine,
                      if_exists="append", index=False)
            total_rows += len(df)
            log.info(f"Extracted {len(df)} rows from {file}")
        except Exception as e:
            log.exception(f"Failed to extract {file}: {e}")

    log.info(f"Total rows loaded into staging_timesheet: {total_rows}")
