# src/etl/extract_bronze.py

# Extract raw CSVs -> staging tables (Bronze)
# CSV -> staging_employee / staging_timesheet

import os
import glob
import pandas as pd
import logging
from etl.utils import get_engine
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)

# File paths from .env (with defaults)
EMPLOYEE_CSV = os.environ["EMPLOYEE_CSV"]
TIMESHEETS_FOLDER = os.environ["TIMESHEETS_FOLDER"]

engine = get_engine()  # ETL DB connection


def extract_employee(file_path=EMPLOYEE_CSV):
    """
    Extract employee CSV into staging_employee table (Bronze layer)
    """
    if not os.path.exists(file_path):
        logging.error(f"Employee file not found: {file_path}")
        return

    try:
        df = pd.read_csv(file_path)
        df.to_sql("staging_employee", engine, if_exists="replace", index=False)
        logging.info(
            f"Extracted employee data -> staging_employee ({len(df)} rows)")
    except Exception as e:
        logging.error(f"Failed to extract employee: {e}")


def extract_timesheets(folder_path=TIMESHEETS_FOLDER):
    """
    Extract all timesheet CSVs from folder -> staging_timesheet table (Bronze layer)
    """
    if not os.path.exists(folder_path):
        logging.error(f"Timesheet folder not found: {folder_path}")
        return

    all_files = glob.glob(os.path.join(folder_path, "*.csv"))
    if not all_files:
        logging.warning(f"No CSV files found in {folder_path}")
        return

    total_rows = 0
    for file in all_files:
        try:
            df = pd.read_csv(file)
            df.to_sql("staging_timesheet", engine,
                      if_exists="append", index=False)
            total_rows += len(df)
            logging.info(f"Extracted {len(df)} rows from {file}")
        except Exception as e:
            logging.error(f"Failed to extract {file}: {e}")

    logging.info(f"Total rows loaded into staging_timesheet: {total_rows}")
