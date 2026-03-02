import os
import logging
import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv
from transform import transform_timesheet  # our transform module

# Load .env
load_dotenv()

DB_USER = os.environ["POSTGRES_USER"]
DB_PASSWORD = os.environ["POSTGRES_PASSWORD"]
DB_HOST = os.environ["POSTGRES_HOST"]
DB_PORT = os.environ["POSTGRES_PORT"]
DB_NAME = os.environ["POSTGRES_DB"]

# Logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)

# Create DB engine
engine = create_engine(
    f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}")


def persist_derived_timesheet(timesheet_df: pd.DataFrame):
    """
    Persist transformed timesheet with derived metrics into timesheet_derived table.
    """
    if timesheet_df.empty:
        logging.warning("No timesheet data to persist.")
        return

    # Transform the timesheet (clean + derived columns)
    df_transformed = transform_timesheet(timesheet_df)

    # Select only columns needed for derived table
    derived_cols = [
        'client_employee_id',
        'punch_apply_date',
        'late_flag',
        'early_departure_flag',
        'overtime_flag',
        'hours_worked',
        'department_id',
        'department_name',
        'scheduled_start_datetime',
        'scheduled_end_datetime'
    ]
    df_final = df_transformed[derived_cols]

    # Write to DB
    try:
        df_final.to_sql('timesheet_derived', engine,
                        if_exists='append', index=False)
        logging.info(f"Persisted {len(df_final)} rows into timesheet_derived")
    except Exception as e:
        logging.error(f"Failed to persist derived table: {e}")
