# Derived table (timesheet_derived)
# timesheet -> timesheet_derived

"""
- Read the Silver timesheet table
- Compute derived columns: late_flag, early_departure_flag, overtime_flag
- Write to persisted derived table: timesheet_derived
- Fully Airflow-ready and idempotent
- Logs every step
"""

# src/etl/derived_gold.py
from venv import logger

from etl.db import get_engine
import pandas as pd
from airflow.utils.log.logging_mixin import LoggingMixin

log = LoggingMixin().log
engine = get_engine()


def derive_timesheet_metrics():
    try:
        df = pd.read_sql("""
            SELECT t.*, e.scheduled_weekly_hour
            FROM timesheet t
            LEFT JOIN employee e
                ON t.client_employee_id = e.client_employee_id
        """, engine)

        if df.empty:
            log.warning("Silver timesheet table is empty")
            return

        log.info(f"Loaded {len(df)} rows with columns: {df.columns.tolist()}")

        df['late_flag'] = (
            df['scheduled_start_datetime'].notna() &
            (df['punch_in_datetime'] > df['scheduled_start_datetime'])
        ).astype(int)

        df['early_departure_flag'] = (
            df['scheduled_end_datetime'].notna() &
            (df['punch_out_datetime'] < df['scheduled_end_datetime'])
        ).astype(int)

        df['overtime_flag'] = (
            df['scheduled_weekly_hour'].notna() &
            (df['hours_worked'] > df['scheduled_weekly_hour'] / 5)
        ).astype(int)

        df.to_sql("timesheet_derived", engine,
                  if_exists="replace", index=False)
        log.info(
            f"Derived timesheet metrics loaded -> timesheet_derived ({len(df)} rows)")
    except Exception as e:
        log.exception(f"Failed to derive timesheet metrics: {e}")
        logger.error(e)
        raise


def run_all():
    derive_timesheet_metrics()
