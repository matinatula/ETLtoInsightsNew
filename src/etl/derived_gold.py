# src/etl/derived_gold.py

# Derived table (timesheet_derived)
# timesheet -> timesheet_derived

"""
- Read the Silver timesheet table
- Compute derived columns: late_flag, early_departure_flag, overtime_flag
- Write to persisted derived table: timesheet_derived
- Fully Airflow-ready and idempotent
- Logs every step
"""

import logging
from etl.utils import get_engine
import pandas as pd

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)

engine = get_engine()


def derive_timesheet_metrics():
    """
    Compute derived metrics for timesheet and write to gold table
    """
    try:
        df = pd.read_sql("SELECT * FROM timesheet", engine)
        if df.empty:
            logging.warning("Silver timesheet table is empty")
            return

        # Derived Columns 

        # Late flag: 1 if punch_in after scheduled_start
        df['late_flag'] = ((df['punch_in_datetime'] > df['scheduled_start_datetime'])
                           & df['scheduled_start_datetime'].notna()).astype(int)

        # Early departure flag: 1 if punch_out before scheduled_end
        df['early_departure_flag'] = ((df['punch_out_datetime'] < df['scheduled_end_datetime'])
                                      & df['scheduled_end_datetime'].notna()).astype(int)

        # Overtime flag: 1 if hours_worked > scheduled_weekly_hour / 5
        df['overtime_flag'] = ((df['hours_worked'] > df['scheduled_weekly_hour'] / 5)
                               & df['scheduled_weekly_hour'].notna()).astype(int)

        # Persist Gold Table 

        df.to_sql("timesheet_derived", engine,
                  if_exists="replace", index=False)
        logging.info(
            f"Derived timesheet metrics loaded: timesheet_derived ({len(df)} rows)")

    except Exception as e:
        logging.error(f"Failed to derive timesheet metrics: {e}")


# Optional helper to run all Gold derivations
def run_all():
    derive_timesheet_metrics()
