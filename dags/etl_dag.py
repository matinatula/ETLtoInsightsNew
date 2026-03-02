# dags/etl_dag.py

"""
ETL Pipeline DAG (Medallion Architecture)

Layers:
- Bronze: extract_employee + extract_timesheets
- Silver: transform_employee + transform_timesheet
- Gold: derive_gold (timesheet_derived)
"""

import sys
import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from etl.extract_bronze import extract_employee, extract_timesheets
from etl.transform_silver import transform_employee, transform_timesheet
from etl.derived_gold import run_all as derive_gold

# Add src to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../src'))

# Default args
default_args = {
    "owner": "etl_user",
    "depends_on_past": False,
    "start_date": datetime(2026, 3, 3),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


# DAG definition
with DAG(
    dag_id="etl_pipeline",
    default_args=default_args,
    schedule_interval=None,  # Manual trigger
    catchup=False,
    tags=["ETL", "Medallion"]
) as dag:

    # -----------------------------
    # Bronze Layer: Extract
    # -----------------------------
    extract_employee_task = PythonOperator(
        task_id="extract_employee",
        python_callable=extract_employee,
        op_kwargs={"file_path": os.getenv("EMPLOYEE_CSV")}
    )

    extract_timesheets_task = PythonOperator(
        task_id="extract_timesheets",
        python_callable=extract_timesheets,
        op_kwargs={"folder_path": os.getenv("TIMESHEETS_FOLDER")}
    )

    # -----------------------------
    # Silver Layer: Transform
    # -----------------------------
    transform_employee_task = PythonOperator(
        task_id="transform_employee",
        python_callable=transform_employee
    )

    transform_timesheet_task = PythonOperator(
        task_id="transform_timesheet",
        python_callable=transform_timesheet
    )

    # -----------------------------
    # Gold Layer: Derived
    # -----------------------------
    derive_gold_task = PythonOperator(
        task_id="derive_gold",
        python_callable=derive_gold
    )

    # -----------------------------
    # Optional: test logging
    # -----------------------------
    def test_logging():
        import logging
        logger = logging.getLogger(__name__)
        logger.info("Test log: Airflow logger works!")
        print("Test print: Airflow print works!")

    test_logging_task = PythonOperator(
        task_id="test_logging",
        python_callable=test_logging
    )

    # -----------------------------
    # DAG Dependencies
    # -----------------------------

    # Test logging first (optional)
    test_logging_task >> [extract_employee_task, extract_timesheets_task]

    # Extract → Transform
    extract_employee_task >> transform_employee_task
    extract_timesheets_task >> transform_timesheet_task

    # Transform → Gold
    [transform_employee_task, transform_timesheet_task] >> derive_gold_task
