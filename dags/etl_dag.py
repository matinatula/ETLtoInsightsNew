"""
DAG Highlights: 

- Bronze layer: extract_employee + extract_timesheets
- Silver layer: transform_employee + transform_timesheet
- Gold layer: derive_gold (timesheet_derived)

"""
from etl.derived_gold import run_all as derive_gold
from etl.transform_silver import transform_employee, transform_timesheet
from etl.extract_bronze import extract_employee, extract_timesheets
import sys
import os
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../src'))


# Default DAG args
default_args = {
    "owner": "etl_user",
    "depends_on_past": False,
    "start_date": datetime(2026, 3, 3),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


def test_logging():
    print("THIS PRINT WILL SHOW IN LOG")
    import logging
    logger = logging.getLogger(__name__)
    logger.info("THIS LOGGER INFO WILL ALSO SHOW IN LOG")
    raise ValueError("FORCE ERROR TO SEE RED TASK")


# DAG definition
with DAG(
    dag_id="etl_pipeline",
    default_args=default_args,
    schedule=None,  # manual or trigger-based
    catchup=False,
    tags=["ETL", "Medallion"]
) as dag:

    # Extract Bronze
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

    # Transform Silver
    transform_employee_task = PythonOperator(
        task_id="transform_employee",
        python_callable=transform_employee
    )

    transform_timesheet_task = PythonOperator(
        task_id="transform_timesheet",
        python_callable=transform_timesheet
    )

    # Gold Derived
    derive_gold_task = PythonOperator(
        task_id="derive_gold",
        python_callable=derive_gold
    )

    test_task = PythonOperator(
        task_id="test_logging",
        python_callable=test_logging
    )

    # DAG Order

    # TEMP DEBUG FLOW
    test_task >> extract_employee_task

    # Extract -> Transform
    extract_employee_task >> transform_employee_task
    extract_timesheets_task >> transform_timesheet_task

    # Transform -> Gold
    [transform_employee_task, transform_timesheet_task] >> derive_gold_task
