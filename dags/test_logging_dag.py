from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import logging
import time

default_args = {
    "owner": "test_user",
    "depends_on_past": False,
    "start_date": datetime(2026, 3, 3),
    "retries": 0,
}


def simple_log_task():
    # Use Airflow’s task logger
    logger = logging.getLogger("airflow.task")
    logger.setLevel(logging.INFO)

    logger.info("LOGGER: This should appear in Airflow task log")
    print("PRINT: This should also appear")
    time.sleep(5)  # ensures output is flushed


with DAG(
    dag_id="test_minimal_logging",
    default_args=default_args,
    schedule_interval=None,
    catchup=False
) as dag:

    t1 = PythonOperator(
        task_id="logging_test",
        python_callable=simple_log_task
    )
