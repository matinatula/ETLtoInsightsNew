from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import logging

default_args = {
    "owner": "test_user",
    "depends_on_past": False,
    "start_date": datetime(2026, 3, 3),
    "retries": 0,
}


def log_test():
    print("PRINT works!")
    logger = logging.getLogger(__name__)
    logger.info("LOGGER works!")
    raise ValueError("Force task failure to see red")


with DAG(
    "log_test_dag",
    default_args=default_args,
    schedule_interval=None,
    catchup=False
) as dag:
    task1 = PythonOperator(
        task_id="test_logging1",
        python_callable=log_test
    )
    task2 = PythonOperator(
        task_id="test_logging2",
        python_callable=log_test
    )

task1 >> task2
