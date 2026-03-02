import logging
from sqlalchemy import create_engine
from dotenv import load_dotenv
import os

load_dotenv()

DB_USER = os.getenv("POSTGRES_USER")
DB_PASSWORD = os.getenv("POSTGRES_PASSWORD")
DB_HOST = os.getenv("POSTGRES_HOST")
DB_PORT = os.getenv("POSTGRES_PORT")
DB_NAME = os.getenv("POSTGRES_DB")

engine = create_engine(
    f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}")

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s [%(levelname)s] %(message)s")


def load_employee(df):
    if df.empty:
        logging.warning("No employee data to load")
        return
    try:
        df.to_sql("employee", engine, if_exists="append", index=False)
        logging.info(f"Loaded {len(df)} rows into employee")
    except Exception as e:
        logging.error(f"Failed to load employee data: {e}")


def load_timesheet(df):
    if df.empty:
        logging.warning("No timesheet data to load")
        return
    try:
        df.to_sql("timesheet", engine, if_exists="append", index=False)
        logging.info(f"Loaded {len(df)} rows into timesheet")
    except Exception as e:
        logging.error(f"Failed to load timesheet data: {e}")
