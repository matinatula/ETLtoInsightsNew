
# DB connection helper
# get_engine() -> always points to etl_db

# src/etl/utils.py
import os
from sqlalchemy import create_engine
from dotenv import load_dotenv

load_dotenv()


def get_engine(db_name=None):
    """
    Returns SQLAlchemy engine using credentials from .env
    If db_name is provided, connects to that database; otherwise uses ETL DB.
    """
    POSTGRES_USER = os.environ["POSTGRES_USER"]
    POSTGRES_PASSWORD = os.environ["POSTGRES_PASSWORD"]
    POSTGRES_HOST = os.environ["POSTGRES_HOST"]
    POSTGRES_PORT = os.environ["POSTGRES_PORT"]
    POSTGRES_DB = db_name or os.environ.get(
        "ETL_POSTGRES_DB","etl_db")  # default ETL DB

    db_url = f"postgresql+psycopg2://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"
    engine = create_engine(db_url, echo=False)
    return engine


