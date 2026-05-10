"""
Database connection config.
Reads credentials from .env — never hardcode passwords.
"""
import os
from dotenv import load_dotenv
from sqlalchemy import create_engine

load_dotenv()

DB_HOST     = os.getenv("DB_HOST", "localhost")
DB_PORT     = os.getenv("DB_PORT", "5432")
DB_NAME     = os.getenv("DB_NAME", "nhs_pipeline")
DB_USER     = os.getenv("DB_USER", "postgres")
DB_PASSWORD = os.getenv("DB_PASSWORD", "")

DATABASE_URL = (
    f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}"
    f"@{DB_HOST}:{DB_PORT}/{DB_NAME}"
)


def get_engine():
    """Return a SQLAlchemy engine for the nhs_pipeline database."""
    return create_engine(DATABASE_URL)
