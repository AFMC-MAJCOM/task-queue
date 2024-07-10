"""Created SQL engine for pytests.
"""
from sqlalchemy import create_engine


test_sql_engine = create_engine(
    "postgresql+psycopg2://postgres:postgres@postgres:5432/postgres",
    echo=True
)
