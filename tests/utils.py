"""Top file docstring
"""
from sqlalchemy import create_engine
import random
import data_pipeline.sql_queue as sqlq

test_sql_engine = create_engine(
    "postgresql+psycopg2://postgres:postgres@postgres:5432/postgres", 
    echo=True
)
