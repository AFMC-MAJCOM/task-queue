import sqlalchemy as sqla
import os

test_sql_engine = sqla.create_engine(sqla.engine.url.URL(
            drivername="postgresql",
            username=os.environ["SQL_USERNAME"],
            password=os.environ["SQL_PASSWORD"],
            host=os.environ["SQL_HOST"],
            database="postgres",
            query={},
            port=os.environ.get("SQL_PORT", 5432),
            echo=True
        ))