from setuptools import setup

setup(
    name="data_pipeline",
    packages = ["data_pipeline", "data_pipeline.events"],
    install_requires = [
        "pendulum >= 2.1.2",
        "s3fs >= 2023.6.0",
        "pandas",
        "pyarrow",
        "sqlmodel",
        "psycopg2-binary",
        "requests",
        "fastapi[all]"
    ]
)
