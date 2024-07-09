from setuptools import setup

setup(
    name="task_queue",
    packages = ["task_queue", "task_queue.events"],
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
