import os
from setuptools import setup, find_packages

def read(rel_path: str) -> str:
    here = os.path.abspath(os.path.dirname(__file__))
    # intentionally *not* adding an encoding option to open.
    # See: https://github.com/pypa/virtualenv/issues/201#issuecomment-3145690
    with open(os.path.join(here, rel_path)) as file_path:
        return file_path.read()

def get_version(rel_path: str) -> str:
    for line in read(rel_path).splitlines():
        if line.startswith("__version__"):
            delim = '"' if '"' in line else "'"
            return line.split(delim)[1]
    raise RuntimeError("Unable to find version string.")

setup(
    name="task_queue",
    version=get_version("task_queue/__init__.py"),
    packages = find_packages(),
    install_requires = [
        "pendulum >= 2.1.2",
        "s3fs >= 2023.6.0",
        "pandas",
        "pyarrow",
        "sqlmodel",
        "psycopg2-binary",
        "requests",
        "fastapi[all]",
        "uvicorn"
    ]
)
