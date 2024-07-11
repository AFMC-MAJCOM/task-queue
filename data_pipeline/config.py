"""Provides configuration settings and functions used by task-queue."""

import os

import s3fs


def get_s3fs_connection(**kwargs):
    """Get S3FileSystem connection.

    If the S3 variables are not present to create the connection to minio,
    use a local setup.

    Parameters
    ----------
    **kwargs to be passed to S3FileSystem's kwargs

    Returns
    -------
    s3fs.S3FileSystem object
    """
    return s3fs.S3FileSystem(
        endpoint_url=os.environ.get("S3_ENDPOINT", None),
        key=os.environ.get("AWS_ACCESS_KEY_ID", None),
        secret=os.environ.get("AWS_SECRET_ACCESS_KEY", None),
        token=os.environ.get("AWS_SESSION_TOKEN", None),
        **kwargs,
    )
