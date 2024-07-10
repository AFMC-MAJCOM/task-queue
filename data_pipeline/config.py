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
    if os.environ.get('S3_ENDPOINT', False):
        return s3fs.S3FileSystem(
            client_kwargs={'endpoint_url': os.environ['S3_ENDPOINT']},
            **kwargs
        )

    return s3fs.S3FileSystem(
        endpoint_url='http://localhost:9000',
        key='minioadmin',
        secret='minioadmin',
        **kwargs
    )
