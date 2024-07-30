"""Wherein is contained the ApiClient class.
"""

from .queue_base import QueueItemStage, QueueBase

class ApiClient(QueueBase):
    """Class for the ApiClient initialization and supporting functions.

    Parameters:
    -----------
    env_dict: dict
        A dictionary containing the configurations necessary to access
        the api endpoint.
    """
    def __init__(self, env_dict: dict):
        is_valid, error_msg = self._is_valid_env_dict(env_dict)
        if not is_valid:
            raise ValueError(error_msg)

        if env_dict["QUEUE_IMPLEMENTATION"] == "sql-json":
            if "SQL_QUEUE_CONNECTION_STRING" in [*env_dict]:
                self.api_base_url = env_dict["SQL_QUEUE_CONNECTION_STRING"]
            else:
                user = env_dict["SQL_QUEUE_POSTGRES_USER"]
                password = env_dict["SQL_QUEUE_POSTGRES_PASSWORD"]
                host = env_dict["SQL_QUEUE_POSTGRES_HOSTNAME"]
                database = env_dict["SQL_QUEUE_POSTGRES_DATABASE"]

                conn_str = \
                    f"postgresql+psycopg2://{user}:{password}@{host}/{database}"
                self.api_base_url = conn_str
        else:
            if "S3_QUEUE_BASE_PATH" in [*env_dict]:
                self.api_base_url = env_dict["S3_QUEUE_BASE_PATH"]

    def _is_valid_env_dict(self, env_dict: dict):
        """Validates if a given env dictionary has the necessary values to
        construct an ApiClient

        Parameters:
        -----------
        env_dict: dict
            The env dictionary being validated.

        Returns:
        -----------
        A tuple where the first value is True if the env dictionary is valid
        and False otherwise, and the second value is the error message if
        validation failed.
        """
        keys = [*env_dict]
        if "QUEUE_IMPLEMENTATION" not in keys:
            return False, "No QUEUE_IMPLEMENTATION value provided."
        if env_dict["QUEUE_IMPLEMENTATION"] != "sql-json" \
            and env_dict["QUEUE_IMPLEMENTATION"] != "s3-json":
            return False, "QUEUE_IMPLEMENTATION must be sql-json or s3-json."
        if env_dict["QUEUE_IMPLEMENTATION"] == "sql-json":
            vars_to_create_conn_string = [
                "SQL_QUEUE_POSTGRES_USER",
                "SQL_QUEUE_POSTGRES_PASSWORD",
                "SQL_QUEUE_POSTGRES_HOSTNAME",
                "SQL_QUEUE_POSTGRES_DATABASE"
            ]
            is_valid = [i for i in vars_to_create_conn_string if i in keys]
            if (not all(is_valid) or not is_valid) and \
                "SQL_QUEUE_CONNECTION_STRING" not in keys:
                return False, "Provided sql variables are invalid to " \
                    + "construct an ApiClient."
        if env_dict["QUEUE_IMPLEMENTATION"] == "s3-json":
            if "S3_QUEUE_BASE_PATH" not in keys:
                return False, "No S3_QUEUE_BASE_PATH provided."
        return True, ""

    def put(self, items):
        """Adds a new Item to the Queue in the WAITING stage.

        Parameters:
        -----------
        items: dict
            Dictionary of Queue Items to add Queue, where Item is a key:value
            pair, where key is the item ID and value is the queue item body.
        """
        return None

    def get(self, n_items=1):
        """Gets the next n Items from the Queue, moving them to PROCESSING.

        Parameters:
        -----------
        n_items: int (default=1)
            Number of items to retrieve from Queue.

        Returns:
        ------------
        Returns a list of n_items from the Queue, as
        List[(queue_item_id, queue_item_body)]
        """
        return None

    def success(self, queue_item_id):
        """Moves a Queue Item from PROCESSING to SUCCESS.

        Parameters:
        -----------
        queue_item_id: str
            ID of Queue Item
        """
        return None

    def fail(self, queue_item_id):
        """Moves a Queue Item from PROCESSING to FAIL.

        Parameters:
        -----------
        queue_item_id: str
            ID of Queue Item
        """
        return None

    def size(self, queue_item_stage):
        """Determines how many Items are in some stage of the Queue.

        Parameters:
        -----------
        queue_item_stage: QueueItemStage object
            The specific stage of the Queue (PROCESSING, FAIL, etc.).

        Returns:
        ------------
        Returns the number of Items in that stage of the Queue as an integer.
        """
        return None

    def lookup_status(self, queue_item_id):
        """Lookuup which stage in the Queue Item is currently in.

        Parameters:
        -----------
        queue_item_id: str
            ID of Queue Item

        Returns:
        ------------
        Returns the current stage of the Item as a QueueItemStage object.
        """
        return None

    def description(self):
        """A brief description of the Queue.

        Returns:
        ------------
        Returns a dictionary with relevant information about the Queue.
        """
        return None
