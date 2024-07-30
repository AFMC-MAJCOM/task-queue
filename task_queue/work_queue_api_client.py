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
                return False, "Provided sql variables are invalid to construct" \
                    + " an ApiClient."
        if env_dict["QUEUE_IMPLEMENTATION"] == "s3-json":
            if "S3_QUEUE_BASE_PATH" not in keys:
                return False, "No S3_QUEUE_BASE_PATH provided."
        return True, ""

    def put(item_id: str, body):
        """Adds a new item to the queue in the WAITING state.

        Parameters:
        -----------
        item_id: string
            The id of the item being added to the queue.
        body: any
            The body of the item being added to the queue.
        """
        return None

    def get(n_items: int):
        """Returns n items from the queue.

        Parameters:
        -----------
        n_items: int
            The number of items that will be returned.

        Returns:
        -----------
        Returns a list of items from the queue.
        """
        return None # returns List[Tuple[str, Any]]

    def success(item_id: str):
        """Classifies an item as successfully completed.

        Parameters:
        -----------
        item_id: str
            The id of the item that needs to be moved to success.
        """
        return None

    def fail(item_id: str):
        """Classifies an item as failed.

        Parameters:
        -----------
        item_id: str
            The id of the item that needs to be moved to failed.
        """
        return None

    def size(item_stage: QueueItemStage):
        """Returns the number of items in specific queue stage.

        Parameters:
        -----------
        item_stage: QueueItemStage
            The stage being requested.

        Returns:
        -----------
        Returns the size of the requested item stage.
        """
        return None # returns int

    def lookup_status(item_id: str):
        """Returns the item stage of a given item.

        Parameters:
        -----------
        item_id: str
            The id of the item being requested.

        Returns:
        -----------
        Returns the queue stage of the requested item.
        """
        return None # returns QueueItemStage

    def description():
        """Returns a description of the queue.

        Returns:
        -----------
        Returns a dictionary description of the queue.
        """
        return None # returns Dict[str, str]
