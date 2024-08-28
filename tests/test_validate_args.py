"""Tests for validating input arguments passed to the CLI
"""
import sys
import pytest

from sqlalchemy import create_engine

from task_queue.cli.work_queue_service_cli import (validate_args,
                                            handle_queue_implementation_choice)
from task_queue.config import config
from task_queue.queues.sql_queue import json_sql_queue
from task_queue.events.sql_event_store import SqlEventStore
from task_queue.queues.queue_with_events import queue_with_events

JSON_S3_QUEUE_CLI_CHOICE=config.QueueImplementations.S3_JSON.value
JSON_SQL_QUEUE_CLI_CHOICE=config.QueueImplementations.SQL_JSON.value
ARGO_WORKFLOWS_INTERFACE_CLI_CHOICE=config.WorkerInterfaceChoices.ARGO_WORKFLOWS.value
NO_EVENT_STORE_CLI_CHOICE=config.EventStoreChoices.NO_EVENTS.value
SQL_EVENT_STORE_CLI_CHOICE=config.EventStoreChoices.SQL_JSON.value


def test_validate_args_s3_success():
    """Test valid arguments for the S3 queue
    """
    args_dict = {'worker_interface': 'argo-workflows',
            'queue_implementation': 's3-json',
            'event_store_implementation': 'none',
            'with_queue_events': False,
            'worker_interface_id': 'dummy-id',
            'endpoint': 'dummy-endpoint',
            'namespace': 'dummy-namespace',
            'connection_string': None,
            'queue_name': None,
            's3_base_path': 'dummypath',
            'add_to_queue_event_name': None,
            'move_queue_event_name': None}
    success, error_string = validate_args(args_dict)
    assert success
    assert error_string == ''

def test_validate_args_s3_missing_base_path():
    """Ensure base path is provided when using S3 queue
    """
    args_dict = {'worker_interface': 'argo-workflows',
            'queue_implementation': 's3-json',
            'event_store_implementation': 'none',
            'with_queue_events': False,
            'worker_interface_id': 'dummy-id',
            'endpoint': 'dummy-endpoint',
            'namespace': 'dummy-namespace',
            'connection_string': None,
            'queue_name': None,
            's3_base_path': None,
            'add_to_queue_event_name': None,
            'move_queue_event_name': None}
    success, error_string = validate_args(args_dict)
    assert not success
    assert f'when queue-implementation is set to {JSON_S3_QUEUE_CLI_CHOICE}'\
           in error_string

def test_validate_args_sql_success():
    """Test valid arguments for sql queue
    """
    args_dict = {'worker_interface': 'argo-workflows',
            'queue_implementation': 'sql-json',
            'event_store_implementation': 'none',
            'with_queue_events': False,
            'worker_interface_id': 'dummy-id',
            'endpoint': 'dummy-endpoint',
            'namespace': 'dummy-namespace',
            'connection_string': 'dummyconnectionstring',
            'queue_name': 'dummyqueuename',
            's3_base_path': None,
            'add_to_queue_event_name': None,
            'move_queue_event_name': None}
    success, error_string = validate_args(args_dict)
    assert success
    assert error_string == ''

def test_validate_args_sql_missing_queue_name():
    """Ensure queue name is provided when using sql queue
    """
    args_dict = {'worker_interface': 'argo-workflows',
            'queue_implementation': 'sql-json',
            'event_store_implementation': 'none',
            'with_queue_events': False,
            'worker_interface_id': 'dummy-id',
            'endpoint': 'dummy-endpoint',
            'namespace': 'dummy-namespace',
            'connection_string': 'dummyconnectionstring',
            'queue_name': None,
            's3_base_path': None,
            'add_to_queue_event_name': None,
            'move_queue_event_name': None}
    success, error_string = validate_args(args_dict)
    assert not success
    assert f'queue-implementation is set to {JSON_SQL_QUEUE_CLI_CHOICE}'\
           in error_string

def test_validate_args_sql_missing_connection_string():
    """Ensure connection string is provided when using sql queue
    """
    args_dict = {'worker_interface': 'argo-workflows',
            'queue_implementation': 'sql-json',
            'event_store_implementation': 'none',
            'with_queue_events': False,
            'worker_interface_id': 'dummy-id',
            'endpoint': 'dummy-endpoint',
            'namespace': 'dummy-namespace',
            'connection_string': None,
            'queue_name': 'dummyqueuename',
            's3_base_path': None,
            'add_to_queue_event_name': None,
            'move_queue_event_name': None}
    success, error_string = validate_args(args_dict)
    assert not success
    assert f'queue-implementation is set to {JSON_SQL_QUEUE_CLI_CHOICE}'\
           in error_string

def test_validate_args_worker_interface_success():
    """Test valid arguments for argo-workflows interface
    """
    args_dict = {'worker_interface': 'argo-workflows',
            'queue_implementation': 'sql-json',
            'event_store_implementation': 'none',
            'with_queue_events': False,
            'worker_interface_id': 'dummy-id',
            'endpoint': 'dummy-endpoint',
            'namespace': 'dummy-namespace',
            'connection_string': 'dummyconnectionstring',
            'queue_name': 'dummyqueuename',
            's3_base_path': None,
            'add_to_queue_event_name': None,
            'move_queue_event_name': None}
    success, error_string = validate_args(args_dict)
    assert success
    assert error_string == ''

def test_validate_args_worker_interface_missing_id():
    """Ensure worker_interface_id is provided when using
       argo workflows worker interface
    """
    args_dict = {'worker_interface': 'argo-workflows',
            'queue_implementation': 'sql-json',
            'event_store_implementation': 'none',
            'with_queue_events': False,
            'worker_interface_id': None,
            'endpoint': 'dummy-endpoint',
            'namespace': 'dummy-namespace',
            'connection_string': 'dummyconnectionstring',
            'queue_name': 'dummyqueuename',
            's3_base_path': None,
            'add_to_queue_event_name': None,
            'move_queue_event_name': None}
    success, error_string = validate_args(args_dict)
    assert not success
    assert f'worker-interface is set to {ARGO_WORKFLOWS_INTERFACE_CLI_CHOICE}'\
           in error_string

def test_validate_args_worker_interface_missing_endpoint():
    """Ensure endpoint is provided when using argo-workflows
       worker_interface
    """
    args_dict = {'worker_interface': 'argo-workflows',
            'queue_implementation': 'sql-json',
            'event_store_implementation': 'none',
            'with_queue_events': False,
            'worker_interface_id': 'dummyID',
            'endpoint': None,
            'namespace': 'dummy-namespace',
            'connection_string': 'dummyconnectionstring',
            'queue_name': 'dummyqueuename',
            's3_base_path': None,
            'add_to_queue_event_name': None,
            'move_queue_event_name': None}
    success, error_string = validate_args(args_dict)
    assert not success
    assert f'worker-interface is set to {ARGO_WORKFLOWS_INTERFACE_CLI_CHOICE}'\
           in error_string

def test_validate_args_worker_interface_missing_namespace():
    """Ensure namespace is provided when using argo-workflows
       worker_interface
    """
    args_dict = {'worker_interface': 'argo-workflows',
            'queue_implementation': 'sql-json',
            'event_store_implementation': 'none',
            'with_queue_events': False,
            'worker_interface_id': 'dummyID',
            'endpoint': 'dummyendpoint',
            'namespace': None,
            'connection_string': 'dummyconnectionstring',
            'queue_name': 'dummyqueuename',
            's3_base_path': None,
            'add_to_queue_event_name': None,
            'move_queue_event_name': None}
    success, error_string = validate_args(args_dict)
    assert not success
    assert f'worker-interface is set to {ARGO_WORKFLOWS_INTERFACE_CLI_CHOICE}'\
           in error_string

def test_validate_args_event_store_implementation_success():
    """Test valid arguments for event store implementation.
    """
    args_dict = {'worker_interface': 'argo-workflows',
            'queue_implementation': 'sql-json',
            'event_store_implementation': 'sql-json',
            'with_queue_events': 'True',
            'worker_interface_id': 'dummy-id',
            'endpoint': 'dummy-endpoint',
            'namespace': 'dummy-namespace',
            'connection_string': 'dummyconnectionstring',
            'queue_name': 'dummyqueuename',
            's3_base_path': None,
            'add_to_queue_event_name': 'dummyeventname',
            'move_queue_event_name': 'dummyeventname2'}
    success, error_string = validate_args(args_dict)
    assert success
    assert error_string == ''

def test_validate_args_event_store_implementation_missing_add_name():
    """Ensure add_to_queue_event_name is provided when
       event_store_implementation is set to sql-json
    """
    args_dict = {'worker_interface': 'argo-workflows',
            'queue_implementation': 'sql-json',
            'event_store_implementation': 'sql-json',
            'with_queue_events': 'True',
            'worker_interface_id': 'dummy-id',
            'endpoint': 'dummy-endpoint',
            'namespace': 'dummy-namespace',
            'connection_string': 'dummyconnectionstring',
            'queue_name': 'dummyqueuename',
            's3_base_path': None,
            'add_to_queue_event_name': None,
            'move_queue_event_name': 'dummyeventname2'}
    success, error_string = validate_args(args_dict)
    assert not success
    assert f'event-store-implementation is not {NO_EVENT_STORE_CLI_CHOICE}'\
           in error_string

def test_validate_args_event_store_implementation_missing_move_name():
    """Ensure move_queue_event_name is provided when
       event_store_implementation is set to sql-json
    """
    args_dict = {'worker_interface': 'argo-workflows',
            'queue_implementation': 'sql-json',
            'event_store_implementation': 'sql-json',
            'with_queue_events': 'True',
            'worker_interface_id': 'dummy-id',
            'endpoint': 'dummy-endpoint',
            'namespace': 'dummy-namespace',
            'connection_string': 'dummyconnectionstring',
            'queue_name': 'dummyqueuename',
            's3_base_path': None,
            'add_to_queue_event_name': 'dummyevent',
            'move_queue_event_name': None}
    success, error_string = validate_args(args_dict)
    assert not success
    assert f'event-store-implementation is not {NO_EVENT_STORE_CLI_CHOICE}'\
           in error_string

def test_validate_args_event_store_implementation_sql_json_only_option():
    """Ensure sql-json is selected for event_store_implementation if
       with_queue_events is set
    """
    args_dict = {'worker_interface': 'argo-workflows',
            'queue_implementation': 'sql-json',
            'event_store_implementation': 'none',
            'with_queue_events': 'True',
            'worker_interface_id': 'dummy-id',
            'endpoint': 'dummy-endpoint',
            'namespace': 'dummy-namespace',
            'connection_string': 'dummyconnectionstring',
            'queue_name': 'dummyqueuename',
            's3_base_path': None,
            'add_to_queue_event_name': 'dummyeventname',
            'move_queue_event_name': 'dummyeventname2'}
    success, error_string = validate_args(args_dict)
    assert not success
    assert  "event_store_implementation must be set to "\
           f"{SQL_EVENT_STORE_CLI_CHOICE}"\
           in error_string

def test_handle_queue_implementation_choice_pass():
    """Checks that the handle_queue_implementation_choice will return the
       correct queue given the right cli settings
    """
    sql_settings = config.get_task_queue_settings(
                setting_class=config.TaskQueueSqlSettings
            )
    environ_settings = sql_settings.model_dump()

    if environ_settings['SQL_QUEUE_CONNECTION_STRING'] is None:
        # Build the connection string
        user = environ_settings['SQL_QUEUE_POSTGRES_USER']
        password = environ_settings['SQL_QUEUE_POSTGRES_PASSWORD']
        hostname = environ_settings['SQL_QUEUE_POSTGRES_HOSTNAME']
        port = environ_settings['SQL_QUEUE_POSTGRES_PORT']
        database = environ_settings['SQL_QUEUE_POSTGRES_DATABASE']


        connection_string = (
            f"postgresql://{user}:{password}"
            f"@{hostname}:{port}/{database}"
        )

    else:
        connection_string = environ_settings['SQL_QUEUE_CONNECTION_STRING']

    sys.argv = ['example.py',
                '--worker_interface', 'argo-workflows',
                '--queue_implementation', 'sql-json',
                '--with-queue-events', 'True',
                '--event_store_implementation', 'sql-json',
                '--connection-string', connection_string,
                '--queue-name', 'dummyqueuename',
                '--add-to-queue-event-name', 'dummyeventname',
                '--move-queue-event-name', 'dummyeventname2']

    test_settings = config.TaskQueueCliSettings()

    test_queue = json_sql_queue(
            create_engine(test_settings.connection_string),
            test_settings.queue_name
        )

    test_store = SqlEventStore(
                create_engine(test_settings.connection_string)
            )

    test_queue = queue_with_events(
            test_queue,
            test_store,
            add_event_name=test_settings.add_to_queue_event_name,
            move_event_name=test_settings.move_queue_event_name
        )

    queue = handle_queue_implementation_choice(test_settings)

    assert queue.description() == test_queue.description()

def test_handle_queue_implementation_choice_fail():
    """Checks that the handle_queue_implementation_choice will raise
       the correct error if the wrong combination of settings are used
    """
    sql_settings = config.get_task_queue_settings(
                setting_class=config.TaskQueueSqlSettings
            )
    environ_settings = sql_settings.model_dump()

    if environ_settings['SQL_QUEUE_CONNECTION_STRING'] is None:
        # Build the connection string
        user = environ_settings['SQL_QUEUE_POSTGRES_USER']
        password = environ_settings['SQL_QUEUE_POSTGRES_PASSWORD']
        hostname = environ_settings['SQL_QUEUE_POSTGRES_HOSTNAME']
        port = environ_settings['SQL_QUEUE_POSTGRES_PORT']
        database = environ_settings['SQL_QUEUE_POSTGRES_DATABASE']


        connection_string = (
            f"postgresql://{user}:{password}"
            f"@{hostname}:{port}/{database}"
        )

    else:
        connection_string = environ_settings['SQL_QUEUE_CONNECTION_STRING']

    sys.argv = ['example.py',
                '--worker_interface', 'argo-workflows',
                '--queue_implementation', 'sql-json',
                '--with-queue-events', 'True',
                '--event_store_implementation', 'none',
                '--connection-string', connection_string,
                '--queue-name', 'dummyqueuename']

    settings = config.TaskQueueCliSettings()

    with pytest.raises(AttributeError,
                       match="SQL_JSON is the only implemented event store"
                                  " that works with with_queue_events"):
        handle_queue_implementation_choice(settings)