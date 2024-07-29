"""Tests for validating input arguments passed to the CLI
"""
from task_queue.work_queue_service_cli import (
                                    validate_args,
                                    JSON_S3_QUEUE_CLI_CHOICE, 
                                    JSON_SQL_QUEUE_CLI_CHOICE, 
                                    ARGO_WORKFLOWS_INTERFACE_CLI_CHOICE, 
                                    NO_EVENT_STORE_CLI_CHOICE, 
                                    SQL_EVENT_STORE_CLI_CHOICE)
                                                 

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