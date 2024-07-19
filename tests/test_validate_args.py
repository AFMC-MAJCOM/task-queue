from data_pipeline.work_queue_service_cli import *

def test_validate_args_s3_success():
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
    assert success == True
    assert error_string == ''

def test_validate_args_s3_missing_base_path():
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
    assert success == False
    assert f'when queue-implementation is set to {JSON_S3_QUEUE_CLI_CHOICE}' in error_string

def test_validate_args_sql_success():
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
    assert success == True
    assert error_string == ''

def test_validate_args_sql_missing_queue_name():
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
    assert success == False
    assert f'queue-implementation is set to {JSON_SQL_QUEUE_CLI_CHOICE}' in error_string

def test_validate_args_sql_missing_connection_string():
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
    assert success == False
    assert f'queue-implementation is set to {JSON_SQL_QUEUE_CLI_CHOICE}' in error_string

def test_validate_args_worker_interface_success():
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
    assert success == True
    assert error_string == ''

def test_validate_args_worker_interface_missing_id():
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
    assert success == False
    assert f'worker-interface is set to {ARGO_WORKFLOWS_INTERFACE_CLI_CHOICE}' in error_string

def test_validate_args_worker_interface_missing_endpoint():
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
    assert success == False
    assert f'worker-interface is set to {ARGO_WORKFLOWS_INTERFACE_CLI_CHOICE}' in error_string

def test_validate_args_worker_interface_missing_namespace():
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
    assert success == False
    assert f'worker-interface is set to {ARGO_WORKFLOWS_INTERFACE_CLI_CHOICE}' in error_string

def test_validate_args_event_store_implementation_success():
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
    assert success == True
    assert error_string == ''

def test_validate_args_event_store_implementation_missing_add_name():
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
    assert success == False
    assert f'event-store-implementation is not {NO_EVENT_STORE_CLI_CHOICE}' in error_string

def test_validate_args_event_store_implementation_missing_move_name():
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
    assert success == False
    assert f'event-store-implementation is not {NO_EVENT_STORE_CLI_CHOICE}' in error_string

def test_validate_args_event_store_implementation_sql_json_only_option():
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
    assert success == False
    assert f'event_store_implementation must be set to {SQL_EVENT_STORE_CLI_CHOICE}' in error_string