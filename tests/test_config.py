"""Test configuration file."""

import os
import pytest
import tempfile

from task_queue import TaskQueueSettings, get_task_queue_settings

def test_config_parameter_order():
    """Test parameter preference order"""
    env_var = "Test environment variable"
    os.environ["SQL_QUEUE_POSTGRES_PASSWORD"] = env_var
    settings = get_task_queue_settings()

    # 1) Environment variables, 2) Config.json, 3) A default value
    assert settings.SQL_QUEUE_POSTGRES_PASSWORD == env_var
    assert settings.SQL_QUEUE_POSTGRES_PORT == 5432 # ENV File
    assert settings.run_argo_tests == False # Default


def test_config_file_rerouting():
    """Test the configuration importing file paths"""

    new_test_val = "TEST NEW FILE"
    with tempfile.TemporaryDirectory() as tmp_dir:
        config_path = os.path.join(tmp_dir, "test_config.env")

        with open(config_path, "w", encoding="utf-8") as config_file:
            settings = get_task_queue_settings()
            config_vals = settings.model_dump()
            config_vals["SQL_QUEUE_POSTGRES_USER"] = new_test_val

            for k, v in config_vals.items():
                config_file.write(f"{k}={v}\n")

        os.environ["CONFIG_PATH"] = config_path
        settings = get_task_queue_settings(config_path)

    assert settings.SQL_QUEUE_POSTGRES_USER == new_test_val
