"""Test configuration file."""

import os
import tempfile

import pytest

from task_queue import get_task_queue_settings
from task_queue import config


class TaskQueueTestSettings(config.TaskQueueApiSettings):
    """Extra settings for testing the task queue library."""
    # Testing configuration parameters
    TASK_QUEUE_ENV_TEST: bool = False
    run_argo_tests: bool = False
    UNIT_TEST_QUEUE_BASE: str = "s3://integration-tests/queue/queue_"


@pytest.mark.unit
def test_config_parameter_order():
    """Test parameter preference order"""
    environ = os.environ.copy()
    os.environ["run_argo_tests"] = "True"


    with tempfile.TemporaryDirectory() as tmp_dir:
        config_path = os.path.join(tmp_dir, "test_config.env")

        with open(config_path, "w", encoding="utf-8") as config_file:
            os.environ["TASK_QUEUE_CONFIG_PATH"] = config_path

            config_vals = {"TASK_QUEUE_ENV_TEST": True}

            for k, v in config_vals.items():
                config_file.write(f"{k}={v}\n")

        settings = get_task_queue_settings(
            setting_class=TaskQueueTestSettings
        )

    os.environ = environ

    # 1) Environment variables, 2) Config.json, 3) A default value
    assert settings.run_argo_tests
    assert settings.TASK_QUEUE_ENV_TEST # ENV File
    assert settings.UNIT_TEST_QUEUE_BASE == "s3://integration-tests/queue/queue_"


@pytest.mark.unit
def test_config_file_rerouting():
    """Test the configuration importing file paths"""

    new_test_val = "TEST NEW FILE"
    with tempfile.TemporaryDirectory() as tmp_dir:
        config_path = os.path.join(tmp_dir, "test_config.env")

        with open(config_path, "w", encoding="utf-8") as config_file:
            settings = get_task_queue_settings(
                setting_class=TaskQueueTestSettings
            )
            config_vals = settings.model_dump()
            config_vals["UNIT_TEST_QUEUE_BASE"] = new_test_val

            for k, v in config_vals.items():
                config_file.write(f"{k}={v}\n")

        os.environ["TASK_QUEUE_CONFIG_PATH"] = config_path
        settings = get_task_queue_settings(TaskQueueTestSettings, config_path)

    assert settings.UNIT_TEST_QUEUE_BASE == new_test_val


@pytest.mark.unit
def test_setting_class():
    """Test the setting_class argument returns the indicated class."""

    setting_test = get_task_queue_settings(
        setting_class=TaskQueueTestSettings
    )
    assert isinstance(setting_test, TaskQueueTestSettings)
