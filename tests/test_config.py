"""Test configuration file."""

import os
import tempfile

from task_queue import get_task_queue_settings
from task_queue import config

def test_config_parameter_order():
    """Test parameter preference order"""
    environ = os.environ.copy()
    env_var = "Test environment variable"
    os.environ["UNIT_TEST_QUEUE_BASE"] = env_var
    settings = get_task_queue_settings(
        test=True
    )
    os.environ = environ

    # 1) Environment variables, 2) Config.json, 3) A default value
    assert settings.UNIT_TEST_QUEUE_BASE == env_var
    assert settings.TASK_QUEUE_ENV_TEST # ENV File
    assert not settings.run_argo_tests


def test_config_file_rerouting():
    """Test the configuration importing file paths"""

    new_test_val = "TEST NEW FILE"
    with tempfile.TemporaryDirectory() as tmp_dir:
        config_path = os.path.join(tmp_dir, "test_config.env")

        with open(config_path, "w", encoding="utf-8") as config_file:
            settings = get_task_queue_settings(test=True)
            config_vals = settings.model_dump()
            config_vals["UNIT_TEST_QUEUE_BASE"] = new_test_val

            for k, v in config_vals.items():
                config_file.write(f"{k}={v}\n")

        os.environ["TASK_QUEUE_CONFIG_PATH"] = config_path
        settings = get_task_queue_settings(config_path, test=True)

    assert settings.UNIT_TEST_QUEUE_BASE == new_test_val


def test_setting_class():
    """Test the setting_class argument returns the indicated class."""

    setting_default = get_task_queue_settings()
    assert isinstance(get_task_queue_settings(), config.TaskQueueApiSettings)

    setting_test = get_task_queue_settings(
        setting_class=config.TaskQueueTestSettings
    )
    assert isinstance(setting_test, config.TaskQueueTestSettings)
