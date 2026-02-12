"""Тесты для модуля оповещений"""

import os
import sys
from unittest.mock import MagicMock, patch

import pytest

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)


@pytest.fixture(autouse=True)
def airflow_env(tmp_path):
    os.environ.setdefault("AIRFLOW_HOME", str(tmp_path))
    os.environ.setdefault("AIRFLOW__CORE__LOAD_EXAMPLES", "false")
    os.environ.setdefault(
        "AIRFLOW__CORE__SQL_ALCHEMY_CONN", "sqlite:///:memory:"
    )


class TestOnTaskFailureCallback:
    """Проверка on_task_failure_callback"""

    def _make_context(self):
        mock_ti = MagicMock()
        mock_ti.dag_id = "etl_accounting_system"
        mock_ti.task_id = "load_public_invoices"
        mock_ti.start_date = "2026-02-12T02:00:00"
        mock_ti.log_url = "http://airflow:8080/log?dag=etl_accounting_system"
        mock_ti.xcom_pull.return_value = "accounting-db/accounting/public.invoices"

        return {
            "task_instance": mock_ti,
            "exception": RuntimeError("Connection timeout"),
            "execution_date": "2026-02-12T02:00:00",
            "run_id": "manual__2026-02-12",
        }

    @patch("plugins.callbacks.alerting._get_variable", return_value="log")
    def test_callback_logs_when_no_channel(self, mock_var):
        from plugins.callbacks.alerting import on_task_failure_callback

        context = self._make_context()
        # не должен бросать только логировать
        on_task_failure_callback(context)

    @patch("plugins.callbacks.alerting.requests")
    @patch("plugins.callbacks.alerting._get_variable")
    def test_callback_sends_telegram(self, mock_var, mock_requests):
        from plugins.callbacks.alerting import on_task_failure_callback

        def var_side_effect(key, default=""):
            values = {
                "alert_channel": "telegram",
                "alert_telegram_token": "123:ABC",
                "alert_telegram_chat_id": "-100123",
            }
            return values.get(key, default)

        mock_var.side_effect = var_side_effect
        mock_requests.post.return_value = MagicMock(status_code=200)

        context = self._make_context()
        on_task_failure_callback(context)

        mock_requests.post.assert_called_once()
        call_args = mock_requests.post.call_args
        assert "api.telegram.org" in call_args[0][0]
        assert "-100123" in str(call_args[1]["json"]["chat_id"])

    @patch("plugins.callbacks.alerting.requests")
    @patch("plugins.callbacks.alerting._get_variable")
    def test_callback_sends_slack(self, mock_var, mock_requests):
        from plugins.callbacks.alerting import on_task_failure_callback

        def var_side_effect(key, default=""):
            values = {
                "alert_channel": "slack",
                "alert_slack_webhook_url": "https://hooks.slack.com/test",
            }
            return values.get(key, default)

        mock_var.side_effect = var_side_effect
        mock_requests.post.return_value = MagicMock(status_code=200)

        context = self._make_context()
        on_task_failure_callback(context)

        mock_requests.post.assert_called_once()
        assert "hooks.slack.com" in mock_requests.post.call_args[0][0]


class TestFormatMessage:
    """Проверка форматирования сообщения"""

    def test_message_contains_required_fields(self):
        from plugins.callbacks.alerting import _format_message

        msg = _format_message(
            dag_id="etl_test",
            task_id="load_table",
            source_info="host/db/schema.table",
            execution_date="2026-02-12",
            start_date="2026-02-12 02:00",
            error="Some error occurred",
            log_url="http://airflow:8080/log",
        )

        assert "etl_test" in msg
        assert "load_table" in msg
        assert "host/db/schema.table" in msg
        assert "Some error occurred" in msg
        assert "СБОЙ ETL ЗАДАЧИ" in msg
