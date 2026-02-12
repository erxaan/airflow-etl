"""
Тесты для модуля проверки качества данных
"""

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


class TestDataQualityCheckOperator:
    """Тесты DataQualityCheckOperator"""

    def _make_operator(self, **overrides):
        from plugins.validators.data_quality import DataQualityCheckOperator

        defaults = dict(
            task_id="dq_test",
            source_host="src-host",
            source_port=5432,
            source_database="src_db",
            source_user="user",
            source_password="pass",
            source_schema="public",
            source_table="src_table",
            target_host="tgt-host",
            target_port=5432,
            target_database="tgt_db",
            target_user="user",
            target_password="pass",
            target_schema="dwh",
            target_table="tgt_table",
        )
        defaults.update(overrides)
        return DataQualityCheckOperator(**defaults)

    @patch("plugins.validators.data_quality.psycopg2")
    def test_all_checks_pass(self, mock_psycopg2):
        """Все проверки проходят успешно"""
        mock_src_conn = MagicMock()
        mock_tgt_conn = MagicMock()
        mock_psycopg2.connect.side_effect = [mock_src_conn, mock_tgt_conn]

        # row count: оба возвращают 100
        mock_src_cursor = MagicMock()
        mock_src_cursor.fetchone.return_value = (100,)
        mock_src_cursor.fetchall.return_value = [("id",), ("name",)]
        mock_src_conn.cursor.return_value.__enter__ = MagicMock(return_value=mock_src_cursor)
        mock_src_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)

        mock_tgt_cursor = MagicMock()
        mock_tgt_cursor.fetchone.return_value = (100,)
        mock_tgt_cursor.fetchall.return_value = [("id",), ("name",)]
        mock_tgt_conn.cursor.return_value.__enter__ = MagicMock(return_value=mock_tgt_cursor)
        mock_tgt_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)

        op = self._make_operator()
        context = {"task_instance": MagicMock(), "run_id": "test"}

        result = op.execute(context)

        assert result["row_count"]["passed"] is True
        assert result["row_count"]["source_count"] == 100
        assert result["row_count"]["target_count"] == 100

    @patch("plugins.validators.data_quality.psycopg2")
    def test_row_count_mismatch_fails(self, mock_psycopg2):
        """Несовпадение количества строк вызывает ошибку"""
        from airflow.exceptions import AirflowException

        mock_src_conn = MagicMock()
        mock_tgt_conn = MagicMock()
        mock_psycopg2.connect.side_effect = [mock_src_conn, mock_tgt_conn]

        # источник 100 цель 50
        call_count = {"n": 0}

        def make_cursor_context(count_val, cols):
            cursor = MagicMock()
            cursor.fetchone.return_value = (count_val,)
            cursor.fetchall.return_value = cols
            return cursor

        mock_src_cursor = make_cursor_context(100, [("id",)])
        mock_src_conn.cursor.return_value.__enter__ = MagicMock(return_value=mock_src_cursor)
        mock_src_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)

        mock_tgt_cursor = make_cursor_context(50, [("id",)])
        mock_tgt_conn.cursor.return_value.__enter__ = MagicMock(return_value=mock_tgt_cursor)
        mock_tgt_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)

        op = self._make_operator(check_nulls=False, check_schema_drift=False)
        context = {"task_instance": MagicMock(), "run_id": "test"}

        with pytest.raises(AirflowException, match="не пройдены"):
            op.execute(context)

    def test_operator_template_fields(self):
        op = self._make_operator()
        assert "source_schema" in op.template_fields
        assert "target_table" in op.template_fields
