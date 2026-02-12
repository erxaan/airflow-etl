"""Тесты для ChunkedPgToPgOperator и PgServerCursorHook"""

import os
import sys
from unittest.mock import MagicMock, patch, call

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


class TestPgServerCursorHook:
    """Проверка поведения PgServerCursorHook"""

    @patch("plugins.hooks.pg_server_cursor_hook.psycopg2")
    def test_get_conn_creates_connection(self, mock_psycopg2):
        from plugins.hooks.pg_server_cursor_hook import PgServerCursorHook

        hook = PgServerCursorHook(
            host="localhost",
            port=5432,
            database="test_db",
            user="user",
            password="pass",
        )

        hook.get_conn()

        mock_psycopg2.connect.assert_called_once_with(
            host="localhost",
            port=5432,
            dbname="test_db",
            user="user",
            password="pass",
            connect_timeout=30,
        )

    @patch("plugins.hooks.pg_server_cursor_hook.psycopg2")
    def test_close_closes_connection(self, mock_psycopg2):
        from plugins.hooks.pg_server_cursor_hook import PgServerCursorHook

        mock_conn = MagicMock()
        mock_conn.closed = False
        mock_psycopg2.connect.return_value = mock_conn

        hook = PgServerCursorHook(
            host="localhost", port=5432, database="db",
            user="u", password="p",
        )
        hook.get_conn()
        hook.close()

        mock_conn.close.assert_called_once()

    @patch("plugins.hooks.pg_server_cursor_hook.psycopg2")
    def test_read_table_in_chunks_yields_chunks(self, mock_psycopg2):
        from plugins.hooks.pg_server_cursor_hook import PgServerCursorHook

        # мок курсора
        mock_conn = MagicMock()
        mock_conn.closed = False
        mock_psycopg2.connect.return_value = mock_conn

        mock_cursor = MagicMock()
        mock_cursor.fetchmany.side_effect = [
            [(1, "a"), (2, "b")],  # первый чанк
            [(3, "c")],           # второй чанк
            [],                   # конец
        ]
        mock_conn.cursor.return_value.__enter__ = MagicMock(return_value=mock_cursor)
        mock_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)

        hook = PgServerCursorHook(
            host="localhost", port=5432, database="db",
            user="u", password="p", chunk_size=2,
        )

        chunks = list(hook.read_table_in_chunks("public", "test_table"))

        assert len(chunks) == 2
        assert len(chunks[0]) == 2
        assert len(chunks[1]) == 1

    @patch("plugins.hooks.pg_server_cursor_hook.psycopg2")
    def test_get_column_names(self, mock_psycopg2):
        from plugins.hooks.pg_server_cursor_hook import PgServerCursorHook

        mock_conn = MagicMock()
        mock_conn.closed = False
        mock_psycopg2.connect.return_value = mock_conn

        mock_cursor = MagicMock()
        mock_cursor.fetchall.return_value = [("id",), ("name",), ("value",)]
        mock_conn.cursor.return_value.__enter__ = MagicMock(return_value=mock_cursor)
        mock_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)

        hook = PgServerCursorHook(
            host="localhost", port=5432, database="db",
            user="u", password="p",
        )

        columns = hook.get_column_names("public", "test_table")

        assert columns == ["id", "name", "value"]


class TestChunkedPgToPgOperator:
    """Проверка поведения ChunkedPgToPgOperator"""

    def _make_operator(self, **overrides):
        from plugins.operators.chunked_pg_operator import ChunkedPgToPgOperator

        defaults = dict(
            task_id="test_task",
            source_host="src-host",
            source_port=5432,
            source_database="src_db",
            source_user="src_user",
            source_password="src_pass",
            source_schema="public",
            source_table="src_table",
            target_host="tgt-host",
            target_port=5432,
            target_database="tgt_db",
            target_user="tgt_user",
            target_password="tgt_pass",
            target_schema="dwh",
            target_table="tgt_table",
            chunk_size=5000,
        )
        defaults.update(overrides)
        return ChunkedPgToPgOperator(**defaults)

    def test_operator_init(self):
        op = self._make_operator()
        assert op.source_host == "src-host"
        assert op.target_schema == "dwh"
        assert op.chunk_size == 5000
        assert op.truncate_before_load is True

    def test_template_fields(self):
        op = self._make_operator()
        assert "source_schema" in op.template_fields
        assert "target_table" in op.template_fields

    @patch("plugins.operators.chunked_pg_operator.psycopg2")
    @patch("plugins.operators.chunked_pg_operator.PgServerCursorHook")
    def test_execute_calls_extract_and_load(self, mock_hook_cls, mock_psycopg2):
        """execute читает чанки и пишет в целевую БД"""
        # мок source hook
        mock_hook = MagicMock()
        mock_hook_cls.return_value = mock_hook
        mock_hook.get_column_names.return_value = ["id", "name"]
        mock_hook.read_table_in_chunks.return_value = iter([
            [(1, "Alice"), (2, "Bob")],
        ])

        # мок целевого соединения
        mock_target_conn = MagicMock()
        mock_target_conn.closed = False
        mock_psycopg2.connect.return_value = mock_target_conn

        # мок контекста
        mock_ti = MagicMock()
        mock_ti.dag_id = "test_dag"
        mock_ti.task_id = "test_task"
        context = {"task_instance": mock_ti, "run_id": "test_run"}

        op = self._make_operator()
        result = op.execute(context)

        assert result["status"] == "success"
        assert result["rows_extracted"] == 2
        assert result["rows_loaded"] == 2

        # проверяем что truncate вызван
        mock_target_conn.cursor().__enter__().execute.assert_any_call(
            "TRUNCATE TABLE dwh.tgt_table"
        )

    @patch("plugins.operators.chunked_pg_operator.psycopg2")
    @patch("plugins.operators.chunked_pg_operator.PgServerCursorHook")
    def test_execute_handles_failure(self, mock_hook_cls, mock_psycopg2):
        """При ошибке выставляется статус failed и пробрасывается исключение"""
        mock_hook = MagicMock()
        mock_hook_cls.return_value = mock_hook
        mock_hook.get_column_names.side_effect = Exception("Connection refused")

        mock_target_conn = MagicMock()
        mock_target_conn.closed = False
        mock_psycopg2.connect.return_value = mock_target_conn

        mock_ti = MagicMock()
        mock_ti.dag_id = "test_dag"
        mock_ti.task_id = "test_task"
        context = {"task_instance": mock_ti, "run_id": "test_run"}

        op = self._make_operator()

        with pytest.raises(Exception, match="Connection refused"):
            op.execute(context)
