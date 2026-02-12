"""Оператор для чанковой загрузки PostgreSQL в PostgreSQL с логированием в etl_log"""

from __future__ import annotations

import io
import csv
import logging
from datetime import datetime, timezone
from typing import Any

import psycopg2
from airflow.models import BaseOperator

from plugins.hooks.pg_server_cursor_hook import PgServerCursorHook

logger = logging.getLogger(__name__)


class ChunkedPgToPgOperator(BaseOperator):
    """Перенос данных PostgreSQL - PostgreSQL чанками"""

    template_fields = (
        "source_schema",
        "source_table",
        "target_schema",
        "target_table",
    )

    def __init__(
        self,
        *,
        # параметры источника
        source_host: str,
        source_port: int = 5432,
        source_database: str,
        source_user: str,
        source_password: str,
        source_schema: str,
        source_table: str,
        # параметры цели
        target_host: str,
        target_port: int = 5432,
        target_database: str,
        target_user: str,
        target_password: str,
        target_schema: str,
        target_table: str,
        # настройки ETL
        chunk_size: int = 10_000,
        truncate_before_load: bool = True,
        columns: str = "*",
        where_clause: str | None = None,
        # логирование
        etl_log_schema: str = "public",
        etl_log_table: str = "etl_log",
        **kwargs,
    ):
        super().__init__(**kwargs)

        # источник
        self.source_host = source_host
        self.source_port = source_port
        self.source_database = source_database
        self.source_user = source_user
        self.source_password = source_password
        self.source_schema = source_schema
        self.source_table = source_table

        # цель
        self.target_host = target_host
        self.target_port = target_port
        self.target_database = target_database
        self.target_user = target_user
        self.target_password = target_password
        self.target_schema = target_schema
        self.target_table = target_table

        # настройки ETL
        self.chunk_size = chunk_size
        self.truncate_before_load = truncate_before_load
        self.columns = columns
        self.where_clause = where_clause

        # логирование
        self.etl_log_schema = etl_log_schema
        self.etl_log_table = etl_log_table

    def execute(self, context: dict[str, Any]) -> dict[str, Any]:
        """Выполнить загрузку и записать лог"""
        started_at = datetime.now(timezone.utc)
        rows_extracted = 0
        rows_loaded = 0
        status = "running"
        error_message = None

        # пишем source_info в XCom для алертинга
        source_info = f"{self.source_host}/{self.source_database}/{self.source_schema}.{self.source_table}"
        context["task_instance"].xcom_push(key="source_info", value=source_info)

        source_hook = PgServerCursorHook(
            host=self.source_host,
            port=self.source_port,
            database=self.source_database,
            user=self.source_user,
            password=self.source_password,
            chunk_size=self.chunk_size,
        )

        target_conn = None

        try:
            # логируем старт
            self._write_etl_log(context, started_at, "running", 0, 0, None)

            # подключаемся к целевой БД
            target_conn = psycopg2.connect(
                host=self.target_host,
                port=self.target_port,
                dbname=self.target_database,
                user=self.target_user,
                password=self.target_password,
            )
            target_conn.autocommit = False

            # очищаем цель если нужно
            if self.truncate_before_load:
                self._truncate_target(target_conn)

            # получаем список колонок для COPY
            column_names = source_hook.get_column_names(
                self.source_schema, self.source_table
            )
            logger.info("Колонки источника: %s", column_names)

            # читаем и пишем чанками
            for chunk in source_hook.read_table_in_chunks(
                schema=self.source_schema,
                table=self.source_table,
                columns=self.columns,
                where_clause=self.where_clause,
            ):
                rows_extracted += len(chunk)
                loaded = self._load_chunk_via_copy(target_conn, chunk, column_names)
                rows_loaded += loaded
                logger.info(
                    "Чанк загружен извлечено=%d загружено=%d",
                    rows_extracted,
                    rows_loaded,
                )

            target_conn.commit()
            status = "success"
            logger.info(
                "Загрузка завершена %s -> %s.%s всего строк: %d",
                source_info,
                self.target_schema,
                self.target_table,
                rows_loaded,
            )

        except Exception as exc:
            status = "failed"
            error_message = str(exc)[:2000]
            logger.error("Ошибка при загрузке: %s", exc)
            if target_conn and not target_conn.closed:
                target_conn.rollback()
            raise

        finally:
            finished_at = datetime.now(timezone.utc)
            self._write_etl_log(
                context, started_at, status, rows_extracted, rows_loaded,
                error_message, finished_at,
            )
            source_hook.close()
            if target_conn and not target_conn.closed:
                target_conn.close()

        return {
            "status": status,
            "rows_extracted": rows_extracted,
            "rows_loaded": rows_loaded,
        }

    def _truncate_target(self, conn) -> None:
        """Очистить целевую таблицу перед загрузкой"""
        fqn = f"{self.target_schema}.{self.target_table}"
        logger.info("Очистка целевой таблицы %s", fqn)
        with conn.cursor() as cur:
            cur.execute(f"TRUNCATE TABLE {fqn}")  

    def _load_chunk_via_copy(
        self, conn, rows: list[tuple], column_names: list[str]
    ) -> int:
        """Загрузить чанк данных через PostgreSQL COPY FROM STDIN"""
        if not rows:
            return 0

        fqn = f"{self.target_schema}.{self.target_table}"
        cols = ", ".join(column_names)

        buf = io.StringIO()
        writer = csv.writer(buf, delimiter="\t", lineterminator="\n")
        for row in rows:
            writer.writerow(
                ["\\N" if val is None else val for val in row]
            )
        buf.seek(0)

        with conn.cursor() as cur:
            cur.copy_expert(
                f"COPY {fqn} ({cols}) FROM STDIN WITH (FORMAT text, NULL '\\N')",
                buf,
            )

        return len(rows)

    def _write_etl_log(
        self,
        context: dict[str, Any],
        started_at: datetime,
        status: str,
        rows_extracted: int,
        rows_loaded: int,
        error_message: str | None,
        finished_at: datetime | None = None,
    ) -> None:
        """Записать строку в таблицу etl_log"""
        try:
            conn = psycopg2.connect(
                host=self.target_host,
                port=self.target_port,
                dbname=self.target_database,
                user=self.target_user,
                password=self.target_password,
            )
            conn.autocommit = True

            duration_sec = None
            if finished_at and started_at:
                duration_sec = (finished_at - started_at).total_seconds()

            ti = context.get("task_instance")
            dag_id = ti.dag_id if ti else None
            task_id = ti.task_id if ti else None
            run_id = context.get("run_id")

            with conn.cursor() as cur:
                cur.execute(
                    f"""
                    INSERT INTO {self.etl_log_schema}.{self.etl_log_table}
                        (dag_id, task_id, run_id,
                         source_host, source_schema, source_table,
                         target_schema, target_table,
                         status, rows_extracted, rows_loaded,
                         error_message, started_at, finished_at, duration_sec)
                    VALUES
                        (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """,
                    (
                        dag_id, task_id, run_id,
                        self.source_host, self.source_schema, self.source_table,
                        self.target_schema, self.target_table,
                        status, rows_extracted, rows_loaded,
                        error_message, started_at, finished_at, duration_sec,
                    ),
                )
            conn.close()

        except Exception as log_exc:
            # ошибки логирования не должны ломать основной ETL
            logger.warning("Не удалось записать строку в etl_log: %s", log_exc)
