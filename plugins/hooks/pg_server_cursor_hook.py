"""Hook для чтения PostgreSQL таблиц чанками через server-side cursor"""

from __future__ import annotations

import logging
from contextlib import contextmanager
from typing import Any, Generator, Iterator

import psycopg2
import psycopg2.extras

logger = logging.getLogger(__name__)


class PgServerCursorHook:
    """Подключение к PostgreSQL и чтение чанками"""

    DEFAULT_CHUNK_SIZE = 10_000

    def __init__(
        self,
        host: str,
        port: int,
        database: str,
        user: str,
        password: str,
        chunk_size: int = DEFAULT_CHUNK_SIZE,
        connect_timeout: int = 30,
    ):
        self.host = host
        self.port = port
        self.database = database
        self.user = user
        self.password = password
        self.chunk_size = chunk_size
        self.connect_timeout = connect_timeout
        self._conn = None

    # управление соединением

    def get_conn(self):
        """Создать или вернуть соединение"""
        if self._conn is None or self._conn.closed:
            logger.info(
                "Подключение к PostgreSQL %s:%s/%s пользователем %s",
                self.host,
                self.port,
                self.database,
                self.user,
            )
            self._conn = psycopg2.connect(
                host=self.host,
                port=self.port,
                dbname=self.database,
                user=self.user,
                password=self.password,
                connect_timeout=self.connect_timeout,
            )
        return self._conn

    def close(self):
        """Закрыть соединение"""
        if self._conn and not self._conn.closed:
            self._conn.close()
            logger.info("Соединение с PostgreSQL закрыто")

    @contextmanager
    def server_cursor(self, name: str = "etl_cursor"):
        """Контекстный менеджер для server-side cursor"""
        conn = self.get_conn()
        cursor = conn.cursor(name=name, cursor_factory=psycopg2.extras.DictCursor)
        cursor.itersize = self.chunk_size
        try:
            yield cursor
        finally:
            cursor.close()

    def read_table_in_chunks(
        self,
        schema: str,
        table: str,
        columns: str = "*",
        where_clause: str | None = None,
    ) -> Generator[list[tuple[Any, ...]], None, int]:
        """Читать таблицу чанками через server-side cursor"""
        query = f"SELECT {columns} FROM {schema}.{table}"  # noqa: S608
        if where_clause:
            query += f" WHERE {where_clause}"

        total_rows = 0

        with self.server_cursor() as cursor:
            logger.info("Выполнение запроса: %s", query)
            cursor.execute(query)

            while True:
                rows = cursor.fetchmany(self.chunk_size)
                if not rows:
                    break
                total_rows += len(rows)
                logger.debug("Получен чанк: %d строк (всего: %d)", len(rows), total_rows)
                yield rows

        logger.info("Чтение %s.%s завершено всего строк: %d", schema, table, total_rows)
        return total_rows

    def get_column_names(self, schema: str, table: str) -> list[str]:
        """Получить список колонок таблицы"""
        conn = self.get_conn()
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT column_name
                FROM information_schema.columns
                WHERE table_schema = %s AND table_name = %s
                ORDER BY ordinal_position
                """,
                (schema, table),
            )
            return [row[0] for row in cur.fetchall()]

    def get_row_count(self, schema: str, table: str) -> int:
        """Получить приблизительное количество строк (через pg_class)"""
        conn = self.get_conn()
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT reltuples::BIGINT
                FROM pg_class c
                JOIN pg_namespace n ON n.oid = c.relnamespace
                WHERE n.nspname = %s AND c.relname = %s
                """,
                (schema, table),
            )
            result = cur.fetchone()
            return result[0] if result else 0
