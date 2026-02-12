"""Проверка качества данных после загрузки (row count, NULL, дрифт схемы)"""

from __future__ import annotations

import logging
from typing import Any

import psycopg2
from airflow.models import BaseOperator
from airflow.exceptions import AirflowException

logger = logging.getLogger(__name__)


class DataQualityCheckOperator(BaseOperator):
    """Проверка количества строк, PK-NULL и схемы"""

    template_fields = (
        "source_schema",
        "source_table",
        "target_schema",
        "target_table",
    )

    def __init__(
        self,
        *,
        source_host: str,
        source_port: int,
        source_database: str,
        source_user: str,
        source_password: str,
        source_schema: str,
        source_table: str,
        target_host: str,
        target_port: int,
        target_database: str,
        target_user: str,
        target_password: str,
        target_schema: str,
        target_table: str,
        row_count_tolerance: float = 0.0,
        check_nulls: bool = True,
        check_schema_drift: bool = True,
        **kwargs,
    ):
        super().__init__(**kwargs)

        self.source_host = source_host
        self.source_port = source_port
        self.source_database = source_database
        self.source_user = source_user
        self.source_password = source_password
        self.source_schema = source_schema
        self.source_table = source_table

        self.target_host = target_host
        self.target_port = target_port
        self.target_database = target_database
        self.target_user = target_user
        self.target_password = target_password
        self.target_schema = target_schema
        self.target_table = target_table

        self.row_count_tolerance = row_count_tolerance
        self.check_nulls = check_nulls
        self.check_schema_drift = check_schema_drift

    def execute(self, context: dict[str, Any]) -> dict[str, Any]:
        """Выполнить проверки и вернуть результаты"""
        results = {}

        source_conn = psycopg2.connect(
            host=self.source_host,
            port=self.source_port,
            dbname=self.source_database,
            user=self.source_user,
            password=self.source_password,
        )
        target_conn = psycopg2.connect(
            host=self.target_host,
            port=self.target_port,
            dbname=self.target_database,
            user=self.target_user,
            password=self.target_password,
        )

        try:
            # проверка количества строк
            results["row_count"] = self._check_row_count(source_conn, target_conn)

            # проверка NULL в PK
            if self.check_nulls:
                results["null_check"] = self._check_nulls_in_pk(target_conn)

            # проверка дрифта схемы
            if self.check_schema_drift:
                results["schema_drift"] = self._check_schema_drift(
                    source_conn, target_conn
                )

            all_passed = all(r.get("passed", False) for r in results.values())

            if all_passed:
                logger.info(
                    "Проверки качества данных успешно пройдены для %s.%s",
                    self.target_schema,
                    self.target_table,
                )
            else:
                failed = [k for k, v in results.items() if not v.get("passed")]
                msg = (
                    f"Проверки качества данных не пройдены для "
                    f"{self.target_schema}.{self.target_table}: {failed}"
                )
                logger.error(msg)
                raise AirflowException(msg)

        finally:
            source_conn.close()
            target_conn.close()

        return results

    def _check_row_count(self, source_conn, target_conn) -> dict:
        """Проверить совпадение количества строк"""
        source_count = self._get_count(
            source_conn, self.source_schema, self.source_table
        )
        target_count = self._get_count(
            target_conn, self.target_schema, self.target_table
        )

        diff = abs(source_count - target_count)
        tolerance = int(source_count * self.row_count_tolerance) if source_count else 0
        passed = diff <= tolerance

        result = {
            "passed": passed,
            "source_count": source_count,
            "target_count": target_count,
            "difference": diff,
        }

        if passed:
            logger.info(
                "Проверка количества строк пройдена источник=%d цель=%d",
                source_count,
                target_count,
            )
        else:
            logger.error(
                "Проверка количества строк не пройдена источник=%d цель=%d разница=%d",
                source_count,
                target_count,
                diff,
            )

        return result

    def _check_nulls_in_pk(self, target_conn) -> dict:
        """Проверить наличие NULL в PK-колонках целевой таблицы"""
        pk_columns = self._get_pk_columns(target_conn)

        if not pk_columns:
            logger.info(
                "PK колонки не найдены для %s.%s — пропускаем null check",
                self.target_schema,
                self.target_table,
            )
            return {"passed": True, "skipped": True}

        null_counts = {}
        with target_conn.cursor() as cur:
            for col in pk_columns:
                cur.execute(
                    f"SELECT COUNT(*) FROM {self.target_schema}.{self.target_table} "
                    f"WHERE {col} IS NULL"
                )
                count = cur.fetchone()[0]
                if count > 0:
                    null_counts[col] = count

        passed = len(null_counts) == 0
        result = {"passed": passed, "pk_columns": pk_columns, "null_counts": null_counts}

        if passed:
            logger.info("Проверка NULL в PK колонках пройдена")
        else:
            logger.error("Проверка NULL в PK колонках не пройдена: %s", null_counts)

        return result

    def _check_schema_drift(self, source_conn, target_conn) -> dict:
        """Проверить совпадение набора колонок между источником и целью"""
        source_cols = set(
            self._get_columns(source_conn, self.source_schema, self.source_table)
        )
        target_cols = set(
            self._get_columns(target_conn, self.target_schema, self.target_table)
        )

        missing_in_target = source_cols - target_cols
        extra_in_target = target_cols - source_cols

        passed = len(missing_in_target) == 0

        result = {
            "passed": passed,
            "source_columns": sorted(source_cols),
            "target_columns": sorted(target_cols),
            "missing_in_target": sorted(missing_in_target),
            "extra_in_target": sorted(extra_in_target),
        }

        if passed:
            logger.info("Дрифт схемы не обнаружен")
        else:
            logger.error(
                "Обнаружен дрифт схемы отсутствуют колонки в цели: %s",
                missing_in_target,
            )

        return result

    @staticmethod
    def _get_count(conn, schema: str, table: str) -> int:
        with conn.cursor() as cur:
            cur.execute(f"SELECT COUNT(*) FROM {schema}.{table}")
            return cur.fetchone()[0]

    def _get_pk_columns(self, conn) -> list[str]:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT a.attname
                FROM pg_index i
                JOIN pg_attribute a ON a.attrelid = i.indrelid
                    AND a.attnum = ANY(i.indkey)
                JOIN pg_class c ON c.oid = i.indrelid
                JOIN pg_namespace n ON n.oid = c.relnamespace
                WHERE i.indisprimary
                    AND n.nspname = %s
                    AND c.relname = %s
                """,
                (self.target_schema, self.target_table),
            )
            return [row[0] for row in cur.fetchall()]

    @staticmethod
    def _get_columns(conn, schema: str, table: str) -> list[str]:
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
