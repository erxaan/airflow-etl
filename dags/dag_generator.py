"""Генерация DAG'ов из конфигурации источников"""

from __future__ import annotations

import json
import logging
import os
from datetime import timedelta
from pathlib import Path

import yaml
import jsonschema
from airflow import DAG
from airflow.utils.dates import days_ago

from plugins.operators.chunked_pg_operator import ChunkedPgToPgOperator
from plugins.callbacks.alerting import on_task_failure_callback
from plugins.validators.data_quality import DataQualityCheckOperator

logger = logging.getLogger(__name__)

# пути к файлам конфигурации
CONFIG_DIR = Path(__file__).parent / "config"
SOURCES_PATH = CONFIG_DIR / "sources.yaml"
SCHEMA_PATH = CONFIG_DIR / "sources_schema.json"

def _load_config() -> dict:
    """Загрузить и провалидировать конфиг источников"""
    if not SOURCES_PATH.exists():
        logger.error("Файл конфигурации не найден: %s", SOURCES_PATH)
        return {}

    with open(SOURCES_PATH, "r", encoding="utf-8") as f:
        config = yaml.safe_load(f)

    # валидация по JSON Schema
    if SCHEMA_PATH.exists():
        with open(SCHEMA_PATH, "r", encoding="utf-8") as f:
            schema = json.load(f)
        try:
            jsonschema.validate(config, schema)
        except jsonschema.ValidationError as exc:
            logger.error("Ошибка валидации конфигурации: %s", exc.message)
            return {}

    return config


def _resolve_value(value: str) -> str:
    """Подставить значение Airflow Variable в строку конфига если используется {{ var.value.KEY }}"""
    if not isinstance(value, str):
        return value
    if "{{ var.value." in value:
        # достаём имя переменной
        import re
        match = re.search(r"\{\{\s*var\.value\.(\w+)\s*\}\}", value)
        if match:
            var_name = match.group(1)
            try:
                from airflow.models import Variable
                return Variable.get(var_name, default_var=value)
            except Exception:
                logger.warning("Не удалось получить Airflow Variable: %s", var_name)
                return value
    return value


config = _load_config()

if config:
    target = config["target"]
    defaults = config.get("defaults", {})
    sources = config.get("sources", [])

    for source in sources:
        source_name = source["name"]
        dag_id = f"etl_{source_name}"

        schedule = source.get(
            "schedule_interval",
            defaults.get("schedule_interval", "@daily"),
        )
        retries = source.get("retries", defaults.get("retries", 2))
        retry_delay = source.get(
            "retry_delay_minutes", defaults.get("retry_delay_minutes", 5)
        )
        sla_minutes = source.get("sla_minutes", defaults.get("sla_minutes", 60))

        default_args = {
            "owner": "etl",
            "depends_on_past": False,
            "start_date": days_ago(1),
            "retries": retries,
            "retry_delay": timedelta(minutes=retry_delay),
            "on_failure_callback": on_task_failure_callback,
            "sla": timedelta(minutes=sla_minutes),
        }

        dag = DAG(
            dag_id=dag_id,
            default_args=default_args,
            description=source.get("description", f"ETL for {source_name}"),
            schedule_interval=schedule,
            catchup=False,
            max_active_runs=1,
            tags=["etl", source_name],
        )

        source_password = _resolve_value(source["password"])

        for table_cfg in source["tables"]:
            task_id = f"load_{table_cfg['source_schema']}_{table_cfg['source_table']}"

            chunk_size = table_cfg.get(
                "chunk_size", defaults.get("chunk_size", 10_000)
            )
            truncate = table_cfg.get(
                "truncate_before_load",
                defaults.get("truncate_before_load", True),
            )

            # задача Extract + Load
            etl_task = ChunkedPgToPgOperator(
                task_id=task_id,
                source_host=source["host"],
                source_port=source["port"],
                source_database=source["database"],
                source_user=source["user"],
                source_password=source_password,
                source_schema=table_cfg["source_schema"],
                source_table=table_cfg["source_table"],
                target_host=target["host"],
                target_port=target["port"],
                target_database=target["database"],
                target_user=target["user"],
                target_password=target["password"],
                target_schema=table_cfg["target_schema"],
                target_table=table_cfg["target_table"],
                chunk_size=chunk_size,
                truncate_before_load=truncate,
                columns=table_cfg.get("columns", "*"),
                where_clause=table_cfg.get("where_clause"),
                dag=dag,
            )

            # задача проверки качества
            dq_task_id = f"dq_{table_cfg['source_schema']}_{table_cfg['source_table']}"
            dq_task = DataQualityCheckOperator(
                task_id=dq_task_id,
                source_host=source["host"],
                source_port=source["port"],
                source_database=source["database"],
                source_user=source["user"],
                source_password=source_password,
                source_schema=table_cfg["source_schema"],
                source_table=table_cfg["source_table"],
                target_host=target["host"],
                target_port=target["port"],
                target_database=target["database"],
                target_user=target["user"],
                target_password=target["password"],
                target_schema=table_cfg["target_schema"],
                target_table=table_cfg["target_table"],
                dag=dag,
            )

            etl_task >> dq_task

        # регистрируем DAG чтобы Airflow его увидел
        globals()[dag_id] = dag
