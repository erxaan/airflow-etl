"""Оповещения о падении задач Airflow в Telegram / Slack / Email"""

from __future__ import annotations

import logging
import smtplib
from datetime import datetime, timezone
from email.mime.text import MIMEText
from typing import Any

import requests

logger = logging.getLogger(__name__)


def on_task_failure_callback(context: dict[str, Any]) -> None:
    """on_failure callback: сформировать сообщение и отправить в выбранный канал"""
    ti = context.get("task_instance")
    exception = context.get("exception", "Unknown error")

    dag_id = ti.dag_id if ti else "unknown"
    task_id = ti.task_id if ti else "unknown"
    execution_date = context.get("execution_date", datetime.now(timezone.utc))
    start_date = ti.start_date if ti else None
    log_url = ti.log_url if ti else ""

    # source_info из XCom
    source_info = "N/A"
    if ti:
        try:
            source_info = ti.xcom_pull(key="source_info") or "N/A"
        except Exception:
            pass

    message = _format_message(
        dag_id=dag_id,
        task_id=task_id,
        source_info=source_info,
        execution_date=execution_date,
        start_date=start_date,
        error=str(exception),
        log_url=log_url,
    )

    # выбираем канал оповещения
    channel = _get_variable("alert_channel", "log")

    if channel == "telegram":
        _send_telegram(message)
    elif channel == "slack":
        _send_slack(message)
    elif channel == "email":
        _send_email(message, dag_id)
    else:
        # если канал не настроен логируем
        logger.warning("Алерт не отправлен канал не настроен\n%s", message)


def _format_message(
    dag_id: str,
    task_id: str,
    source_info: str,
    execution_date,
    start_date,
    error: str,
    log_url: str,
) -> str:
    """Собрать текст сообщения о сбое"""
    return (
        f"СБОЙ ETL ЗАДАЧИ\n"
        f"{'=' * 40}\n"
        f"DAG:        {dag_id}\n"
        f"Задача:     {task_id}\n"
        f"Источник:   {source_info}\n"
        f"Запуск:     {execution_date}\n"
        f"Старт:      {start_date or 'N/A'}\n"
        f"Падение:    {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')}\n"
        f"Ошибка:     {error[:500]}\n"
        f"Логи:       {log_url}\n"
    )


def _send_telegram(message: str) -> None:
    """Отправить сообщение в Telegram"""
    token = _get_variable("alert_telegram_token")
    chat_id = _get_variable("alert_telegram_chat_id")

    if not token or not chat_id:
        logger.warning("Не настроен Telegram алерт нет токена или chat_id")
        return

    url = f"https://api.telegram.org/bot{token}/sendMessage"
    payload = {
        "chat_id": chat_id,
        "text": message,
        "parse_mode": "HTML",
    }

    try:
        resp = requests.post(url, json=payload, timeout=10)
        resp.raise_for_status()
        logger.info("Telegram алерт отправлен в чат %s", chat_id)
    except Exception as exc:
        logger.error("Не удалось отправить Telegram алерт: %s", exc)


def _send_slack(message: str) -> None:
    """Отправить сообщение в Slack"""
    webhook_url = _get_variable("alert_slack_webhook_url")

    if not webhook_url:
        logger.warning("Не настроен Slack алерт нет webhook URL")
        return

    payload = {"text": message}

    try:
        resp = requests.post(webhook_url, json=payload, timeout=10)
        resp.raise_for_status()
        logger.info("Slack алерт отправлен")
    except Exception as exc:
        logger.error("Не удалось отправить Slack алерт: %s", exc)


def _send_email(message: str, subject_suffix: str = "") -> None:
    """Отправить письмо по SMTP"""
    email_to = _get_variable("alert_email_to")
    smtp_host = _get_variable("alert_smtp_host", "localhost")
    smtp_port = int(_get_variable("alert_smtp_port", "25"))
    smtp_user = _get_variable("alert_smtp_user", "")
    smtp_password = _get_variable("alert_smtp_password", "")
    email_from = _get_variable("alert_email_from", "etl-alerts@company.com")

    if not email_to:
        logger.warning("Не настроен Email алерт нет alert_email_to")
        return

    msg = MIMEText(message)
    msg["Subject"] = f"ETL Failure: {subject_suffix}"
    msg["From"] = email_from
    msg["To"] = email_to

    try:
        with smtplib.SMTP(smtp_host, smtp_port, timeout=10) as server:
            if smtp_user and smtp_password:
                server.starttls()
                server.login(smtp_user, smtp_password)
            server.sendmail(email_from, [email_to], msg.as_string())
        logger.info("Email алерт отправлен на %s", email_to)
    except Exception as exc:
        logger.error("Не удалось отправить Email алерт: %s", exc)


def _get_variable(key: str, default: str = "") -> str:
    """Получить Airflow Variable с дефолтом"""
    try:
        from airflow.models import Variable
        return Variable.get(key, default_var=default)
    except Exception:
        return default
