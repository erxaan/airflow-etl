# ETL-процесс на базе Apache Airflow для переноса данных из учётных систем в PostgreSQL-хранилище


## Возможности

- **Чанковая загрузка** чтение таблиц через server-side cursor и загрузка в DWH через COPY
- **Структурированные логи** таблица `etl_log` с доступом через SQL
- **Уведомления о сбоях** Email / Telegram / Slack по on_failure callback
- **CI/CD** GitHub Actions линтинг, тесты, сборка образа, деплой
- **Динамические DAG** источники и таблицы в YAML-конфиге
- **Проверки качества данных** сравнение количества строк, проверка NULL в PK, контроль схемы


## Быстрый старт

### Запуск 

```bash
docker compose up -d
```
Airflow Web UI: [http://localhost:8080](http://localhost:8080) (admin / admin)

### Настройка Airflow Variables

В Variables добавить:

| Key | Описание |
|-----|----------|
| `accounting_password` | Пароль для подключения к учётной системе |
| `warehouse_password` | Пароль для подключения к системе склада |
| `crm_password` | Пароль для подключения к CRM |
| `alert_channel` | Канал уведомлений: `telegram` / `slack` / `email` / `log` |
| `alert_telegram_token` | Токен Telegram бота |
| `alert_telegram_chat_id` | ID чата Telegram |

### Добавление нового источника

Отредактировать `dags/config/sources.yaml`:

```yaml
sources:
  - name: "new_system"
    host: "new-db.internal"
    port: 5432
    database: "new_db"
    user: "etl_reader"
    password: "{{ var.value.new_system_password }}"
    tables:
      - source_schema: "public"
        source_table: "orders"
        target_schema: "new"
        target_table: "orders"
```

DAG будет создан автоматически с id `etl_new_system`.

## Мониторинг

### Airflow Web UI
- Статусы DAG и задач
- Графы зависимостей
- История запусков
- Логи задач


