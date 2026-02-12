-- создание таблицы etl_log для логов ETL-задач

CREATE TABLE IF NOT EXISTS public.etl_log (
    id              BIGSERIAL PRIMARY KEY,
    dag_id          VARCHAR(250),
    task_id         VARCHAR(250),
    run_id          VARCHAR(250),
    source_host     VARCHAR(500),
    source_schema   VARCHAR(250),
    source_table    VARCHAR(250),
    target_schema   VARCHAR(250),
    target_table    VARCHAR(250),
    status          VARCHAR(50)     NOT NULL DEFAULT 'running',
    rows_extracted  BIGINT          DEFAULT 0,
    rows_loaded     BIGINT          DEFAULT 0,
    error_message   TEXT,
    started_at      TIMESTAMP WITH TIME ZONE,
    finished_at     TIMESTAMP WITH TIME ZONE,
    duration_sec    NUMERIC(12, 3)
);

-- Индексы для типичных запросов мониторинга
CREATE INDEX IF NOT EXISTS idx_etl_log_status
    ON public.etl_log (status);

CREATE INDEX IF NOT EXISTS idx_etl_log_started_at
    ON public.etl_log (started_at DESC);

CREATE INDEX IF NOT EXISTS idx_etl_log_source
    ON public.etl_log (source_host, source_schema, source_table);

CREATE INDEX IF NOT EXISTS idx_etl_log_dag
    ON public.etl_log (dag_id, task_id);

COMMENT ON TABLE public.etl_log IS 'Структурированный лог ETL-задач';
COMMENT ON COLUMN public.etl_log.status IS 'running | success | failed';
COMMENT ON COLUMN public.etl_log.duration_sec IS 'Длительность выполнения в секундах';
