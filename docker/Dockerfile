FROM apache/airflow:2.9.3-python3.11

USER root

# psycopg2
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        build-essential \
        libpq-dev && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

USER airflow

# Python зависимости
COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

#Airflow
COPY plugins/ ${AIRFLOW_HOME}/plugins/

# DAG Airflow
COPY dags/ ${AIRFLOW_HOME}/dags/

# миграции (для ручного запуска внутри контейнера)
COPY migrations/ ${AIRFLOW_HOME}/migrations/
