"""
Тесты целостности DAG

Проверяют, что все DAG корректно парсятся Airflow и не содержат ошибок
Выполняются в CI при каждом пуше
"""

import os
import sys
import pytest

# корень проекта в sys.path
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)


@pytest.fixture(autouse=True)
def airflow_env(tmp_path):
    """минимальное окружение Airflow для тестов"""
    os.environ.setdefault("AIRFLOW_HOME", str(tmp_path))
    os.environ.setdefault("AIRFLOW__CORE__LOAD_EXAMPLES", "false")
    os.environ.setdefault(
        "AIRFLOW__CORE__SQL_ALCHEMY_CONN", "sqlite:///:memory:"
    )


class TestDAGIntegrity:
    """Тесты целостности DAG-файлов"""

    def test_dag_bag_has_no_import_errors(self):
        """Все DAG-файлы парсятся без ошибок"""
        from airflow.models import DagBag

        dag_folder = os.path.join(PROJECT_ROOT, "dags")
        bag = DagBag(dag_folder=dag_folder, include_examples=False)

        assert len(bag.import_errors) == 0, (
            f"DAG import errors:\n"
            + "\n".join(
                f"  {path}: {err}" for path, err in bag.import_errors.items()
            )
        )

    def test_dags_are_generated_from_config(self):
        """Генератор создаёт DAG для каждого источника из конфигурации"""
        import yaml
        from airflow.models import DagBag

        dag_folder = os.path.join(PROJECT_ROOT, "dags")
        config_path = os.path.join(dag_folder, "config", "sources.yaml")

        with open(config_path, "r", encoding="utf-8") as f:
            config = yaml.safe_load(f)

        bag = DagBag(dag_folder=dag_folder, include_examples=False)
        sources = config.get("sources", [])

        for source in sources:
            dag_id = f"etl_{source['name']}"
            assert dag_id in bag.dags, (
                f"DAG '{dag_id}' not found. Available: {list(bag.dags.keys())}"
            )

    def test_each_dag_has_tasks(self):
        """Каждый сгенерированный DAG содержит хотя бы одну задачу"""
        from airflow.models import DagBag

        dag_folder = os.path.join(PROJECT_ROOT, "dags")
        bag = DagBag(dag_folder=dag_folder, include_examples=False)

        for dag_id, dag in bag.dags.items():
            if dag_id.startswith("etl_"):
                assert len(dag.tasks) > 0, f"DAG '{dag_id}' has no tasks"

    def test_each_etl_task_has_dq_successor(self):
        """Каждая ETL-задача (load_*) имеет задачу проверки качества (dq_*)"""
        from airflow.models import DagBag

        dag_folder = os.path.join(PROJECT_ROOT, "dags")
        bag = DagBag(dag_folder=dag_folder, include_examples=False)

        for dag_id, dag in bag.dags.items():
            if not dag_id.startswith("etl_"):
                continue

            load_tasks = [t for t in dag.tasks if t.task_id.startswith("load_")]
            for load_task in load_tasks:
                downstream_ids = [t.task_id for t in load_task.downstream_list]
                dq_task_id = load_task.task_id.replace("load_", "dq_", 1)
                assert dq_task_id in downstream_ids, (
                    f"Load task '{load_task.task_id}' in DAG '{dag_id}' "
                    f"is missing DQ successor '{dq_task_id}'"
                )

    def test_dag_tags_include_etl(self):
        """Все ETL DAG имеют тег 'etl'"""
        from airflow.models import DagBag

        dag_folder = os.path.join(PROJECT_ROOT, "dags")
        bag = DagBag(dag_folder=dag_folder, include_examples=False)

        for dag_id, dag in bag.dags.items():
            if dag_id.startswith("etl_"):
                assert "etl" in dag.tags, (
                    f"DAG '{dag_id}' is missing 'etl' tag"
                )
