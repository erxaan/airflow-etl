"""Тесты валидации YAML-конфигурации ETL-источников"""

import json
import os
import sys

import pytest
import yaml
import jsonschema

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)


CONFIG_PATH = os.path.join(PROJECT_ROOT, "dags", "config", "sources.yaml")
SCHEMA_PATH = os.path.join(PROJECT_ROOT, "dags", "config", "sources_schema.json")


@pytest.fixture
def config():
    with open(CONFIG_PATH, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


@pytest.fixture
def schema():
    with open(SCHEMA_PATH, "r", encoding="utf-8") as f:
        return json.load(f)


class TestConfigValidation:
    """Проверка структуры и содержимого конфигурации"""

    def test_config_matches_schema(self, config, schema):
        """Конфигурация проходит валидацию по JSON Schema"""
        jsonschema.validate(config, schema)

    def test_config_has_target(self, config):
        """Конфигурация содержит секцию target"""
        assert "target" in config
        assert "host" in config["target"]
        assert "database" in config["target"]

    def test_config_has_sources(self, config):
        """Конфигурация содержит хотя бы один источник"""
        assert "sources" in config
        assert len(config["sources"]) > 0

    def test_each_source_has_tables(self, config):
        """Каждый источник содержит хотя бы одну таблицу"""
        for source in config["sources"]:
            assert "tables" in source, f"Source '{source['name']}' has no tables"
            assert len(source["tables"]) > 0

    def test_source_names_are_unique(self, config):
        """Имена источников уникальны"""
        names = [s["name"] for s in config["sources"]]
        assert len(names) == len(set(names)), f"Duplicate source names: {names}"

    def test_source_names_are_snake_case(self, config):
        """Имена источников в формате snake_case"""
        import re
        pattern = re.compile(r"^[a-z][a-z0-9_]*$")
        for source in config["sources"]:
            assert pattern.match(source["name"]), (
                f"Source name '{source['name']}' is not snake_case"
            )

    def test_invalid_config_fails_validation(self, schema):
        """Невалидная конфигурация не проходит валидацию"""
        invalid_config = {"target": {"host": "x"}}  # отсутствуют обязательные поля
        with pytest.raises(jsonschema.ValidationError):
            jsonschema.validate(invalid_config, schema)
