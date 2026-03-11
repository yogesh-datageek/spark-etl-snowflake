"""
Unit tests for utility modules.

Run:  pytest tests/ -v
"""

from __future__ import annotations

import os
import json
import tempfile
from pathlib import Path

import pytest


# ==================================================================
#  Config Loader
# ==================================================================
class TestConfigLoader:
    """Tests for src.utils.config_loader."""

    @staticmethod
    def _write_yaml(tmpdir: Path, name: str, content: str) -> Path:
        p = tmpdir / name
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_text(content)
        return p

    def test_basic_load(self, tmp_path: Path):
        base = self._write_yaml(
            tmp_path,
            "pipeline_config.yaml",
            "pipeline:\n  name: test\n  environment: dev\n",
        )
        from src.utils.config_loader import load_config

        cfg = load_config(base_path=base, env="dev")
        assert cfg["pipeline"]["name"] == "test"

    def test_env_override(self, tmp_path: Path):
        self._write_yaml(
            tmp_path,
            "pipeline_config.yaml",
            "pipeline:\n  name: test\n  environment: dev\n  log_level: INFO\n",
        )
        self._write_yaml(
            tmp_path / "env",
            "staging.yaml",
            "pipeline:\n  log_level: WARNING\n",
        )
        from src.utils.config_loader import load_config

        cfg = load_config(base_path=tmp_path / "pipeline_config.yaml", env="staging")
        assert cfg["pipeline"]["log_level"] == "WARNING"
        assert cfg["pipeline"]["name"] == "test"  # not overridden

    def test_env_var_interpolation(self, tmp_path: Path, monkeypatch):
        monkeypatch.setenv("MY_HOST", "db.prod.internal")
        base = self._write_yaml(
            tmp_path,
            "pipeline_config.yaml",
            'pipeline:\n  environment: dev\npostgres:\n  host: "${MY_HOST:-localhost}"\n',
        )
        from src.utils.config_loader import load_config

        cfg = load_config(base_path=base, env="dev")
        assert cfg["postgres"]["host"] == "db.prod.internal"

    def test_env_var_default(self, tmp_path: Path, monkeypatch):
        monkeypatch.delenv("MISSING_VAR", raising=False)
        base = self._write_yaml(
            tmp_path,
            "pipeline_config.yaml",
            'pipeline:\n  environment: dev\npostgres:\n  host: "${MISSING_VAR:-fallback}"\n',
        )
        from src.utils.config_loader import load_config

        cfg = load_config(base_path=base, env="dev")
        assert cfg["postgres"]["host"] == "fallback"


# ==================================================================
#  Schema Helper
# ==================================================================
class TestSchemaHelper:
    def test_load_schema(self, tmp_path: Path):
        schema_file = tmp_path / "test.json"
        schema_file.write_text(json.dumps({
            "type": "struct",
            "fields": [
                {"name": "id", "type": "integer", "nullable": False},
                {"name": "name", "type": "string", "nullable": True},
            ],
        }))
        from src.utils.schema_helper import load_schema

        st = load_schema(schema_file)
        assert len(st.fields) == 2
        assert st.fields[0].name == "id"
        assert st.fields[1].nullable is True


# ==================================================================
#  Retry
# ==================================================================
class TestRetry:
    def test_succeeds_first_try(self):
        from src.utils.retry import retry

        call_count = 0

        @retry(max_attempts=3, backoff_seconds=0.01)
        def good():
            nonlocal call_count
            call_count += 1
            return "ok"

        assert good() == "ok"
        assert call_count == 1

    def test_retries_then_succeeds(self):
        from src.utils.retry import retry

        attempts = 0

        @retry(max_attempts=3, backoff_seconds=0.01)
        def flaky():
            nonlocal attempts
            attempts += 1
            if attempts < 3:
                raise ValueError("not yet")
            return "done"

        assert flaky() == "done"
        assert attempts == 3

    def test_exhausts_retries(self):
        from src.utils.retry import retry

        @retry(max_attempts=2, backoff_seconds=0.01)
        def always_fail():
            raise RuntimeError("boom")

        with pytest.raises(RuntimeError, match="boom"):
            always_fail()


# ==================================================================
#  Logger
# ==================================================================
class TestLogger:
    def test_json_output(self, capsys):
        from src.utils.logger import get_logger

        log = get_logger("test_json", level="DEBUG")
        log.info("hello %s", "world")
        captured = capsys.readouterr()
        data = json.loads(captured.err.strip())
        assert data["message"] == "hello world"
        assert data["level"] == "INFO"
