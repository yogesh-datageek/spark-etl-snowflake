"""
Centralised configuration loader.

Reads the base pipeline_config.yaml, deep-merges any environment override
file, and interpolates ${ENV_VAR:-default} placeholders from os.environ.
"""

from __future__ import annotations

import copy
import os
import re
from pathlib import Path
from typing import Any

import yaml


_ENV_VAR_PATTERN = re.compile(
    r"\$\{(?P<var>[A-Za-z_][A-Za-z0-9_]*)(?::-(?P<default>[^}]*))?\}"
)


def _interpolate(value: Any) -> Any:
    """Recursively replace ${VAR:-default} tokens with environment values."""
    if isinstance(value, str):
        def _replacer(match: re.Match) -> str:
            env_key = match.group("var")
            default = match.group("default") or ""
            return os.environ.get(env_key, default)

        result = _ENV_VAR_PATTERN.sub(_replacer, value)
        # Cast pure-numeric strings back to int so YAML semantics are preserved
        if result.isdigit():
            return int(result)
        return result
    if isinstance(value, dict):
        return {k: _interpolate(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_interpolate(item) for item in value]
    return value


def _deep_merge(base: dict, override: dict) -> dict:
    """Recursively merge *override* into a copy of *base*."""
    merged = copy.deepcopy(base)
    for key, val in override.items():
        if key in merged and isinstance(merged[key], dict) and isinstance(val, dict):
            merged[key] = _deep_merge(merged[key], val)
        else:
            merged[key] = copy.deepcopy(val)
    return merged


def load_config(
    base_path: str | Path = "config/pipeline_config.yaml",
    env: str | None = None,
) -> dict:
    """
    Load and return the fully-resolved configuration dictionary.

    Parameters
    ----------
    base_path : path to the base YAML config.
    env       : optional environment name (dev / staging / prod).
                When supplied, ``config/env/{env}.yaml`` is merged on top.
                Falls back to the ``PIPELINE_ENV`` env-var, then to the
                value inside the base config, and finally to ``"dev"``.
    """
    base_path = Path(base_path)
    with open(base_path) as fh:
        cfg = yaml.safe_load(fh)

    # Determine target environment
    env = env or os.environ.get("PIPELINE_ENV") or cfg.get("pipeline", {}).get("environment", "dev")

    env_path = base_path.parent / "env" / f"{env}.yaml"
    if env_path.exists():
        with open(env_path) as fh:
            env_cfg = yaml.safe_load(fh) or {}
        cfg = _deep_merge(cfg, env_cfg)

    # Interpolate env vars last so overrides take effect
    cfg = _interpolate(cfg)
    return cfg
