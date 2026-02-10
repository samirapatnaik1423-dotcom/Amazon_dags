# ═══════════════════════════════════════════════════════════════════════
# TEAM 1 - SPRINT 1 & 2: Configuration Loader
# Tasks: T0006, T0012
# ═══════════════════════════════════════════════════════════════════════

"""
config_loader.py - YAML/JSON Configuration Loader

TASKS IMPLEMENTED:
- T0006: Build starter config loader (YAML/JSON)
- T0012: Build config-driven cleaning rules (loads cleaning configs)

Methods:
- load_yaml(): Load YAML configuration files
- load_json(): Load JSON configuration files

Usage:
    config = ConfigLoader.load_yaml('config/customers_config.yaml')
"""

from __future__ import annotations

from pathlib import Path
from typing import Any, Union
import json

try:
    import yaml  # type: ignore
except Exception:  # pragma: no cover
    yaml = None  # PyYAML may not be available in some environments


PathLike = Union[str, Path]


class ConfigLoader:
    """Lightweight configuration loader for YAML/JSON files.

    Responsibilities:
    - Read YAML/JSON files from disk and return dicts
    - Raise FileNotFoundError for missing paths (per tests)
    - Keep behavior minimal and dependency-light for reuse in Airflow tasks
    """

    @staticmethod
    def _ensure_exists(path: PathLike) -> Path:
        p = Path(path)
        if not p.exists():
            raise FileNotFoundError(f"Config file not found: {p}")
        if not p.is_file():
            raise FileNotFoundError(f"Config path is not a file: {p}")
        return p

    # ========================================
    # Team 1 - T0012: Config-driven cleaning rules (YAML loader)
    # ========================================
    @staticmethod
    def load_yaml(path: PathLike) -> dict[str, Any]:
        """Load a YAML file and return a dictionary.

        Mirrors behavior expected by tests in tests/test_etl_pipeline.py.
        """
        p = ConfigLoader._ensure_exists(path)
        if yaml is None:
            raise ImportError(
                "PyYAML is required to load YAML files. Please install PyYAML."
            )
        with p.open("r", encoding="utf-8") as f:
            data = yaml.safe_load(f)  # type: ignore[no-untyped-call]
        return data or {}

    @staticmethod
    def load_json(path: PathLike) -> dict[str, Any]:
        """Load a JSON file and return a dictionary."""
        p = ConfigLoader._ensure_exists(path)
        with p.open("r", encoding="utf-8") as f:
            data = json.load(f)
        if not isinstance(data, dict):
            # Keep simple; tests expect dict semantics
            return {"_": data}
        return data
