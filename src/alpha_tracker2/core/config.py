from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml


@dataclass(frozen=True)
class Settings:
    project_name: str
    lake_dir: Path
    store_db: Path
    runs_dir: Path
    out_dir: Path
    log_level: str
    timezone: str


def _ensure_required_fields(config: dict[str, Any]) -> None:
    project = config.get("project") or {}
    name = project.get("name")
    if not isinstance(name, str) or not name.strip():
        raise ValueError("configs/default.yaml must define 'project.name' as a non-empty string.")


def load_settings(project_root: Path) -> Settings:
    """
    Load settings from configs/default.yaml located under project_root.

    Relative paths in the config are resolved against project_root.
    """
    config_path = project_root / "configs" / "default.yaml"
    if not config_path.is_file():
        raise FileNotFoundError(f"Config file not found: {config_path}")

    with config_path.open("r", encoding="utf-8") as f:
        config: dict[str, Any] = yaml.safe_load(f) or {}

    _ensure_required_fields(config)

    project_cfg = config.get("project", {})
    paths_cfg = config.get("paths", {})
    logging_cfg = config.get("logging", {})

    def _resolve_path(path_value: str | Path | None) -> Path:
        if path_value is None:
            raise ValueError("Path value in configs/default.yaml cannot be null.")
        p = Path(path_value)
        if not p.is_absolute():
            p = project_root / p
        return p

    project_name = str(project_cfg["name"])
    timezone = str(project_cfg.get("timezone", "UTC"))

    lake_dir = _resolve_path(paths_cfg.get("lake_dir", "data/lake"))
    store_db = _resolve_path(paths_cfg.get("store_db", "data/store/alpha_tracker.duckdb"))
    runs_dir = _resolve_path(paths_cfg.get("runs_dir", "data/runs"))
    out_dir = _resolve_path(paths_cfg.get("out_dir", "data/out"))

    log_level_raw = logging_cfg.get("level", "INFO")
    log_level = str(log_level_raw).upper()

    return Settings(
        project_name=project_name,
        lake_dir=lake_dir,
        store_db=store_db,
        runs_dir=runs_dir,
        out_dir=out_dir,
        log_level=log_level,
        timezone=timezone,
    )


def get_raw_config(project_root: Path) -> dict[str, Any]:
    """
    Load the full config dict from configs/default.yaml.
    Use for reading scoring.v1.weights, scoring.v2_v3_v4.bt_column_weights, etc.
    """
    config_path = project_root / "configs" / "default.yaml"
    if not config_path.is_file():
        raise FileNotFoundError(f"Config file not found: {config_path}")
    with config_path.open("r", encoding="utf-8") as f:
        return yaml.safe_load(f) or {}

