from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import yaml


@dataclass(frozen=True)
class Settings:
    project_name: str
    lake_dir: Path
    store_db: Path
    runs_dir: Path
    log_level: str


def load_settings(project_root: Path) -> Settings:
    cfg_path = project_root / "configs" / "default.yaml"
    if not cfg_path.exists():
        raise FileNotFoundError(f"Missing config: {cfg_path}")

    cfg = yaml.safe_load(cfg_path.read_text(encoding="utf-8"))

    # --- minimal validation (只校验本步用到的字段) ---
    project = cfg.get("project", {})
    paths = cfg.get("paths", {})
    logging = cfg.get("logging", {})

    project_name = project.get("name", "").strip()
    if not project_name:
        raise ValueError("configs/default.yaml: project.name is required")

    lake_dir = project_root / paths.get("lake_dir", "data/lake")
    store_db = project_root / paths.get("store_db", "data/store/alpha_tracker.duckdb")
    runs_dir = project_root / paths.get("runs_dir", "data/runs")
    log_level = str(logging.get("level", "INFO")).upper()

    return Settings(
        project_name=project_name,
        lake_dir=lake_dir,
        store_db=store_db,
        runs_dir=runs_dir,
        log_level=log_level,
    )
