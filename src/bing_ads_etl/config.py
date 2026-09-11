from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path


def _project_root() -> Path:
    env_root = os.getenv("PROJECT_ROOT")
    if env_root:
        return Path(env_root)
    return Path(__file__).resolve().parents[2]


@dataclass(frozen=True)
class Settings:
    project_root: Path
    source_csv: Path
    staging_dir: Path
    schema_sql: Path
    mysql_host: str
    mysql_port: int
    mysql_user: str
    mysql_password: str
    mysql_database: str

    @property
    def extracted_path(self) -> Path:
        return self.staging_dir / "extracted.pkl"

    @property
    def validated_path(self) -> Path:
        return self.staging_dir / "validated.pkl"

    @property
    def transformed_dir(self) -> Path:
        return self.staging_dir / "transformed"


def get_settings() -> Settings:
    root = _project_root()
    default_csv = root / "data" / "raw" / "BING_MultiDays.csv"
    return Settings(
        project_root=root,
        source_csv=Path(os.getenv("SOURCE_CSV", default_csv)),
        staging_dir=Path(os.getenv("STAGING_DIR", root / "data" / "staging")),
        schema_sql=Path(os.getenv("SCHEMA_SQL", root / "sql" / "schema.sql")),
        mysql_host=os.getenv("MYSQL_HOST", "127.0.0.1"),
        mysql_port=int(os.getenv("MYSQL_PORT", "3307")),
        mysql_user=os.getenv("MYSQL_USER", "ads"),
        mysql_password=os.getenv("MYSQL_PASSWORD", "ads"),
        mysql_database=os.getenv("MYSQL_DATABASE", "bing_ads_dw"),
    )
