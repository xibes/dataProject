"""Project configuration, read from environment variables.

Every setting has a default, so nothing needs to be set to run locally.

  DATA_DIR            root data directory               (default: data)
  RAW_DIR             raw-layer output directory        (default: $DATA_DIR/raw)
  NBP_RATES_FILE      NBP rates Parquet output          (default: $RAW_DIR/nbp_rates_a.parquet)
  GL_JOURNALS_FILE    synthetic GL lines Parquet output (default: $RAW_DIR/journal_lines.parquet)
  NBP_BASE_URL        NBP table A API endpoint          (default: https://api.nbp.pl/api/exchangerates/tables/a)

To load them from a file:  uv run --env-file .env python -m src.ingest ...
"""

import os
from dataclasses import dataclass
from pathlib import Path


def _path(name: str, default: Path) -> Path:
    value = os.environ.get(name, "").strip()
    return Path(value).expanduser() if value else default


@dataclass(frozen=True)
class Settings:
    data_dir: Path
    raw_dir: Path
    nbp_rates_file: Path
    gl_journals_file: Path
    nbp_base_url: str

    @classmethod
    def from_env(cls) -> "Settings":
        data_dir = _path("DATA_DIR", Path("data"))
        raw_dir = _path("RAW_DIR", data_dir / "raw")
        return cls(
            data_dir=data_dir,
            raw_dir=raw_dir,
            nbp_rates_file=_path("NBP_RATES_FILE", raw_dir / "nbp_rates_a.parquet"),
            gl_journals_file=_path("GL_JOURNALS_FILE", raw_dir / "journal_lines.parquet"),
            nbp_base_url=os.environ.get("NBP_BASE_URL", "").strip().rstrip("/")
            or "https://api.nbp.pl/api/exchangerates/tables/a",
        )
