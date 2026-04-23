from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path


def env_bool(name: str, default: bool) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "y", "on"}


def env_int(name: str, default: int) -> int:
    value = os.getenv(name)
    if value is None or value.strip() == "":
        return default
    return int(value)


@dataclass(frozen=True)
class Settings:
    data_dir: Path
    public_dir: Path
    pdfs_dir: Path
    debug_dir: Path
    db_path: Path
    source_url: str
    fallback_source_url: str
    poll_cron: str
    timezone: str
    run_on_start: bool
    port: int
    log_level: str
    navigation_timeout_ms: int
    page_wait_timeout_ms: int
    max_pages: int

    @classmethod
    def from_env(cls) -> "Settings":
        data_dir = Path(os.getenv("DATA_DIR", "/data")).resolve()
        public_dir = data_dir / "public"
        return cls(
            data_dir=data_dir,
            public_dir=public_dir,
            pdfs_dir=public_dir / "pdfs",
            debug_dir=data_dir / "debug",
            db_path=data_dir / "upc.sqlite3",
            source_url=os.getenv(
                "SOURCE_URL",
                "https://www.unifiedpatentcourt.org/en/decisions-and-orders",
            ),
            fallback_source_url=os.getenv(
                "FALLBACK_SOURCE_URL",
                "https://www.unified-patent-court.org/en/decisions-and-orders",
            ),
            poll_cron=os.getenv("POLL_CRON", "0 * * * *"),
            timezone=os.getenv("TIMEZONE", "Europe/London"),
            run_on_start=env_bool("RUN_ON_START", True),
            port=env_int("PORT", 8000),
            log_level=os.getenv("LOG_LEVEL", "INFO"),
            navigation_timeout_ms=env_int("NAVIGATION_TIMEOUT_MS", 60000),
            page_wait_timeout_ms=env_int("PAGE_WAIT_TIMEOUT_MS", 20000),
            max_pages=env_int("MAX_PAGES", 0),
        )

    def ensure_dirs(self) -> None:
        self.data_dir.mkdir(parents=True, exist_ok=True)
        self.public_dir.mkdir(parents=True, exist_ok=True)
        self.pdfs_dir.mkdir(parents=True, exist_ok=True)
        self.debug_dir.mkdir(parents=True, exist_ok=True)
