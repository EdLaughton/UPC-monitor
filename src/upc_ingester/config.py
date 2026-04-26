from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path


DEFAULT_SOURCE_URL = (
    "https://www.unifiedpatentcourt.org/en/decisions-and-orders"
    "?case_number_search="
    "&registry_number="
    "&judgemet_reference="
    "&judgement_type=All"
    "&party_name="
    "&court_type=All"
    "&division_1=125"
    "&division_2=126"
    "&division_3=139"
    "&division_4=223"
    "&keywords="
    "&headnotes="
    "&proceedings_lang=All"
    "&judgement_date_from%5Bdate%5D="
    "&judgement_date_to%5Bdate%5D="
    "&location_id=All"
)

DEFAULT_FALLBACK_SOURCE_URL = (
    "https://www.unified-patent-court.org/en/decisions-and-orders"
    "?case_number_search="
    "&registry_number="
    "&judgemet_reference="
    "&judgement_type=All"
    "&party_name="
    "&court_type=All"
    "&division_1=125"
    "&division_2=126"
    "&division_3=139"
    "&division_4=223"
    "&keywords="
    "&headnotes="
    "&proceedings_lang=All"
    "&judgement_date_from%5Bdate%5D="
    "&judgement_date_to%5Bdate%5D="
    "&location_id=All"
)

DEFAULT_PUBLIC_BASE_URL = "https://upc.edlaughton.uk"


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
    max_items: int
    date_from: str
    date_to: str
    date_window_days: int
    index_page_retry_delay_seconds: int
    index_page_max_retries: int
    latest_export_limit: int
    write_all_json: bool
    alerts_enabled: bool = False
    alerts_after_ingestion: bool = False
    alerts_sync_airtable: bool = False
    alerts_schedule_hour: int = 10
    alerts_schedule_minute: int = 5
    alerts_since_days: int = 2
    alerts_include_low_confidence: bool = False
    alerts_min_confidence: str = "High"
    alerts_profile_filter: str = ""
    alerts_sync_limit: int = 0
    alerts_airtable_max_sync_records: int = 100
    airtable_base_id: str = "appzaT3sgr7AfBKkn"
    public_base_url: str = DEFAULT_PUBLIC_BASE_URL

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
            source_url=os.getenv("SOURCE_URL", DEFAULT_SOURCE_URL),
            fallback_source_url=os.getenv("FALLBACK_SOURCE_URL", DEFAULT_FALLBACK_SOURCE_URL),
            poll_cron=os.getenv("POLL_CRON", "0 * * * *"),
            timezone=os.getenv("TIMEZONE", "Europe/London"),
            run_on_start=env_bool("RUN_ON_START", True),
            port=env_int("PORT", 8000),
            log_level=os.getenv("LOG_LEVEL", "INFO"),
            navigation_timeout_ms=env_int("NAVIGATION_TIMEOUT_MS", 60000),
            page_wait_timeout_ms=env_int("PAGE_WAIT_TIMEOUT_MS", 20000),
            max_pages=env_int("MAX_PAGES", 1),
            max_items=env_int("MAX_ITEMS", 10),
            date_from=os.getenv("DATE_FROM", ""),
            date_to=os.getenv("DATE_TO", ""),
            date_window_days=env_int("DATE_WINDOW_DAYS", 0),
            index_page_retry_delay_seconds=env_int("INDEX_PAGE_RETRY_DELAY_SECONDS", 30),
            index_page_max_retries=env_int("INDEX_PAGE_MAX_RETRIES", 3),
            latest_export_limit=env_int("LATEST_EXPORT_LIMIT", 50),
            write_all_json=env_bool("WRITE_ALL_JSON", False),
            alerts_enabled=env_bool("ALERTS_ENABLED", False),
            alerts_after_ingestion=env_bool("ALERTS_AFTER_INGESTION", False),
            alerts_sync_airtable=env_bool("ALERTS_SYNC_AIRTABLE", False),
            alerts_schedule_hour=env_int("ALERTS_SCHEDULE_HOUR", 10),
            alerts_schedule_minute=env_int("ALERTS_SCHEDULE_MINUTE", 5),
            alerts_since_days=env_int("ALERTS_SINCE_DAYS", 2),
            alerts_include_low_confidence=env_bool("ALERTS_INCLUDE_LOW_CONFIDENCE", False),
            alerts_min_confidence=os.getenv("ALERTS_MIN_CONFIDENCE", "High"),
            alerts_profile_filter=os.getenv("ALERTS_PROFILE_FILTER", ""),
            alerts_sync_limit=env_int("ALERTS_SYNC_LIMIT", 0),
            alerts_airtable_max_sync_records=env_int("ALERTS_AIRTABLE_MAX_SYNC_RECORDS", 100),
            airtable_base_id=os.getenv("AIRTABLE_BASE_ID", "appzaT3sgr7AfBKkn"),
            public_base_url=os.getenv("PUBLIC_BASE_URL", DEFAULT_PUBLIC_BASE_URL),
        )

    def ensure_dirs(self) -> None:
        self.data_dir.mkdir(parents=True, exist_ok=True)
        self.public_dir.mkdir(parents=True, exist_ok=True)
        self.pdfs_dir.mkdir(parents=True, exist_ok=True)
        self.debug_dir.mkdir(parents=True, exist_ok=True)
