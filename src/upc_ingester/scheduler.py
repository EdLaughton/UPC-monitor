from __future__ import annotations

import asyncio
import logging
from datetime import datetime
from zoneinfo import ZoneInfo

from croniter import croniter

from .alerts import run_alerts
from .config import Settings
from .scraper import run_ingestion


logger = logging.getLogger(__name__)


class CronScheduler:
    def __init__(self, settings: Settings):
        self.settings = settings
        self._lock = asyncio.Lock()
        self._alerts_lock = asyncio.Lock()

    async def _run_once_locked(self) -> None:
        if self._lock.locked():
            logger.warning("previous ingestion run is still active; skipping scheduled run")
            return
        async with self._lock:
            try:
                await run_ingestion(self.settings, bootstrap=False)
            except Exception:
                logger.exception("scheduled ingestion run failed; service will keep running")
                return
            if self.settings.alerts_after_ingestion:
                logger.info("ingestion succeeded; running after-ingestion alerts")
                await self._run_alerts_once_locked()
            else:
                logger.info("ingestion succeeded; after-ingestion alerts disabled")

    async def _run_alerts_once_locked(self) -> None:
        if self._alerts_lock.locked():
            logger.warning("previous alerts run is still active; skipping after-ingestion alerts run")
            return
        async with self._alerts_lock:
            try:
                include_low_confidence = self.settings.alerts_include_low_confidence or self.settings.alerts_min_confidence == "Low"
                summary = await asyncio.to_thread(
                    run_alerts,
                    self.settings,
                    since_days=self.settings.alerts_since_days,
                    include_low_confidence=include_low_confidence,
                    write_json=True,
                    sync_airtable=self.settings.alerts_sync_airtable,
                    max_sync_records=self.settings.alerts_airtable_max_sync_records,
                    dry_run=False,
                    profile=self.settings.alerts_profile_filter,
                    min_confidence=self.settings.alerts_min_confidence,
                    sync_limit=self.settings.alerts_sync_limit,
                )
                sync = summary.get("airtable_sync", {})
                logger.info(
                    "after-ingestion alerts complete: decisions_scanned=%s matches_total=%s matches_syncable=%s created_items=%s updated_items=%s created_matches=%s updated_matches=%s",
                    summary.get("decisions_scanned"),
                    summary.get("matches_total"),
                    summary.get("matches_syncable"),
                    sync.get("created_items", 0),
                    sync.get("updated_items", 0),
                    sync.get("created_matches", 0),
                    sync.get("updated_matches", 0),
                )
            except RuntimeError as exc:
                logger.error("after-ingestion alerts refused or failed: %s; monitor service will keep running", exc, exc_info=True)
            except Exception:
                logger.exception("after-ingestion alerts run failed; monitor service will keep running")

    async def serve_forever(self) -> None:
        tz = ZoneInfo(self.settings.timezone)
        logger.info(
            "scheduler started with POLL_CRON=%r TIMEZONE=%s RUN_ON_START=%s",
            self.settings.poll_cron,
            self.settings.timezone,
            self.settings.run_on_start,
        )
        logger.info(
            "after-ingestion alerts %s; sync_airtable=%s since_days=%s min_confidence=%s profile_filter=%r sync_limit=%s",
            "enabled" if self.settings.alerts_after_ingestion else "disabled",
            self.settings.alerts_sync_airtable,
            self.settings.alerts_since_days,
            self.settings.alerts_min_confidence,
            self.settings.alerts_profile_filter,
            self.settings.alerts_sync_limit,
        )
        if self.settings.alerts_enabled:
            logger.warning("ALERTS_ENABLED separate scheduler is deprecated and ignored; use ALERTS_AFTER_INGESTION=true")

        if self.settings.run_on_start:
            await self._run_once_locked()

        while True:
            now = datetime.now(tz)
            next_at = croniter(self.settings.poll_cron, now).get_next(datetime)
            sleep_seconds = max(0.0, (next_at - now).total_seconds())
            logger.info("next ingestion run scheduled for %s", next_at.isoformat())
            await asyncio.sleep(sleep_seconds)
            await self._run_once_locked()
