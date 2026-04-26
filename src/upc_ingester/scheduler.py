from __future__ import annotations

import asyncio
import contextlib
import logging
from datetime import datetime, timedelta
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

    async def _run_alerts_once_locked(self) -> None:
        if self._alerts_lock.locked():
            logger.warning("previous alerts run is still active; skipping scheduled alerts run")
            return
        async with self._alerts_lock:
            try:
                await asyncio.to_thread(
                    run_alerts,
                    self.settings,
                    since_days=self.settings.alerts_since_days,
                    include_low_confidence=self.settings.alerts_include_low_confidence,
                    write_json=True,
                    sync_airtable=self.settings.alerts_sync_airtable,
                    max_sync_records=self.settings.alerts_airtable_max_sync_records,
                    dry_run=False,
                )
            except Exception:
                logger.exception("scheduled alerts run failed; monitor service will keep running")

    async def _alerts_loop(self, tz: ZoneInfo) -> None:
        while True:
            now = datetime.now(tz)
            next_at = now.replace(
                hour=max(0, min(23, int(self.settings.alerts_schedule_hour))),
                minute=max(0, min(59, int(self.settings.alerts_schedule_minute))),
                second=0,
                microsecond=0,
            )
            if next_at <= now:
                next_at = next_at + timedelta(days=1)
            sleep_seconds = max(0.0, (next_at - now).total_seconds())
            logger.info("next alerts run scheduled for %s", next_at.isoformat())
            await asyncio.sleep(sleep_seconds)
            await self._run_alerts_once_locked()

    async def serve_forever(self) -> None:
        tz = ZoneInfo(self.settings.timezone)
        logger.info(
            "scheduler started with POLL_CRON=%r TIMEZONE=%s RUN_ON_START=%s",
            self.settings.poll_cron,
            self.settings.timezone,
            self.settings.run_on_start,
        )
        alert_task: asyncio.Task | None = None
        if self.settings.alerts_enabled:
            logger.info(
                "alerts scheduler enabled at %02d:%02d; sync_airtable=%s since_days=%s",
                self.settings.alerts_schedule_hour,
                self.settings.alerts_schedule_minute,
                self.settings.alerts_sync_airtable,
                self.settings.alerts_since_days,
            )
            alert_task = asyncio.create_task(self._alerts_loop(tz))
        else:
            logger.info("alerts scheduler disabled")

        if self.settings.run_on_start:
            await self._run_once_locked()

        try:
            while True:
                now = datetime.now(tz)
                next_at = croniter(self.settings.poll_cron, now).get_next(datetime)
                sleep_seconds = max(0.0, (next_at - now).total_seconds())
                logger.info("next ingestion run scheduled for %s", next_at.isoformat())
                await asyncio.sleep(sleep_seconds)
                await self._run_once_locked()
        finally:
            if alert_task:
                alert_task.cancel()
                with contextlib.suppress(Exception):
                    await alert_task
