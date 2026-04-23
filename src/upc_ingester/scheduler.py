from __future__ import annotations

import asyncio
import logging
from datetime import datetime
from zoneinfo import ZoneInfo

from croniter import croniter

from .config import Settings
from .scraper import run_ingestion


logger = logging.getLogger(__name__)


class CronScheduler:
    def __init__(self, settings: Settings):
        self.settings = settings
        self._lock = asyncio.Lock()

    async def _run_once_locked(self) -> None:
        if self._lock.locked():
            logger.warning("previous ingestion run is still active; skipping scheduled run")
            return
        async with self._lock:
            try:
                await run_ingestion(self.settings, bootstrap=False)
            except Exception:
                logger.exception("scheduled ingestion run failed; service will keep running")

    async def serve_forever(self) -> None:
        tz = ZoneInfo(self.settings.timezone)
        logger.info(
            "scheduler started with POLL_CRON=%r TIMEZONE=%s RUN_ON_START=%s",
            self.settings.poll_cron,
            self.settings.timezone,
            self.settings.run_on_start,
        )
        if self.settings.run_on_start:
            await self._run_once_locked()

        while True:
            now = datetime.now(tz)
            next_at = croniter(self.settings.poll_cron, now).get_next(datetime)
            sleep_seconds = max(0.0, (next_at - now).total_seconds())
            logger.info("next ingestion run scheduled for %s", next_at.isoformat())
            await asyncio.sleep(sleep_seconds)
            await self._run_once_locked()
