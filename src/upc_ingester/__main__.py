from __future__ import annotations

import argparse
import asyncio
import json
import logging
import os

from .config import Settings
from .db import Database
from .logging_config import configure_logging
from .render import render_outputs
from .scheduler import CronScheduler
from .scraper import run_ingestion
from .server import start_static_server


logger = logging.getLogger(__name__)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="python -m upc_ingester")
    subparsers = parser.add_subparsers(dest="command")
    subparsers.add_parser("serve", help="start the scheduler and static mirror server")
    subparsers.add_parser("run-once", help="run one normal ingestion pass")
    subparsers.add_parser("bootstrap", help="mark all currently visible UPC items as already seen")
    backfill = subparsers.add_parser("backfill", help="run a deliberate broader one-off crawl for statistics/backfill")
    backfill.add_argument("--max-pages", type=int, default=0, help="maximum UPC index pages to inspect; 0 means unlimited")
    backfill.add_argument("--max-items", type=int, default=0, help="maximum UPC items to ingest; 0 means unlimited")
    backfill.add_argument("--start-page", type=int, default=0, help="UPC index page number to start from; default 0")
    backfill.add_argument("--date-from", default="", help="oldest decision date to include, YYYY-MM-DD")
    backfill.add_argument("--date-to", default="", help="newest decision date to include, YYYY-MM-DD; default today in UTC")
    backfill.add_argument(
        "--date-window-days",
        type=int,
        default=0,
        help="split index discovery into shallow date windows; 0 disables date-window mode",
    )
    backfill.add_argument(
        "--write-all-json",
        action="store_true",
        help="also write /all.json; /all.ndjson is always written",
    )
    backfill.add_argument(
        "--index-only",
        action="store_true",
        help="store rows from index metadata only; do not fetch detail pages or PDFs",
    )
    return parser


async def serve(settings: Settings) -> None:
    settings.ensure_dirs()
    db = Database(settings.db_path)
    db.init()
    render_outputs(db, settings)
    server = start_static_server(settings.public_dir, settings.port)
    try:
        await CronScheduler(settings).serve_forever()
    finally:
        logger.info("shutting down static server")
        server.shutdown()


def settings_for_backfill(
    base: Settings,
    max_pages: int,
    max_items: int,
    start_page: int,
    date_from: str,
    date_to: str,
    date_window_days: int,
    write_all_json: bool,
) -> Settings:
    return Settings(
        data_dir=base.data_dir,
        public_dir=base.public_dir,
        pdfs_dir=base.pdfs_dir,
        debug_dir=base.debug_dir,
        db_path=base.db_path,
        source_url=base.source_url,
        fallback_source_url=base.fallback_source_url,
        poll_cron=base.poll_cron,
        timezone=base.timezone,
        run_on_start=base.run_on_start,
        port=base.port,
        log_level=base.log_level,
        navigation_timeout_ms=base.navigation_timeout_ms,
        page_wait_timeout_ms=base.page_wait_timeout_ms,
        max_pages=max_pages,
        max_items=max_items,
        start_page=start_page,
        date_from=date_from,
        date_to=date_to,
        date_window_days=date_window_days,
        latest_export_limit=max(base.latest_export_limit, min(max_items, 200)),
        write_all_json=write_all_json or base.write_all_json,
    )


async def main_async() -> int:
    args = build_parser().parse_args()
    settings = Settings.from_env()
    configure_logging(settings.log_level)

    command = args.command or "serve"
    if command == "serve":
        await serve(settings)
        return 0
    if command == "run-once":
        result = await run_ingestion(settings, bootstrap=False)
        print(json.dumps(result, indent=2, sort_keys=True))
        return 0
    if command == "bootstrap":
        result = await run_ingestion(settings, bootstrap=True)
        print(json.dumps(result, indent=2, sort_keys=True))
        return 0
    if command == "backfill":
        # Backfill is intentionally opt-in and one-shot so the always-on service
        # can remain gentle. The env vars are also set for any code paths that
        # still read Settings.from_env indirectly.
        os.environ["MAX_PAGES"] = str(args.max_pages)
        os.environ["MAX_ITEMS"] = str(args.max_items)
        os.environ["START_PAGE"] = str(args.start_page)
        os.environ["DATE_FROM"] = args.date_from
        os.environ["DATE_TO"] = args.date_to
        os.environ["DATE_WINDOW_DAYS"] = str(args.date_window_days)
        if args.write_all_json:
            os.environ["WRITE_ALL_JSON"] = "true"
        backfill_settings = settings_for_backfill(
            settings,
            max_pages=args.max_pages,
            max_items=args.max_items,
            start_page=args.start_page,
            date_from=args.date_from,
            date_to=args.date_to,
            date_window_days=args.date_window_days,
            write_all_json=args.write_all_json,
        )
        result = await run_ingestion(backfill_settings, bootstrap=False, index_only=args.index_only)
        print(json.dumps(result, indent=2, sort_keys=True))
        return 0
    raise ValueError(f"unknown command: {command}")


def main() -> int:
    try:
        return asyncio.run(main_async())
    except KeyboardInterrupt:
        logger.info("stopped")
        return 130


if __name__ == "__main__":
    raise SystemExit(main())
