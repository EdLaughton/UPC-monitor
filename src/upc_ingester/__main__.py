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
    backfill.add_argument("--max-pages", type=int, default=5, help="maximum UPC index pages to inspect")
    backfill.add_argument("--max-items", type=int, default=100, help="maximum UPC items to ingest")
    backfill.add_argument(
        "--write-all-json",
        action="store_true",
        help="also write /all.json; /all.ndjson is always written",
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


def settings_for_backfill(base: Settings, max_pages: int, max_items: int, write_all_json: bool) -> Settings:
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
        if args.write_all_json:
            os.environ["WRITE_ALL_JSON"] = "true"
        backfill_settings = settings_for_backfill(
            settings,
            max_pages=args.max_pages,
            max_items=args.max_items,
            write_all_json=args.write_all_json,
        )
        result = await run_ingestion(backfill_settings, bootstrap=False)
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
