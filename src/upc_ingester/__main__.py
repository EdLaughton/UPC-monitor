from __future__ import annotations

import argparse
import asyncio
import json
import logging

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
    subparsers.add_parser("run-once", help="run one ingestion pass")
    subparsers.add_parser("bootstrap", help="mark all currently visible UPC items as already seen")
    return parser


async def serve(settings: Settings) -> None:
    settings.ensure_dirs()
    db = Database(settings.db_path)
    db.init()
    render_outputs(db, settings.public_dir)
    server = start_static_server(settings.public_dir, settings.port)
    try:
        await CronScheduler(settings).serve_forever()
    finally:
        logger.info("shutting down static server")
        server.shutdown()


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
    raise ValueError(f"unknown command: {command}")


def main() -> int:
    try:
        return asyncio.run(main_async())
    except KeyboardInterrupt:
        logger.info("stopped")
        return 130


if __name__ == "__main__":
    raise SystemExit(main())
