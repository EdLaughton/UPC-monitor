import asyncio
from pathlib import Path

from upc_ingester.config import Settings
from upc_ingester.db import Database
from upc_ingester.parser import IndexItem
from upc_ingester.scraper import (
    ingest_discovered_items,
    needs_enrichment,
    upsert_index_only_item,
)


def make_index_item() -> IndexItem:
    return IndexItem(
        item_key="node-123456",
        node_url="https://www.unifiedpatentcourt.org/en/node/123456",
        decision_date="2026-04-15",
        registry_number="ACT_10138/2026",
        order_or_decision_number="ORD_10339/2026",
        case_number="UPC_CFI_1/2026",
        division="Local Division Paris",
        type_of_action="Infringement action",
        parties_raw="Alexion Pharmaceuticals, Inc.\nv.\nSamsung Bioepis Co., Ltd.",
        title_raw="Order",
        source_index_snapshot={"Date": "15 April, 2026", "UPC Document": "Order"},
    )


def make_settings(tmp_path: Path) -> Settings:
    return Settings(
        data_dir=tmp_path,
        public_dir=tmp_path / "public",
        pdfs_dir=tmp_path / "public" / "pdfs",
        debug_dir=tmp_path / "debug",
        db_path=tmp_path / "upc.sqlite3",
        source_url="https://example.test/index",
        fallback_source_url="",
        poll_cron="0 * * * *",
        timezone="Europe/London",
        run_on_start=False,
        port=8000,
        log_level="INFO",
        navigation_timeout_ms=1000,
        page_wait_timeout_ms=1000,
        max_pages=1,
        max_items=10,
        latest_export_limit=50,
        write_all_json=False,
    )


def test_index_only_storage_creates_decision_from_index_item(tmp_path: Path) -> None:
    db = Database(tmp_path / "upc.sqlite3")
    db.init()

    upsert_index_only_item(db, make_index_item(), "2026-04-23T12:00:00+00:00")

    decision = db.get_decision("node-123456")
    assert decision is not None
    assert decision["node_url"] == "https://www.unifiedpatentcourt.org/en/node/123456"
    assert decision["decision_date"] == "2026-04-15"
    assert decision["registry_number"] == "ACT_10138/2026"
    assert decision["order_or_decision_number"] == "ORD_10339/2026"
    assert decision["division"] == "Local Division Paris"
    assert decision["type_of_action"] == "Infringement action"
    assert decision["party_names_normalised"] == ["alexion pharmaceuticals", "samsung bioepis co"]
    assert decision["pdf_url_official"] == ""
    assert decision["pdf_url_mirror"] == ""
    assert decision["pdf_sha256"] == ""
    assert decision["headnote_text"] == ""
    assert decision["keywords_raw"] == ""
    assert decision["last_error"] == "index-only backfill; detail not yet fetched"


def test_needs_enrichment_remains_true_for_index_only_rows(tmp_path: Path) -> None:
    db = Database(tmp_path / "upc.sqlite3")
    db.init()
    upsert_index_only_item(db, make_index_item(), "2026-04-23T12:00:00+00:00")

    assert needs_enrichment(db.get_decision("node-123456")) is True


def test_index_only_mode_does_not_call_detail_ingest(tmp_path: Path, monkeypatch) -> None:
    db = Database(tmp_path / "upc.sqlite3")
    db.init()
    settings = make_settings(tmp_path)

    async def fail_if_called(*args, **kwargs) -> None:
        raise AssertionError("detail ingest should not be called in index-only mode")

    monkeypatch.setattr("upc_ingester.scraper.fetch_detail", fail_if_called)
    monkeypatch.setattr("upc_ingester.scraper.download_pdf", fail_if_called)
    monkeypatch.setattr("upc_ingester.scraper.ingest_item", fail_if_called)

    new_count, skipped_count, errors = asyncio.run(
        ingest_discovered_items(
            context=object(),
            db=db,
            settings=settings,
            items=[make_index_item()],
            debug_run_dir=tmp_path / "debug",
            index_only=True,
        )
    )

    assert new_count == 1
    assert skipped_count == 0
    assert errors == []
    assert db.get_decision("node-123456") is not None
