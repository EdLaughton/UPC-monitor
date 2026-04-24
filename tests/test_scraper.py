import asyncio
from dataclasses import replace
from pathlib import Path

from upc_ingester.__main__ import settings_for_backfill
from upc_ingester.config import Settings
from upc_ingester.db import Database, INDEX_ONLY_LAST_ERROR
from upc_ingester.parser import IndexItem
from upc_ingester.scraper import (
    build_index_url,
    cap_items,
    ingest_discovered_items,
    needs_enrichment,
    parse_page_number,
    select_next_index_url,
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


def make_updated_index_item() -> IndexItem:
    return IndexItem(
        item_key="node-123456",
        node_url="https://www.unifiedpatentcourt.org/en/node/123456?updated=1",
        decision_date="2026-04-16",
        registry_number="ACT_99999/2026",
        order_or_decision_number="ORD_99999/2026",
        case_number="UPC_CFI_999/2026",
        division="Local Division Munich",
        type_of_action="Revocation action",
        parties_raw="Index Claimant\nv.\nIndex Defendant",
        title_raw="Index Order",
        source_index_snapshot={"Date": "16 April, 2026", "UPC Document": "Index Order"},
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
        start_page=0,
        latest_export_limit=50,
        write_all_json=False,
    )


def insert_decision(
    db: Database,
    *,
    last_error: str = "",
    pdf_url_official: str = "https://example.test/official.pdf",
    pdf_url_mirror: str = "/pdfs/2026/node-123456/official.pdf",
    pdf_sha256: str = "abc",
    headnote_text: str = "Existing headnote",
    keywords_raw: str = "Existing; Keywords",
    keywords_list: list[str] | None = None,
    language: str = "English",
    panel: str = "Existing panel",
) -> int:
    return db.upsert_decision(
        {
            "item_key": "node-123456",
            "title_raw": "Detail title",
            "case_name_raw": "Detail Claimant v. Detail Defendant",
            "parties_raw": "Detail Claimant\nv.\nDetail Defendant",
            "parties_json": [{"role": "claimant", "name": "Detail Claimant"}],
            "party_names_all": ["Detail Claimant", "Detail Defendant"],
            "party_names_normalised": ["detail claimant", "detail defendant"],
            "primary_adverse_caption": "Detail Claimant v. Detail Defendant",
            "adverse_pair_key": "detail claimant :: detail defendant",
            "division": "Local Division Paris",
            "panel": panel,
            "case_number": "UPC_CFI_1/2026",
            "registry_number": "ACT_10138/2026",
            "order_or_decision_number": "ORD_10339/2026",
            "decision_date": "2026-04-15",
            "document_type": "Detail document",
            "type_of_action": "Infringement action",
            "language": language,
            "headnote_raw": headnote_text,
            "headnote_text": headnote_text,
            "keywords_raw": keywords_raw,
            "keywords_list": keywords_list if keywords_list is not None else ["Existing", "Keywords"],
            "pdf_url_official": pdf_url_official,
            "pdf_url_mirror": pdf_url_mirror,
            "node_url": "https://www.unifiedpatentcourt.org/en/node/123456",
            "pdf_sha256": pdf_sha256,
            "first_seen_at": "2026-04-22T12:00:00+00:00",
            "last_seen_at": "2026-04-22T12:00:00+00:00",
            "ingested_at": "2026-04-22T12:00:00+00:00",
            "alerted_at": "2026-04-22T12:00:00+00:00",
            "last_error": last_error,
            "source_index_snapshot": "{}",
        }
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
    assert decision["last_error"] == INDEX_ONLY_LAST_ERROR


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


def test_index_only_does_not_overwrite_complete_row(tmp_path: Path) -> None:
    db = Database(tmp_path / "upc.sqlite3")
    db.init()
    decision_id = insert_decision(db)
    db.replace_documents(
        decision_id,
        [
            {
                "language": "EN",
                "pdf_url_official": "https://example.test/official.pdf",
                "pdf_url_mirror": "/pdfs/2026/node-123456/official.pdf",
                "pdf_sha256": "abc",
                "file_path": "/data/public/pdfs/2026/node-123456/official.pdf",
                "is_primary": True,
                "downloaded_at": "2026-04-22T12:00:00+00:00",
            }
        ],
    )

    new_count, skipped_count, errors = asyncio.run(
        ingest_discovered_items(
            context=object(),
            db=db,
            settings=make_settings(tmp_path),
            items=[make_updated_index_item()],
            debug_run_dir=tmp_path / "debug",
            index_only=True,
        )
    )

    decision = db.get_decision("node-123456")
    assert new_count == 0
    assert skipped_count == 1
    assert errors == []
    assert decision is not None
    assert decision["pdf_url_official"] == "https://example.test/official.pdf"
    assert decision["pdf_url_mirror"] == "/pdfs/2026/node-123456/official.pdf"
    assert decision["pdf_sha256"] == "abc"
    assert decision["headnote_text"] == "Existing headnote"
    assert decision["keywords_raw"] == "Existing; Keywords"
    assert decision["language"] == "English"
    assert decision["panel"] == "Existing panel"
    assert decision["documents"][0]["pdf_url_mirror"] == "/pdfs/2026/node-123456/official.pdf"
    assert needs_enrichment(decision) is False


def test_index_only_refreshes_incomplete_without_wiping_enriched_fields(tmp_path: Path) -> None:
    db = Database(tmp_path / "upc.sqlite3")
    db.init()
    insert_decision(
        db,
        last_error="PDF download failed for https://example.test/official.pdf: timeout",
        pdf_url_official="https://example.test/official.pdf",
        pdf_url_mirror="",
        pdf_sha256="",
        headnote_text="Partial headnote",
        keywords_raw="Partial; Keywords",
        keywords_list=["Partial", "Keywords"],
    )

    upsert_index_only_item(db, make_updated_index_item(), "2026-04-23T12:00:00+00:00")

    decision = db.get_decision("node-123456")
    assert decision is not None
    assert decision["node_url"] == "https://www.unifiedpatentcourt.org/en/node/123456?updated=1"
    assert decision["decision_date"] == "2026-04-16"
    assert decision["registry_number"] == "ACT_99999/2026"
    assert decision["case_number"] == "UPC_CFI_999/2026"
    assert decision["order_or_decision_number"] == "ORD_99999/2026"
    assert decision["division"] == "Local Division Munich"
    assert decision["type_of_action"] == "Revocation action"
    assert decision["title_raw"] == "Detail title"
    assert decision["parties_raw"] == "Detail Claimant\nv.\nDetail Defendant"
    assert decision["pdf_url_official"] == "https://example.test/official.pdf"
    assert decision["headnote_text"] == "Partial headnote"
    assert decision["keywords_raw"] == "Partial; Keywords"
    assert decision["keywords_list"] == ["Partial", "Keywords"]
    assert decision["language"] == "English"
    assert decision["panel"] == "Existing panel"
    assert decision["last_error"] == "PDF download failed for https://example.test/official.pdf: timeout"
    assert needs_enrichment(decision) is True


def test_discovery_max_items_cap_keeps_latest_items(tmp_path: Path) -> None:
    settings = replace(make_settings(tmp_path), max_items=1)
    items = [make_index_item(), make_updated_index_item()]

    assert cap_items(items, settings) == items[:1]


def test_build_index_url_preserves_filter_query_and_sets_page() -> None:
    url = (
        "https://www.unifiedpatentcourt.org/en/decisions-and-orders"
        "?case_number_search=&registry_number=&judgement_type=All&division_1=125&page=3"
    )

    built = build_index_url(url, 22)

    assert built == (
        "https://www.unifiedpatentcourt.org/en/decisions-and-orders"
        "?case_number_search=&registry_number=&judgement_type=All&division_1=125&page=22"
    )
    assert parse_page_number(built) == 22


def test_later_unsubmitted_form_reset_uses_direct_next_page_url() -> None:
    source_url = (
        "https://www.unifiedpatentcourt.org/en/decisions-and-orders"
        "?case_number_search=&registry_number=&judgement_type=All&division_1=125"
    )
    actual_url_after_apply = build_index_url(source_url, 0)
    reset_next_url = build_index_url(source_url, 1)

    selected = select_next_index_url(
        source_url=source_url,
        requested_page_number=21,
        actual_url=actual_url_after_apply,
        next_url=reset_next_url,
    )

    assert selected == build_index_url(source_url, 22)
    assert parse_page_number(selected) == 22


def test_settings_for_backfill_start_page(tmp_path: Path) -> None:
    settings = settings_for_backfill(
        make_settings(tmp_path),
        max_pages=80,
        max_items=2200,
        start_page=22,
        write_all_json=False,
    )

    assert settings.start_page == 22
    assert settings.max_pages == 80
    assert settings.max_items == 2200
