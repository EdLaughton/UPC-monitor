import asyncio
import re
from datetime import date
from dataclasses import replace
from pathlib import Path
from urllib.parse import parse_qsl, urlparse

from upc_ingester.__main__ import settings_for_backfill
from upc_ingester.config import Settings
from upc_ingester.db import Database, INDEX_ONLY_LAST_ERROR
from upc_ingester.parser import IndexItem
from upc_ingester.scraper import (
    build_index_url,
    build_date_index_url,
    cap_items,
    date_windows,
    discover_items,
    fetch_detail,
    ingest_discovered_items,
    load_and_parse_index_page,
    needs_enrichment,
    parse_page_number,
    parse_or_submit_index_page,
    ScraperError,
    select_next_index_url,
    upsert_index_only_item,
)


def index_html(page_number: int, item_key: str, next_page: int | None = None) -> str:
    next_link = f'<a rel="next" href="?page={next_page}">Next</a>' if next_page is not None else ""
    node_id = item_key.removeprefix("node-")
    return f"""
    <!doctype html>
    <html>
    <body>
      <table>
        <thead>
          <tr>
            <th>Date</th>
            <th>Registry number/Order reference</th>
            <th>Court</th>
            <th>Type of action</th>
            <th>Parties</th>
            <th>UPC Document</th>
          </tr>
        </thead>
        <tbody>
          <tr>
            <td>{15 + page_number} April, 2026</td>
            <td>ACT_{node_id}/2026<br>ORD_{node_id}/2026<br><a href="/en/node/{node_id}">Full Details</a></td>
            <td>Local Division Paris</td>
            <td>Infringement action</td>
            <td>Claimant {node_id}<br>v.<br>Defendant {node_id}</td>
            <td>Order</td>
          </tr>
        </tbody>
      </table>
      {next_link}
    </body>
    </html>
    """


def failure_html() -> str:
    return "<html><body>The API is currently unavailable</body></html>"


def unsubmitted_form_html() -> str:
    return """
    <!doctype html>
    <html>
    <body>
      <p>Please submit the form below to get your result</p>
      <form data-drupal-selector="views-exposed-form-collection-of-judgements-page-1">
        <input id="edit-submit-collection-of-judgements" type="submit" value="Apply">
      </form>
    </body>
    </html>
    """


def page_html_value(value) -> str:
    if isinstance(value, list):
        if len(value) > 1:
            return value.pop(0)
        return value[0]
    return value


def page_html_peek(value) -> str:
    if isinstance(value, list):
        return value[0] if value else ""
    return value


class FakeLocator:
    def __init__(self, page: "FakePage", selector: str):
        self.page = page
        self.selector = selector
        self.first = self

    async def count(self) -> int:
        html = self.page.last_html or page_html_peek(
            self.page.pages.get(self.page.current_window, "")
            if self.page.current_window is not None
            else self.page.pages.get(self.page.current_page, "")
        )
        if "pager" in self.selector or "rel='next'" in self.selector or "Go to next" in self.selector:
            return 1 if self.page.next_page is not None else 0
        if "edit-submit-collection-of-judgements" in self.selector or "views-exposed-form" in self.selector:
            return 1 if "edit-submit-collection-of-judgements" in html else 0
        return 0

    async def get_attribute(self, name: str) -> str:
        if name == "href" and self.page.next_page is not None:
            return f"?page={self.page.next_page}"
        return ""

    async def click(self, timeout: int = 0) -> None:
        if "edit-submit-collection-of-judgements" in self.selector or "views-exposed-form" in self.selector:
            self.page.apply_clicks += 1
            self.page.current_page = 0
            self.page.url = build_index_url(self.page.base_url, 0)
            return
        if self.page.next_page is None:
            return
        self.page.history.append((self.page.current_page, self.page.url, self.page.current_window))
        self.page.current_page = self.page.next_page
        self.page.url = build_index_url(self.page.base_url, self.page.current_page)
        self.page.current_window = None
        self.page.last_html = ""


class FakePage:
    def __init__(self, pages: dict, base_url: str = "https://example.test/index?filter=1"):
        self.pages = pages
        self.base_url = base_url
        self.current_page = 0
        self.current_window: tuple[str, str] | None = None
        self.url = build_index_url(base_url, 0)
        self.apply_clicks = 0
        self.visited_urls: list[str] = []
        self.last_html = ""
        self.history: list[tuple[int, str, tuple[str, str] | None]] = []
        self.fail_go_back = False

    @property
    def next_page(self) -> int | None:
        value = self.pages.get(self.current_window, "") if self.current_window is not None else self.pages.get(self.current_page, "")
        html = page_html_peek(value)
        match = re.search(r'href="\?page=(\d+)"', html)
        return int(match.group(1)) if match else None

    async def goto(self, url: str, wait_until: str = "", timeout: int = 0) -> None:
        self.url = url
        self.visited_urls.append(url)
        self.last_html = ""
        self.current_page = parse_page_number(url)
        from_query = dict(parse_qsl(urlparse(url).query, keep_blank_values=True))
        start = from_query.get("judgement_date_from[date]", "")
        end = from_query.get("judgement_date_to[date]", "")
        self.current_window = (start, end) if start or end else None

    async def go_back(self, wait_until: str = "", timeout: int = 0) -> None:
        if self.fail_go_back or not self.history:
            raise RuntimeError("cannot go back")
        self.current_page, self.url, self.current_window = self.history.pop()
        self.last_html = ""

    async def content(self) -> str:
        if self.current_window is not None:
            self.last_html = page_html_value(self.pages.get(self.current_window, ""))
            return self.last_html
        self.last_html = page_html_value(self.pages.get(self.current_page, ""))
        return self.last_html

    def locator(self, selector: str) -> FakeLocator:
        return FakeLocator(self, selector)

    async def wait_for_load_state(self, state: str, timeout: int = 0) -> None:
        return None

    async def wait_for_selector(self, selector: str, timeout: int = 0) -> None:
        return None

    async def wait_for_timeout(self, timeout: int) -> None:
        return None

    async def close(self) -> None:
        return None


class FakeContext:
    def __init__(self, pages: dict, *, fail_go_back: bool = False):
        self.pages = pages
        self.last_page: FakePage | None = None
        self.fail_go_back = fail_go_back

    async def new_page(self) -> FakePage:
        self.last_page = FakePage(self.pages)
        self.last_page.fail_go_back = self.fail_go_back
        return self.last_page


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
        date_from="",
        date_to="",
        date_window_days=0,
        index_page_retry_delay_seconds=0,
        index_page_max_retries=3,
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


def test_build_date_index_url_preserves_query_and_sets_date_window() -> None:
    url = (
        "https://www.unifiedpatentcourt.org/en/decisions-and-orders"
        "?case_number_search=&registry_number=&judgement_type=All&division_1=125&page=3"
    )

    built = build_date_index_url(url, date(2026, 4, 1), date(2026, 4, 7))

    assert "page=" not in built
    assert "case_number_search=" in built
    assert "judgement_date_from%5Bdate%5D=2026-04-01" in built
    assert "judgement_date_to%5Bdate%5D=2026-04-07" in built


def test_date_windows_chunk_range() -> None:
    assert date_windows(date(2026, 4, 1), date(2026, 4, 10), 4) == [
        (date(2026, 4, 1), date(2026, 4, 4)),
        (date(2026, 4, 5), date(2026, 4, 8)),
        (date(2026, 4, 9), date(2026, 4, 10)),
    ]


def test_backward_pager_selects_deterministic_next_page_url() -> None:
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


def test_settings_for_backfill_overrides_crawl_limits(tmp_path: Path) -> None:
    settings = settings_for_backfill(
        make_settings(tmp_path),
        max_pages=80,
        max_items=2200,
        date_from="2024-01-01",
        date_to="2026-04-24",
        date_window_days=7,
        index_page_retry_delay_seconds=30,
        index_page_max_retries=3,
        write_all_json=False,
    )

    assert settings.max_pages == 80
    assert settings.max_items == 2200
    assert settings.date_from == "2024-01-01"
    assert settings.date_to == "2026-04-24"
    assert settings.date_window_days == 7


def test_first_index_page_unsubmitted_form_still_clicks_apply(tmp_path: Path) -> None:
    pages = {
        0: [unsubmitted_form_html(), index_html(0, "node-100")],
    }
    page = FakePage(pages)
    settings = make_settings(tmp_path)
    html = asyncio.run(page.content())

    _, items, submitted = asyncio.run(parse_or_submit_index_page(page, settings, tmp_path / "debug", 0, html))

    assert submitted is True
    assert page.apply_clicks == 1
    assert [item.item_key for item in items] == ["node-100"]


def test_paginated_unsubmitted_form_does_not_click_apply_and_is_retryable(tmp_path: Path) -> None:
    pages = {
        1: unsubmitted_form_html(),
    }
    page = FakePage(pages)
    settings = make_settings(tmp_path)
    page.url = build_index_url(page.base_url, 1)
    page.current_page = 1
    html = asyncio.run(page.content())

    try:
        asyncio.run(parse_or_submit_index_page(page, settings, tmp_path / "debug", 1, html))
    except ScraperError as exc:
        assert "rendered unsubmitted form/reset state" in str(exc)
    else:
        raise AssertionError("paginated unsubmitted form should be retryable instead of submitted")

    assert page.apply_clicks == 0


def test_paginated_unsubmitted_form_retries_same_url_and_then_succeeds(tmp_path: Path) -> None:
    pages = {
        1: [unsubmitted_form_html(), index_html(1, "node-101")],
    }
    page = FakePage(pages)
    settings = replace(
        make_settings(tmp_path),
        index_page_retry_delay_seconds=0,
        index_page_max_retries=1,
    )
    url = build_index_url(page.base_url, 1)

    _, items, submitted = asyncio.run(load_and_parse_index_page(page, settings, tmp_path / "debug", 1, url))

    assert submitted is False
    assert page.apply_clicks == 0
    assert page.visited_urls == [url, url]
    assert [item.item_key for item in items] == ["node-101"]


def test_duplicate_item_keys_are_deduped_after_discovery(tmp_path: Path) -> None:
    page_zero = index_html(0, "node-100", next_page=1)
    pages = {
        0: page_zero,
        1: page_zero,
    }
    settings = replace(make_settings(tmp_path), source_url="https://example.test/index?filter=1", max_pages=2, max_items=0)

    items = asyncio.run(discover_items(FakeContext(pages), settings, tmp_path / "debug"))

    assert [item.item_key for item in items] == ["node-100"]


def test_index_page_failure_retries_same_page_before_collecting(tmp_path: Path) -> None:
    pages = {
        0: index_html(0, "node-100", next_page=1),
        1: [failure_html(), index_html(1, "node-101", next_page=None)],
    }
    settings = replace(
        make_settings(tmp_path),
        source_url="https://example.test/index?filter=1",
        max_pages=2,
        max_items=0,
        index_page_retry_delay_seconds=0,
        index_page_max_retries=1,
    )

    items = asyncio.run(discover_items(FakeContext(pages), settings, tmp_path / "debug"))

    assert [item.item_key for item in items] == ["node-100", "node-101"]


def test_discovery_prefers_clicking_next_pager_before_direct_page_url(tmp_path: Path) -> None:
    pages = {
        0: index_html(0, "node-100", next_page=1),
        1: index_html(1, "node-101", next_page=None),
    }
    context = FakeContext(pages)
    settings = replace(make_settings(tmp_path), source_url="https://example.test/index?filter=1", max_pages=2, max_items=0)

    items = asyncio.run(discover_items(context, settings, tmp_path / "debug"))

    assert [item.item_key for item in items] == ["node-100", "node-101"]
    assert context.last_page is not None
    assert context.last_page.visited_urls == [build_index_url("https://example.test/index?filter=1", 0)]


def test_clicked_navigation_failure_restores_last_good_page_and_clicks_again(tmp_path: Path) -> None:
    pages = {page: index_html(page, f"node-{100 + page}", next_page=page + 1) for page in range(6)}
    pages[6] = [failure_html(), index_html(6, "node-106", next_page=None)]
    context = FakeContext(pages)
    settings = replace(
        make_settings(tmp_path),
        source_url="https://example.test/index?filter=1",
        max_pages=7,
        max_items=0,
        index_page_retry_delay_seconds=0,
        index_page_max_retries=1,
    )

    items = asyncio.run(discover_items(context, settings, tmp_path / "debug"))

    assert [item.item_key for item in items] == [f"node-{100 + page}" for page in range(7)]
    assert getattr(items, "partial") is False
    assert context.last_page is not None
    assert build_index_url("https://example.test/index?filter=1", 6) not in context.last_page.visited_urls


def test_paginated_reset_after_clicked_navigation_retries_from_last_good_without_apply(tmp_path: Path) -> None:
    pages = {
        0: index_html(0, "node-100", next_page=1),
        1: [unsubmitted_form_html(), index_html(1, "node-101", next_page=None)],
    }
    context = FakeContext(pages)
    settings = replace(
        make_settings(tmp_path),
        source_url="https://example.test/index?filter=1",
        max_pages=2,
        max_items=0,
        index_page_retry_delay_seconds=0,
        index_page_max_retries=1,
    )

    items = asyncio.run(discover_items(context, settings, tmp_path / "debug"))

    assert [item.item_key for item in items] == ["node-100", "node-101"]
    assert context.last_page is not None
    assert context.last_page.apply_clicks == 0
    assert build_index_url("https://example.test/index?filter=1", 1) not in context.last_page.visited_urls


def test_restore_last_good_page_failure_returns_partial_results(tmp_path: Path) -> None:
    pages = {
        0: [index_html(0, "node-100", next_page=1), failure_html()],
        1: [failure_html(), index_html(1, "node-101", next_page=None)],
    }
    settings = replace(
        make_settings(tmp_path),
        source_url="https://example.test/index?filter=1",
        max_pages=2,
        max_items=0,
        index_page_retry_delay_seconds=0,
        index_page_max_retries=1,
    )

    items = asyncio.run(discover_items(FakeContext(pages, fail_go_back=True), settings, tmp_path / "debug"))

    assert [item.item_key for item in items] == ["node-100"]
    assert getattr(items, "partial") is True
    assert "could not restore last good UPC index page 0" in getattr(items, "stopped_reason")


def test_index_page_retry_exhaustion_returns_partial_results(tmp_path: Path) -> None:
    pages = {
        0: index_html(0, "node-100", next_page=1),
        1: [failure_html(), failure_html()],
    }
    settings = replace(
        make_settings(tmp_path),
        source_url="https://example.test/index?filter=1",
        max_pages=2,
        max_items=0,
        index_page_retry_delay_seconds=0,
        index_page_max_retries=1,
    )

    items = asyncio.run(discover_items(FakeContext(pages), settings, tmp_path / "debug"))

    assert [item.item_key for item in items] == ["node-100"]
    assert getattr(items, "partial") is True
    assert "appears to be unavailable or challenged" in getattr(items, "stopped_reason")


def test_paginated_unsubmitted_form_retry_exhaustion_returns_partial_results(tmp_path: Path) -> None:
    pages = {
        0: index_html(0, "node-100", next_page=1),
        1: [unsubmitted_form_html(), unsubmitted_form_html()],
    }
    settings = replace(
        make_settings(tmp_path),
        source_url="https://example.test/index?filter=1",
        max_pages=2,
        max_items=0,
        index_page_retry_delay_seconds=0,
        index_page_max_retries=1,
    )

    items = asyncio.run(discover_items(FakeContext(pages), settings, tmp_path / "debug"))

    assert [item.item_key for item in items] == ["node-100"]
    assert getattr(items, "partial") is True
    assert "rendered unsubmitted form/reset state" in getattr(items, "stopped_reason")


def test_detail_page_failure_retries_same_url_before_parsing(tmp_path: Path) -> None:
    detail_html = (Path(__file__).parent / "fixtures" / "detail.html").read_text(encoding="utf-8")
    pages = {
        0: [failure_html(), detail_html],
    }
    settings = replace(
        make_settings(tmp_path),
        index_page_retry_delay_seconds=0,
        index_page_max_retries=1,
    )

    metadata = asyncio.run(fetch_detail(FakeContext(pages), make_index_item(), settings, tmp_path / "debug"))

    assert metadata.case_number == "UPC_CFI_101/2026"
    assert metadata.pdf_links[0].url.endswith("ORD_10339_2026_EN.pdf")


def test_date_window_discovery_collects_shallow_windows(tmp_path: Path) -> None:
    pages = {
        ("2026-04-01", "2026-04-07"): index_html(1, "node-101"),
        ("2026-04-08", "2026-04-14"): index_html(2, "node-102"),
    }
    settings = replace(
        make_settings(tmp_path),
        source_url="https://example.test/index?filter=1",
        date_from="2026-04-01",
        date_to="2026-04-14",
        date_window_days=7,
        max_pages=0,
        max_items=0,
    )

    items = asyncio.run(discover_items(FakeContext(pages), settings, tmp_path / "debug"))

    assert [item.item_key for item in items] == ["node-101", "node-102"]


def test_date_window_failure_retries_same_window_before_collecting(tmp_path: Path) -> None:
    pages = {
        ("2026-04-01", "2026-04-07"): [failure_html(), index_html(1, "node-101")],
    }
    settings = replace(
        make_settings(tmp_path),
        source_url="https://example.test/index?filter=1",
        date_from="2026-04-01",
        date_to="2026-04-07",
        date_window_days=7,
        max_pages=0,
        max_items=0,
        index_page_retry_delay_seconds=0,
        index_page_max_retries=1,
    )

    items = asyncio.run(discover_items(FakeContext(pages), settings, tmp_path / "debug"))

    assert [item.item_key for item in items] == ["node-101"]


def test_date_window_discovery_splits_truncated_window_without_counting_parent(tmp_path: Path) -> None:
    pages = {
        ("2026-04-01", "2026-04-02"): index_html(0, "node-100", next_page=1),
        ("2026-04-01", "2026-04-01"): index_html(1, "node-101"),
        ("2026-04-02", "2026-04-02"): index_html(2, "node-102"),
    }
    settings = replace(
        make_settings(tmp_path),
        source_url="https://example.test/index?filter=1",
        date_from="2026-04-01",
        date_to="2026-04-02",
        date_window_days=2,
        max_pages=0,
        max_items=0,
    )

    items = asyncio.run(discover_items(FakeContext(pages), settings, tmp_path / "debug"))

    assert [item.item_key for item in items] == ["node-101", "node-102"]
