from __future__ import annotations

import asyncio
import json
import logging
import re
from datetime import datetime, timezone
from pathlib import Path

from .config import Settings
from .db import Database
from .parser import (
    DetailMetadata,
    IndexItem,
    clean_text,
    extract_next_page_url,
    is_failure_page,
    parse_detail_page,
    parse_index_page,
)
from .parties import parse_parties
from .pdfs import download_pdf, extract_pdf_sections
from .render import render_outputs

logger = logging.getLogger(__name__)


class ScraperError(RuntimeError):
    pass


def utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def run_id() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def debug_name(value: str) -> str:
    value = re.sub(r"[^A-Za-z0-9_.-]+", "-", value).strip("-")
    return value[:90] or "artifact"


async def save_debug(debug_dir: Path, name: str, html: str = "", page=None, note: str = "") -> None:
    debug_dir.mkdir(parents=True, exist_ok=True)
    prefix = debug_dir / debug_name(name)
    if html:
        (prefix.with_suffix(".html")).write_text(html, encoding="utf-8")
    if note:
        (prefix.with_suffix(".txt")).write_text(note, encoding="utf-8")
    if page is not None:
        try:
            await page.screenshot(path=str(prefix.with_suffix(".png")), full_page=True)
        except Exception as exc:
            logger.debug("could not capture debug screenshot: %s", exc)


async def accept_cookies(page) -> None:
    for selector in (
        "button.eu-cookie-compliance-default-button",
        "button:has-text('OK, I agree')",
    ):
        try:
            button = page.locator(selector).first
            if await button.count():
                await button.click(timeout=1500)
                return
        except Exception:
            continue


async def settle_page(page, settings: Settings) -> None:
    try:
        await page.wait_for_load_state("networkidle", timeout=settings.page_wait_timeout_ms)
    except Exception:
        pass
    try:
        await page.wait_for_selector("table, main, text=The API is currently unavailable", timeout=3000)
    except Exception:
        pass
    await page.wait_for_timeout(500)


def dedupe_items(items: list[IndexItem]) -> list[IndexItem]:
    seen: set[str] = set()
    deduped: list[IndexItem] = []
    for item in items:
        if item.item_key in seen:
            continue
        seen.add(item.item_key)
        deduped.append(item)
    return deduped


async def discover_items(context, settings: Settings, debug_dir: Path) -> list[IndexItem]:
    sources = [settings.source_url]
    if settings.fallback_source_url and settings.fallback_source_url != settings.source_url:
        sources.append(settings.fallback_source_url)

    last_error: Exception | None = None
    for source_url in sources:
        page = await context.new_page()
        try:
            items: list[IndexItem] = []
            seen_urls: set[str] = set()
            page_number = 0
            url = source_url
            while url:
                if url in seen_urls:
                    logger.warning("stopping discovery because pager looped back to %s", url)
                    break
                seen_urls.add(url)

                logger.info("discovering UPC index page %s: %s", page_number, url)
                await page.goto(url, wait_until="domcontentloaded", timeout=settings.navigation_timeout_ms)
                await accept_cookies(page)
                await settle_page(page, settings)
                html = await page.content()

                if is_failure_page(html):
                    await save_debug(debug_dir, f"index-page-{page_number}-failure", html, page)
                    raise ScraperError(f"UPC index page {page_number} appears to be unavailable or challenged")

                page_items = parse_index_page(html, page.url)
                if page_number == 0 and not page_items:
                    await save_debug(
                        debug_dir,
                        "index-page-0-no-results",
                        html,
                        page,
                        "No decision rows were found in the first index page.",
                    )
                    raise ScraperError("no decision rows found on the first UPC index page")

                logger.info("index page %s yielded %s items", page_number, len(page_items))
                items.extend(page_items)

                if settings.max_pages and page_number + 1 >= settings.max_pages:
                    logger.info("stopping discovery at MAX_PAGES=%s", settings.max_pages)
                    break

                next_url = extract_next_page_url(html, page.url)
                if not next_url:
                    break
                url = next_url
                page_number += 1

            return dedupe_items(items)
        except Exception as exc:
            last_error = exc
            logger.warning("discovery failed for %s: %s", source_url, exc)
        finally:
            await page.close()

    raise ScraperError(f"UPC discovery failed for all source URLs: {last_error}")


async def fetch_detail(context, item: IndexItem, settings: Settings, debug_dir: Path) -> DetailMetadata:
    if not item.node_url:
        raise ScraperError(f"item {item.item_key} has no UPC detail URL")
    page = await context.new_page()
    try:
        logger.info("fetching UPC detail page %s", item.node_url)
        await page.goto(item.node_url, wait_until="domcontentloaded", timeout=settings.navigation_timeout_ms)
        await accept_cookies(page)
        await settle_page(page, settings)
        html = await page.content()
        if is_failure_page(html):
            await save_debug(debug_dir, f"{item.item_key}-detail-failure", html, page)
            raise ScraperError(f"UPC detail page appears unavailable for {item.item_key}")
        metadata = parse_detail_page(html, page.url)
        if not metadata.pdf_links:
            await save_debug(
                debug_dir,
                f"{item.item_key}-detail-no-official-pdfs",
                html,
                page,
                "No official UPC decision/order PDF links were found. Metadata will still be persisted.",
            )
        return metadata
    finally:
        await page.close()


def merge_index_fallbacks(metadata: DetailMetadata, item: IndexItem) -> DetailMetadata:
    metadata.title_raw = metadata.title_raw or item.title_raw
    metadata.case_name_raw = metadata.case_name_raw or item.parties_raw
    metadata.parties_raw = metadata.parties_raw or item.parties_raw
    metadata.division = metadata.division or item.division
    metadata.case_number = metadata.case_number or item.case_number
    metadata.registry_number = metadata.registry_number or item.registry_number
    metadata.order_or_decision_number = metadata.order_or_decision_number or item.order_or_decision_number
    metadata.decision_date = metadata.decision_date or item.decision_date
    metadata.document_type = metadata.document_type or item.title_raw
    metadata.type_of_action = metadata.type_of_action or item.type_of_action
    return metadata


def split_keywords(raw: str) -> list[str]:
    return [part.strip() for part in re.split(r"\s*;\s*", raw or "") if part.strip()]


def empty_document_values(metadata: DetailMetadata) -> dict[str, str]:
    first_pdf = metadata.pdf_links[0].url if metadata.pdf_links else ""
    return {
        "pdf_url_official": first_pdf,
        "pdf_url_mirror": "",
        "pdf_sha256": "",
        "file_path": "",
    }


async def ingest_item(context, db: Database, settings: Settings, item: IndexItem, debug_dir: Path) -> None:
    now = utc_now()
    detail_error = ""
    try:
        metadata = await fetch_detail(context, item, settings, debug_dir)
    except Exception as exc:
        detail_error = f"detail fetch/parse failed: {exc}"
        logger.exception("continuing with index metadata for %s after detail failure", item.item_key)
        await save_debug(
            debug_dir,
            f"{item.item_key}-detail-error",
            note=json.dumps({"item": item.__dict__, "error": str(exc)}, ensure_ascii=False, indent=2),
        )
        metadata = DetailMetadata()

    metadata = merge_index_fallbacks(metadata, item)
    documents = []
    download_errors: list[str] = []

    for index, pdf_link in enumerate(metadata.pdf_links):
        try:
            documents.append(
                await download_pdf(
                    context=context,
                    link=pdf_link,
                    public_dir=settings.public_dir,
                    decision_date=metadata.decision_date,
                    node_url=item.node_url,
                    order_ref=metadata.order_or_decision_number or item.order_or_decision_number,
                    downloaded_at=now,
                    is_primary=index == 0,
                )
            )
        except Exception as exc:
            message = f"PDF download failed for {pdf_link.url}: {exc}"
            logger.exception(message)
            download_errors.append(message)
            await save_debug(
                debug_dir,
                f"{item.item_key}-pdf-{index}-error",
                note=json.dumps(
                    {"item": item.__dict__, "pdf_link": pdf_link.__dict__, "error": str(exc)},
                    ensure_ascii=False,
                    indent=2,
                ),
            )

    primary = documents[0] if documents else empty_document_values(metadata)
    if documents and (not metadata.headnote_text or not metadata.keywords_raw) and primary.get("file_path"):
        pdf_headnotes, pdf_keywords = extract_pdf_sections(Path(str(primary["file_path"])))
        if not metadata.headnote_text and pdf_headnotes:
            metadata.headnote_raw = pdf_headnotes
            metadata.headnote_text = clean_text(pdf_headnotes)
        if not metadata.keywords_raw and pdf_keywords:
            metadata.keywords_raw = pdf_keywords
            metadata.keywords_list = split_keywords(pdf_keywords)

    party_data = parse_parties(metadata.parties_raw)
    last_error = "\n".join(part for part in [detail_error, *download_errors] if part)
    decision_values = {
        "item_key": item.item_key,
        "title_raw": metadata.title_raw,
        "case_name_raw": metadata.case_name_raw,
        "parties_raw": metadata.parties_raw,
        "parties_json": party_data.parties_json,
        "party_names_all": party_data.party_names_all,
        "party_names_normalised": party_data.party_names_normalised,
        "primary_adverse_caption": party_data.primary_adverse_caption,
        "adverse_pair_key": party_data.adverse_pair_key,
        "division": metadata.division,
        "panel": metadata.panel,
        "case_number": metadata.case_number,
        "registry_number": metadata.registry_number,
        "order_or_decision_number": metadata.order_or_decision_number,
        "decision_date": metadata.decision_date,
        "document_type": metadata.document_type,
        "type_of_action": metadata.type_of_action,
        "language": metadata.language,
        "headnote_raw": metadata.headnote_raw,
        "headnote_text": metadata.headnote_text,
        "keywords_raw": metadata.keywords_raw,
        "keywords_list": metadata.keywords_list,
        "pdf_url_official": str(primary.get("pdf_url_official", "")),
        "pdf_url_mirror": str(primary.get("pdf_url_mirror", "")),
        "node_url": item.node_url,
        "pdf_sha256": str(primary.get("pdf_sha256", "")),
        "first_seen_at": now,
        "last_seen_at": now,
        "ingested_at": now,
        "alerted_at": now,
        "last_error": last_error,
    }
    decision_id = db.upsert_decision(decision_values)
    if documents:
        db.replace_documents(decision_id, documents)
    db.mark_seen(item, now, bootstrapped=False)
    if documents:
        logger.info("ingested %s with %s mirrored PDF(s)", item.item_key, len(documents))
    else:
        logger.warning("ingested %s metadata without mirrored PDFs", item.item_key)


async def run_ingestion(settings: Settings, bootstrap: bool = False) -> dict[str, int | str]:
    settings.ensure_dirs()
    db = Database(settings.db_path)
    db.init()
    if not (settings.public_dir / "latest.json").exists():
        render_outputs(db, settings.public_dir)

    this_run_id = run_id()
    debug_run_dir = settings.debug_dir / this_run_id
    debug_run_dir.mkdir(parents=True, exist_ok=True)
    started_at = utc_now()
    run_db_id = db.start_run(started_at, str(debug_run_dir))
    discovered_count = 0
    new_count = 0
    item_errors: list[str] = []

    try:
        try:
            from playwright.async_api import async_playwright
        except ImportError as exc:
            raise ScraperError("playwright is not installed; install requirements.txt or use the Docker image") from exc

        async with async_playwright() as playwright:
            browser = await playwright.chromium.launch(
                headless=True,
                args=["--no-sandbox", "--disable-dev-shm-usage"],
            )
            context = await browser.new_context(
                accept_downloads=False,
                user_agent=(
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/121.0.0.0 Safari/537.36"
                ),
                viewport={"width": 1440, "height": 1200},
            )
            try:
                items = await discover_items(context, settings, debug_run_dir)
                discovered_count = len(items)
                logger.info("discovered %s UPC index item(s)", discovered_count)

                if bootstrap:
                    now = utc_now()
                    for item in items:
                        db.mark_seen(item, now, bootstrapped=True)
                    render_outputs(db, settings.public_dir)
                    db.finish_run(run_db_id, utc_now(), "success", discovered_count, 0)
                    return {"status": "success", "discovered_count": discovered_count, "new_count": 0}

                for item in items:
                    now = utc_now()
                    if db.has_seen(item.item_key):
                        db.touch_seen(item, now)
                        continue
                    try:
                        await ingest_item(context, db, settings, item, debug_run_dir)
                        new_count += 1
                    except Exception as exc:
                        logger.exception("failed to persist %s", item.item_key)
                        item_errors.append(f"{item.item_key}: {exc}")
                        await save_debug(
                            debug_run_dir,
                            f"{item.item_key}-error",
                            note=json.dumps({"item": item.__dict__, "error": str(exc)}, ensure_ascii=False, indent=2),
                        )
            finally:
                await context.close()
                await browser.close()

        render_outputs(db, settings.public_dir)
        status = "success" if not item_errors else "partial_success"
        db.finish_run(
            run_db_id,
            utc_now(),
            status,
            discovered_count,
            new_count,
            "\n".join(item_errors),
        )
        return {"status": status, "discovered_count": discovered_count, "new_count": new_count}
    except Exception as exc:
        logger.exception("ingestion run failed")
        db.finish_run(run_db_id, utc_now(), "failed", discovered_count, new_count, str(exc))
        raise


def run_ingestion_sync(settings: Settings, bootstrap: bool = False) -> dict[str, int | str]:
    return asyncio.run(run_ingestion(settings, bootstrap=bootstrap))
