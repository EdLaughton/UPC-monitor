from __future__ import annotations

import asyncio
import json
import logging
import re
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from urllib.parse import parse_qsl, urlencode, urljoin, urlparse, urlunparse

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


def strip_query(url: str) -> str:
    parsed = urlparse(url)
    return urlunparse(parsed._replace(query="", fragment=""))


def build_index_url(base_url: str, page_number: int) -> str:
    parsed = urlparse(base_url)
    query = dict(parse_qsl(parsed.query, keep_blank_values=True))
    if page_number <= 0:
        query.pop("page", None)
    else:
        query["page"] = str(page_number)
    return urlunparse(parsed._replace(query=urlencode(query), fragment=""))


def build_date_index_url(base_url: str, start: date, end: date) -> str:
    parsed = urlparse(base_url)
    query = dict(parse_qsl(parsed.query, keep_blank_values=True))
    query.pop("page", None)
    query["judgement_date_from[date]"] = start.isoformat()
    query["judgement_date_to[date]"] = end.isoformat()
    return urlunparse(parsed._replace(query=urlencode(query), fragment=""))


def parse_page_number(url: str) -> int:
    parsed = urlparse(url)
    query = dict(parse_qsl(parsed.query, keep_blank_values=True))
    try:
        return int(query.get("page") or 0)
    except ValueError:
        return 0


def select_next_index_url(source_url: str, requested_page_number: int, actual_url: str, next_url: str) -> str:
    requested_next = requested_page_number + 1
    direct_next = build_index_url(source_url, requested_next)
    if not next_url:
        return ""
    absolute_next = urljoin(actual_url or source_url, next_url)
    parsed_next = parse_page_number(absolute_next)
    if parsed_next <= requested_page_number:
        logger.warning(
            "UPC next pager moved backwards or reset: requested_page=%s actual_url=%s parsed_next_page=%s; using direct URL %s",
            requested_page_number,
            actual_url,
            parsed_next,
            direct_next,
        )
        return direct_next
    return absolute_next


def item_signature(items: list[IndexItem]) -> tuple[str, ...]:
    return tuple(item.item_key for item in items)


def parse_result_total(html: str) -> int | None:
    match = re.search(r"Displaying\s+\d+\s*-\s*\d+\s+of\s+(\d+)", html, flags=re.I)
    if not match:
        return None
    return int(match.group(1))


def split_date_range(start: date, end: date) -> tuple[tuple[date, date], tuple[date, date]]:
    days = (end - start).days
    midpoint = start + timedelta(days=days // 2)
    return (start, midpoint), (midpoint + timedelta(days=1), end)


def configured_date_range(settings: Settings) -> tuple[date, date]:
    start = date.fromisoformat(settings.date_from or "2024-01-01")
    end = date.fromisoformat(settings.date_to) if settings.date_to else datetime.now(timezone.utc).date()
    if end < start:
        raise ScraperError(f"DATE_TO {end.isoformat()} is before DATE_FROM {start.isoformat()}")
    return start, end


def date_windows(start: date, end: date, days: int) -> list[tuple[date, date]]:
    if days < 1:
        raise ScraperError("DATE_WINDOW_DAYS must be at least 1 when date-window discovery is enabled")
    windows: list[tuple[date, date]] = []
    cursor = start
    while cursor <= end:
        window_end = min(end, cursor + timedelta(days=days - 1))
        windows.append((cursor, window_end))
        cursor = window_end + timedelta(days=1)
    return windows


def discovery_sources(settings: Settings) -> list[str]:
    candidates = [settings.source_url, strip_query(settings.source_url)]
    if settings.fallback_source_url:
        candidates.extend([settings.fallback_source_url, strip_query(settings.fallback_source_url)])
    seen: set[str] = set()
    unique: list[str] = []
    for candidate in candidates:
        if not candidate or candidate in seen:
            continue
        seen.add(candidate)
        unique.append(candidate)
    return unique


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


def is_unsubmitted_decisions_form(html: str) -> bool:
    lower = html.lower()
    return (
        "edit-submit-collection-of-judgements" in lower
        and (
            "please submit the form below to get your result" in lower
            or "select any filter and click on apply to see results" in lower
        )
    )


async def submit_decisions_form(page, settings: Settings) -> bool:
    """Submit the UPC exposed filter form when it renders without results.

    Some UPC responses render the Decisions and Orders search form plus the
    warning "Please submit the form below to get your result" instead of the
    result table. The debug HTML shows the relevant submit input is
    edit-submit-collection-of-judgements. Treat that as a normal pre-results
    state and click Apply before concluding that discovery failed.
    """
    selectors = (
        "#edit-submit-collection-of-judgements",
        "input[data-drupal-selector='edit-submit-collection-of-judgements']",
        "form[data-drupal-selector='views-exposed-form-collection-of-judgements-page-1'] input[type='submit'][value='Apply']",
        "form.views-exposed-form-no-auto-submit input[type='submit'][value='Apply']",
    )
    for selector in selectors:
        try:
            locator = page.locator(selector).first
            if not await locator.count():
                continue
            logger.info("submitting UPC Decisions and Orders filter form via %s", selector)
            await locator.click(timeout=settings.navigation_timeout_ms)
            await settle_page(page, settings)
            return True
        except Exception as exc:
            logger.debug("could not submit UPC decisions form using %s: %s", selector, exc)
    return False


async def click_next_pager(page, settings: Settings) -> bool:
    """Advance by clicking the rendered Drupal pager's next link.

    The UPC page emits ordinary pager hrefs, but in practice directly loading
    the extracted page=1 URL can sometimes produce an empty result set. Clicking
    the rendered link keeps the same browser/page state and gives Drupal's
    behaviours a chance to handle the pager in the same way as a normal browser.
    """
    selectors = (
        "li.pager__item--next a[href]",
        "a[rel='next'][href]",
        "a[title='Go to next page'][href]",
    )
    for selector in selectors:
        try:
            locator = page.locator(selector).first
            if not await locator.count():
                continue
            href = await locator.get_attribute("href")
            logger.info("clicking UPC next pager link %s", href or "")
            await locator.click(timeout=settings.navigation_timeout_ms)
            await settle_page(page, settings)
            return True
        except Exception as exc:
            logger.debug("could not click pager link %s: %s", selector, exc)
    return False


def dedupe_items(items: list[IndexItem]) -> list[IndexItem]:
    seen: set[str] = set()
    deduped: list[IndexItem] = []
    for item in items:
        if item.item_key in seen:
            continue
        seen.add(item.item_key)
        deduped.append(item)
    return deduped


def cap_items(items: list[IndexItem], settings: Settings) -> list[IndexItem]:
    if settings.max_items and len(items) > settings.max_items:
        logger.info("limiting discovery to MAX_ITEMS=%s latest item(s)", settings.max_items)
        return items[: settings.max_items]
    return items


def needs_enrichment(decision: dict[str, object] | None) -> bool:
    if decision is None:
        return True
    if str(decision.get("last_error") or "").strip():
        return True
    documents = decision.get("documents")
    if not documents:
        return True
    return not (
        str(decision.get("pdf_url_mirror") or "").strip()
        and str(decision.get("pdf_sha256") or "").strip()
    )


async def parse_or_submit_index_page(page, settings: Settings, debug_dir: Path, page_number: int) -> tuple[str, list[IndexItem], bool]:
    html = await page.content()
    if is_failure_page(html):
        await save_debug(debug_dir, f"index-page-{page_number}-failure", html, page)
        raise ScraperError(f"UPC index page {page_number} appears to be unavailable or challenged")

    page_items = parse_index_page(html, page.url)
    if page_items or not is_unsubmitted_decisions_form(html):
        return html, page_items, False

    logger.info("UPC index page %s rendered the unsubmitted search form; clicking Apply", page_number)
    submitted = await submit_decisions_form(page, settings)
    if not submitted:
        return html, page_items, False

    html = await page.content()
    if is_failure_page(html):
        await save_debug(debug_dir, f"index-page-{page_number}-post-submit-failure", html, page)
        raise ScraperError(f"UPC index page {page_number} became unavailable or challenged after form submit")
    page_items = parse_index_page(html, page.url)
    logger.info("UPC index page %s yielded %s item(s) after form submit", page_number, len(page_items))
    return html, page_items, True


async def discover_date_window_items_for_source(page, settings: Settings, debug_dir: Path, source_url: str) -> list[IndexItem]:
    start, end = configured_date_range(settings)
    stack = list(reversed(date_windows(start, end, settings.date_window_days)))
    items: list[IndexItem] = []
    windows_collected = 0
    windows_seen: set[tuple[date, date]] = set()

    logger.info(
        "starting UPC date-window discovery from %s to %s with DATE_WINDOW_DAYS=%s",
        start.isoformat(),
        end.isoformat(),
        settings.date_window_days,
    )

    while stack:
        window_start, window_end = stack.pop()
        if (window_start, window_end) in windows_seen:
            logger.warning("skipping duplicate date window %s..%s", window_start, window_end)
            continue
        windows_seen.add((window_start, window_end))

        url = build_date_index_url(source_url, window_start, window_end)
        logger.info("discovering UPC date window %s..%s: %s", window_start, window_end, url)
        await page.goto(url, wait_until="domcontentloaded", timeout=settings.navigation_timeout_ms)
        await accept_cookies(page)
        await settle_page(page, settings)
        html, page_items, submitted_form = await parse_or_submit_index_page(page, settings, debug_dir, 0)
        actual_url = page.url
        total = parse_result_total(html)
        has_next_page = bool(extract_next_page_url(html, actual_url))
        days = (window_end - window_start).days + 1

        logger.info(
            "UPC date window %s..%s actual_url=%s items=%s total=%s has_next=%s submitted_form=%s",
            window_start,
            window_end,
            actual_url,
            len(page_items),
            total if total is not None else "unknown",
            has_next_page,
            submitted_form,
        )

        is_truncated = has_next_page or (total is not None and total > len(page_items))
        if is_truncated and window_start < window_end:
            earlier, later = split_date_range(window_start, window_end)
            logger.info(
                "splitting UPC date window %s..%s because it appears truncated; next windows %s..%s and %s..%s",
                window_start,
                window_end,
                earlier[0],
                earlier[1],
                later[0],
                later[1],
            )
            stack.append(later)
            stack.append(earlier)
            continue

        if is_truncated and window_start < window_end:
            logger.warning(
                "UPC date window %s..%s still appears truncated at configured window size; storing first page only",
                window_start,
                window_end,
            )
        elif is_truncated:
            logger.warning(
                "UPC single-day date window %s has more than one page; storing first page only",
                window_start,
            )

        items.extend(page_items)
        windows_collected += 1
        logger.info(
            "date-window discovery progress: windows_collected=%s cumulative_item_count=%s",
            windows_collected,
            len(items),
        )
        if settings.max_items and len(items) >= settings.max_items:
            logger.info("stopping date-window discovery at MAX_ITEMS=%s", settings.max_items)
            break
        if settings.max_pages and windows_collected >= settings.max_pages:
            logger.info("stopping date-window discovery at MAX_PAGES=%s collected window(s)", settings.max_pages)
            break

    return cap_items(dedupe_items(items), settings)


async def discover_date_window_items(context, settings: Settings, debug_dir: Path) -> list[IndexItem]:
    last_error: Exception | None = None
    for source_url in discovery_sources(settings):
        page = await context.new_page()
        try:
            return await discover_date_window_items_for_source(page, settings, debug_dir, source_url)
        except Exception as exc:
            last_error = exc
            logger.warning("date-window discovery failed for %s: %s", source_url, exc)
        finally:
            await page.close()
    raise ScraperError(f"UPC date-window discovery failed for all source URLs: {last_error}")


async def discover_items(context, settings: Settings, debug_dir: Path) -> list[IndexItem]:
    if settings.date_window_days:
        return await discover_date_window_items(context, settings, debug_dir)

    sources = discovery_sources(settings)

    last_error: Exception | None = None
    for source_url in sources:
        page = await context.new_page()
        items: list[IndexItem] = []
        try:
            seen_urls: set[str] = set()
            page_signatures: dict[int, tuple[str, ...]] = {}
            page_number = 0
            pages_collected = 0
            url = build_index_url(source_url, 0)
            needs_goto = True
            while url:
                if needs_goto and url in seen_urls:
                    logger.warning("stopping discovery because pager looped back to %s", url)
                    break
                if needs_goto:
                    seen_urls.add(url)

                logger.info(
                    "discovering UPC index requested_page=%s start_page=%s url=%s navigation=%s",
                    page_number,
                    settings.start_page,
                    url,
                    "goto" if needs_goto else "clicked-page",
                )
                if needs_goto:
                    await page.goto(url, wait_until="domcontentloaded", timeout=settings.navigation_timeout_ms)
                    await accept_cookies(page)
                    await settle_page(page, settings)
                html, page_items, submitted_form = await parse_or_submit_index_page(page, settings, debug_dir, page_number)
                actual_url = page.url
                actual_page_number = parse_page_number(actual_url)
                signature = item_signature(page_items)
                if page_number == 0 and signature:
                    page_signatures[0] = signature
                logger.info(
                    "UPC index requested_page=%s actual_url=%s parsed_actual_page=%s item_count=%s",
                    page_number,
                    actual_url,
                    actual_page_number,
                    len(page_items),
                )

                if submitted_form and page_number > 0 and actual_page_number < page_number:
                    message = (
                        f"UPC index requested page {page_number} submitted the filter form but reset to "
                        f"actual page {actual_page_number}; refusing to treat reset rows as page {page_number}"
                    )
                    await save_debug(
                        debug_dir,
                        f"index-page-{page_number}-form-reset",
                        html,
                        page,
                        message,
                    )
                    logger.warning("%s", message)
                    if page_number < settings.start_page:
                        raise ScraperError(f"could not reach start page {settings.start_page}: {message}")
                    break

                if page_number == 0 and not page_items:
                    await save_debug(
                        debug_dir,
                        f"index-page-{page_number}-no-results-{debug_name(source_url)}",
                        html,
                        page,
                        "No decision rows were found in the first index page even after any available form submit.",
                    )
                    raise ScraperError("no decision rows found on the first UPC index page")

                if page_number > 0 and page_signatures.get(0) and signature == page_signatures[0]:
                    message = (
                        f"UPC index requested page {page_number} yielded the same item signature as page 0; "
                        "treating it as a reset/duplicate page"
                    )
                    await save_debug(
                        debug_dir,
                        f"index-page-{page_number}-duplicate-page-0",
                        html,
                        page,
                        message,
                    )
                    logger.warning("%s", message)
                    if page_number < settings.start_page:
                        raise ScraperError(f"could not reach start page {settings.start_page}: {message}")
                    break

                if page_number > 0 and not page_items:
                    await save_debug(
                        debug_dir,
                        f"index-page-{page_number}-empty",
                        html,
                        page,
                        "A later pagination page yielded no rows; keeping already discovered rows.",
                    )
                    logger.warning(
                        "index page %s yielded 0 items after %s item(s) had already been discovered; stopping pagination",
                        page_number,
                        len(items),
                    )
                    break

                collecting = page_number >= settings.start_page
                if collecting:
                    cumulative = len(items) + len(page_items)
                    logger.info(
                        "index page %s yielded %s collected items (cumulative discovered before dedupe: %s)",
                        page_number,
                        len(page_items),
                        cumulative,
                    )
                    items.extend(page_items)
                    pages_collected += 1
                    logger.info(
                        "discovery progress: requested_page=%s pages_collected=%s cumulative_item_count=%s",
                        page_number,
                        pages_collected,
                        len(items),
                    )
                    if settings.max_items and len(items) >= settings.max_items:
                        logger.info("stopping discovery at MAX_ITEMS=%s", settings.max_items)
                        break
                else:
                    logger.info(
                        "walk-to-start skipping requested_page=%s with %s item(s); collection starts at page %s",
                        page_number,
                        len(page_items),
                        settings.start_page,
                    )

                if settings.max_pages and pages_collected >= settings.max_pages:
                    logger.info("stopping discovery at MAX_PAGES=%s", settings.max_pages)
                    break

                raw_next_url = extract_next_page_url(html, actual_url)
                next_url = select_next_index_url(source_url, page_number, actual_url, raw_next_url)
                if not next_url:
                    logger.info("no next pager link found after index page %s", page_number)
                    break
                next_page_number = parse_page_number(next_url)
                logger.info(
                    "UPC index next page selected: requested_page=%s parsed_next_page=%s next_url=%s",
                    page_number,
                    next_page_number,
                    next_url,
                )

                raw_next_absolute = urljoin(actual_url or source_url, raw_next_url)
                should_click = raw_next_absolute == next_url
                previous_url = actual_url
                clicked = await click_next_pager(page, settings) if should_click or page_number < settings.start_page else False
                clicked_page_number = parse_page_number(page.url) if clicked else -1
                if clicked:
                    page_number += 1
                    url = page.url
                    needs_goto = False
                    if url == previous_url or clicked_page_number <= page_number - 1:
                        # Some Drupal behaviours keep the browser URL stable; in
                        # that case retain a deterministic loop key while still
                        # parsing the clicked page next instead of reloading it.
                        url = next_url
                    continue
                if page_number < settings.start_page:
                    raise ScraperError(f"could not reach start page {settings.start_page}: next pager could not be clicked")

                logger.info("using selected next pager URL: %s", next_url)
                url = next_url
                page_number = next_page_number if next_page_number > page_number else page_number + 1
                needs_goto = True

            return cap_items(dedupe_items(items), settings)
        except Exception as exc:
            last_error = exc
            if items:
                logger.warning(
                    "discovery failed for %s after %s item(s); using partial results: %s",
                    source_url,
                    len(items),
                    exc,
                )
                return cap_items(dedupe_items(items), settings)
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
        "source_index_snapshot": json.dumps(item.source_index_snapshot, ensure_ascii=False, sort_keys=True),
    }
    decision_id = db.upsert_decision(decision_values)
    if documents:
        db.replace_documents(decision_id, documents)
    db.mark_seen(item, now, bootstrapped=False)
    if documents:
        logger.info("ingested %s with %s mirrored PDF(s)", item.item_key, len(documents))
    else:
        logger.warning("ingested %s metadata without mirrored PDFs", item.item_key)


def upsert_index_only_item(db: Database, item: IndexItem, now: str) -> None:
    party_data = parse_parties(item.parties_raw)
    db.upsert_index_only_decision(
        item,
        {
            "parties_json": party_data.parties_json,
            "party_names_all": party_data.party_names_all,
            "party_names_normalised": party_data.party_names_normalised,
            "primary_adverse_caption": party_data.primary_adverse_caption,
            "adverse_pair_key": party_data.adverse_pair_key,
        },
        now,
    )
    db.mark_seen(item, now, bootstrapped=False)


async def ingest_discovered_items(
    context,
    db: Database,
    settings: Settings,
    items: list[IndexItem],
    debug_run_dir: Path,
    index_only: bool = False,
) -> tuple[int, int, list[str]]:
    new_count = 0
    skipped_complete_count = 0
    item_errors: list[str] = []
    for item in items:
        now = utc_now()
        decision = db.get_decision(item.item_key)
        if not needs_enrichment(decision):
            db.mark_seen(item, now)
            db.touch_seen(item, now)
            skipped_complete_count += 1
            continue
        try:
            if index_only:
                upsert_index_only_item(db, item, now)
                logger.info("index-only upserted %s", item.item_key)
            else:
                await ingest_item(context, db, settings, item, debug_run_dir)
            new_count += 1
        except Exception as exc:
            logger.exception("failed to ingest %s", item.item_key)
            item_errors.append(f"{item.item_key}: {exc}")
            await save_debug(
                debug_run_dir,
                f"{item.item_key}-error",
                note=json.dumps({"item": item.__dict__, "error": str(exc)}, ensure_ascii=False, indent=2),
            )
    return new_count, skipped_complete_count, item_errors


async def run_ingestion(
    settings: Settings,
    bootstrap: bool = False,
    index_only: bool = False,
) -> dict[str, int | str]:
    settings.ensure_dirs()
    db = Database(settings.db_path)
    db.init()
    render_outputs(db, settings)

    this_run_id = run_id()
    debug_run_dir = settings.debug_dir / this_run_id
    debug_run_dir.mkdir(parents=True, exist_ok=True)
    started_at = utc_now()
    run_db_id = db.start_run(started_at, str(debug_run_dir))
    discovered_count = 0
    new_count = 0
    skipped_complete_count = 0
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
                    db.finish_run(run_db_id, utc_now(), "success", discovered_count, 0)
                    render_outputs(db, settings)
                    return {"status": "success", "discovered_count": discovered_count, "new_count": 0}

                new_count, skipped_complete_count, item_errors = await ingest_discovered_items(
                    context,
                    db,
                    settings,
                    items,
                    debug_run_dir,
                    index_only=index_only,
                )
            finally:
                await context.close()
                await browser.close()

        render_outputs(db, settings)
        if index_only:
            logger.info("index-only inserted/updated %s item(s)", new_count)
        if skipped_complete_count:
            logger.info("skipped %s complete item(s)", skipped_complete_count)
        status = "success" if not item_errors else "partial_success"
        db.finish_run(
            run_db_id,
            utc_now(),
            status,
            discovered_count,
            new_count,
            "\n".join(item_errors),
        )
        return {
            "status": status,
            "discovered_count": discovered_count,
            "new_count": new_count,
            "skipped_complete_count": skipped_complete_count,
        }
    except Exception as exc:
        logger.exception("ingestion run failed")
        db.finish_run(run_db_id, utc_now(), "failed", discovered_count, new_count, str(exc))
        render_outputs(db, settings)
        raise


def run_ingestion_sync(settings: Settings, bootstrap: bool = False, index_only: bool = False) -> dict[str, int | str]:
    return asyncio.run(run_ingestion(settings, bootstrap=bootstrap, index_only=index_only))
