from __future__ import annotations

import hashlib
import re
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any
from urllib.parse import urljoin, urlparse

from bs4 import BeautifulSoup, Tag


DATE_FORMATS = (
    "%d %B, %Y",
    "%d %B %Y",
    "%d %b, %Y",
    "%d %b %Y",
)

DETAIL_LABELS = (
    "Case number",
    "Registry number",
    "Date",
    "Parties",
    "Order/Decision reference",
    "Type of action",
    "Language of Proceedings",
    "Court - Division",
    "Panel",
)


@dataclass(frozen=True)
class IndexItem:
    item_key: str
    node_url: str
    decision_date: str = ""
    registry_number: str = ""
    order_or_decision_number: str = ""
    case_number: str = ""
    division: str = ""
    type_of_action: str = ""
    parties_raw: str = ""
    title_raw: str = ""
    source_index_snapshot: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class PdfLink:
    url: str
    label: str = ""
    language: str = ""


@dataclass
class DetailMetadata:
    title_raw: str = ""
    case_name_raw: str = ""
    parties_raw: str = ""
    division: str = ""
    panel: str = ""
    case_number: str = ""
    registry_number: str = ""
    order_or_decision_number: str = ""
    decision_date: str = ""
    document_type: str = ""
    type_of_action: str = ""
    language: str = ""
    headnote_raw: str = ""
    headnote_text: str = ""
    keywords_raw: str = ""
    keywords_list: list[str] = field(default_factory=list)
    pdf_links: list[PdfLink] = field(default_factory=list)


class ParserError(RuntimeError):
    pass


def clean_text(value: str | None) -> str:
    if not value:
        return ""
    value = value.replace("\xa0", " ")
    return re.sub(r"[ \t\r\f\v]+", " ", value).strip()


def clean_multiline(value: str | None) -> str:
    if not value:
        return ""
    lines = [clean_text(line) for line in value.replace("\xa0", " ").splitlines()]
    return "\n".join(line for line in lines if line)


def normalise_heading(value: str) -> str:
    return clean_text(value).rstrip(":").lower()


def parse_date(value: str) -> str:
    value = clean_text(value).replace(" ,", ",")
    if not value:
        return ""
    for fmt in DATE_FORMATS:
        try:
            return datetime.strptime(value, fmt).date().isoformat()
        except ValueError:
            pass
    return value


def is_failure_page(html: str) -> bool:
    lower = html.lower()
    return any(
        marker in lower
        for marker in (
            "just a moment",
            "cf-chl",
            "cf_chl",
            "cf-mitigated",
            "checking if the site connection is secure",
            "the api is currently unavailable",
        )
    )


def visible_lines(html: str) -> list[str]:
    soup = BeautifulSoup(html, "html.parser")
    for tag in soup(["script", "style", "noscript", "svg"]):
        tag.decompose()
    text = soup.get_text("\n")
    return [clean_text(line) for line in text.splitlines() if clean_text(line)]


def _find_results_table(soup: BeautifulSoup) -> tuple[Tag, list[str]] | None:
    for table in soup.find_all("table"):
        header_row = table.find("tr")
        if not header_row:
            continue
        headers = [clean_text(th.get_text(" ", strip=True)) for th in header_row.find_all("th")]
        header_blob = " | ".join(headers).lower()
        if "date" in header_blob and "registry" in header_blob and "parties" in header_blob:
            return table, headers
    return None


def _cell_text(cell: Tag | None) -> str:
    if not cell:
        return ""
    return clean_multiline(cell.get_text("\n", strip=True))


def _value_for(values: dict[str, str], *needles: str) -> str:
    normalised = {normalise_heading(key): value for key, value in values.items()}
    for needle in needles:
        wanted = normalise_heading(needle)
        if wanted in normalised:
            return normalised[wanted]
    for key, value in normalised.items():
        if all(part in key for part in needles):
            return value
    return ""


def _extract_refs(text: str) -> tuple[str, str, str]:
    # UPC data is not fully consistent: recent rows may use underscores
    # (UPC_CFI_472/2024), hyphens (UPC-CFI-461/2025), or padded CoA
    # numbers (UPC-COA-0000901/2025). Keep the original display value.
    refs = re.findall(
        r"\b(?:"
        r"UPC[-_](?:CFI|CoA|COA)[-_]?[0-9]+/[0-9]{4}|"
        r"[A-Z][A-Za-z]*_[0-9]+/[0-9]{4}"
        r")\b",
        text,
        flags=re.I,
    )
    case_number = next((ref for ref in refs if re.match(r"^UPC[-_]", ref, re.I)), "")
    order_ref = next((ref for ref in refs if ref.startswith(("ORD_", "DEC_"))), "")
    registry = next((ref for ref in refs if not ref.startswith(("ORD_", "DEC_")) and not re.match(r"^UPC[-_](?:CFI|CoA|COA)", ref, re.I)), "")
    if not registry and case_number:
        registry = case_number
    elif not registry and refs:
        registry = refs[0]
    return registry, order_ref, case_number


def _make_item_key(node_url: str, order_ref: str, registry: str, row_text: str) -> str:
    if node_url:
        match = re.search(r"/node/(\d+)", node_url)
        if match:
            return f"node-{match.group(1)}"
        return node_url
    if order_ref:
        return f"order-{order_ref}"
    if registry:
        return f"registry-{registry}"
    return "row-" + hashlib.sha256(row_text.encode("utf-8")).hexdigest()[:24]


def parse_index_page(html: str, base_url: str) -> list[IndexItem]:
    if is_failure_page(html):
        raise ParserError("UPC index page appears to be a challenge or API-unavailable page")

    soup = BeautifulSoup(html, "html.parser")
    found = _find_results_table(soup)
    if not found:
        return []

    table, headers = found
    rows = table.select("tbody tr") or table.find_all("tr")[1:]
    items: list[IndexItem] = []

    for row in rows:
        cells = row.find_all(["td", "th"], recursive=False)
        if len(cells) < 3:
            continue
        values = {headers[index]: _cell_text(cell) for index, cell in enumerate(cells) if index < len(headers)}
        row_text = clean_multiline(row.get_text("\n", strip=True))
        link = row.find("a", href=re.compile(r"/(?:en/)?node/|/node/"))
        if link is None:
            link = row.find("a", string=re.compile(r"Full Details", re.I))
        node_url = urljoin(base_url, link["href"]) if link and link.get("href") else ""

        registry_cell = _value_for(values, "registry")
        registry, order_ref, case_number = _extract_refs(registry_cell)

        item = IndexItem(
            item_key=_make_item_key(node_url, order_ref, registry, row_text),
            node_url=node_url,
            decision_date=parse_date(_value_for(values, "date")),
            registry_number=registry,
            order_or_decision_number=order_ref,
            case_number=case_number,
            division=_value_for(values, "court"),
            type_of_action=_value_for(values, "type", "action"),
            parties_raw=_value_for(values, "parties"),
            title_raw=_value_for(values, "upc", "document") or _value_for(values, "type"),
            source_index_snapshot=values,
        )
        if item.node_url or item.order_or_decision_number or item.registry_number:
            items.append(item)

    return items


def extract_last_page(html: str) -> int:
    soup = BeautifulSoup(html, "html.parser")
    pages = []
    for link in soup.find_all("a", href=True):
        match = re.search(r"[?&]page=(\d+)", link["href"])
        if match:
            pages.append(int(match.group(1)))
    return max(pages) if pages else 0


def extract_next_page_url(html: str, base_url: str) -> str:
    """Return the actual UPC pager next URL, preserving Drupal query args.

    The UPC view includes required default query parameters in its pager hrefs.
    Building ?page=N by hand can yield empty pages, so discovery should follow
    the rel=next / pager__item--next link emitted by the site.
    """
    soup = BeautifulSoup(html, "html.parser")
    selectors = (
        "li.pager__item--next a[href]",
        "a[rel='next'][href]",
        "a[title='Go to next page'][href]",
    )
    for selector in selectors:
        link = soup.select_one(selector)
        if link and link.get("href"):
            return urljoin(base_url, link["href"])
    return ""


def _extract_labeled_values(lines: list[str]) -> dict[str, str]:
    labels = {normalise_heading(label): label for label in DETAIL_LABELS}
    values: dict[str, str] = {}
    i = 0
    section_stops = {"headnotes", "keywords", "order documents", "decision documents", "back to decisions and orders"}
    while i < len(lines):
        key = labels.get(normalise_heading(lines[i]))
        if not key:
            i += 1
            continue
        collected: list[str] = []
        i += 1
        while i < len(lines):
            heading = normalise_heading(lines[i])
            if heading in labels or heading in section_stops:
                break
            collected.append(lines[i])
            i += 1
        values[key] = "\n".join(collected).strip()
    return values


def _extract_section(lines: list[str], heading: str, stops: set[str]) -> str:
    start = None
    wanted = normalise_heading(heading)
    for index, line in enumerate(lines):
        if normalise_heading(line) == wanted:
            start = index + 1
            break
    if start is None:
        return ""
    collected: list[str] = []
    for line in lines[start:]:
        if normalise_heading(line) in stops:
            break
        collected.append(line)
    return "\n".join(collected).strip()


def _extract_tile_section_text(soup: BeautifulSoup, heading: str) -> str:
    """Extract text from the body of a UPC tile headed by `heading`.

    This is more reliable than full-page line slicing for long headnotes because
    it avoids footer/navigation text and tolerates empty sections.
    """
    wanted = normalise_heading(heading)
    for h4 in soup.find_all(["h2", "h3", "h4", "h5"]):
        if normalise_heading(h4.get_text(" ", strip=True)) != wanted:
            continue
        tile = h4.find_parent(["section", "footer"])
        if not tile:
            continue
        body = tile.select_one(".tile__body")
        if not body:
            continue
        return clean_multiline(body.get_text("\n", strip=True))
    return ""


def _keywords_list(raw: str) -> list[str]:
    text = clean_text(raw.replace("\n", " "))
    # UPC keywords often use semicolons, en dashes or bullet-like separators.
    parts = re.split(r"\s*;\s*|\s+–\s+|\s+-\s+", text)
    return [part.strip() for part in parts if part.strip()]


def _pdf_language(label: str, url: str) -> str:
    match = re.search(r"\b([A-Z]{2})\s*pdf\b", label, flags=re.I)
    if match:
        return match.group(1).upper()
    match = re.search(r"\b([A-Z]{2})\b", label)
    if match:
        return match.group(1)
    match = re.search(r"_([a-z]{2})\.pdf(?:$|[?#])", url, re.I)
    if match:
        return match.group(1).upper()
    return ""


def is_official_upc_pdf_url(url: str) -> bool:
    parsed = urlparse(url)
    host = (parsed.netloc or "").lower()
    path = parsed.path.lower()
    if host not in {
        "unifiedpatentcourt.org",
        "www.unifiedpatentcourt.org",
        "unified-patent-court.org",
        "www.unified-patent-court.org",
    }:
        return False
    if not path.endswith(".pdf"):
        return False
    # Exclude generic site/legal-text PDFs and keep only UPC decision/order
    # document storage locations observed on the detail pages.
    return path.startswith("/sites/default/files/") and (
        "/api_order/" in path
        or "/api_decision/" in path
        or "/upc_documents/" in path
    )


def _document_sections(soup: BeautifulSoup) -> list[Tag]:
    sections: list[Tag] = []
    for h4 in soup.find_all(["h2", "h3", "h4", "h5"]):
        heading = normalise_heading(h4.get_text(" ", strip=True))
        if heading not in {"order documents", "decision documents"}:
            continue
        section = h4.find_parent(["footer", "section"])
        if section is not None:
            sections.append(section)
    return sections


def _extract_pdf_links(soup: BeautifulSoup, base_url: str) -> list[PdfLink]:
    pdf_links: list[PdfLink] = []
    seen: set[str] = set()

    # First pass: only the explicit Order Documents / Decision Documents tiles.
    candidate_roots = _document_sections(soup)

    # Fallback: some markup variants may not expose the heading wrapper as
    # expected. If so, scan the judgment article, still filtering to official
    # UPC decision/order PDF storage paths.
    if not candidate_roots:
        article = soup.select_one("article.node--type-judgement") or soup.find("main") or soup
        candidate_roots = [article]

    for root in candidate_roots:
        for link in root.find_all("a", href=True):
            href = link["href"]
            if ".pdf" not in href.lower():
                continue
            absolute = urljoin(base_url, href)
            if not is_official_upc_pdf_url(absolute):
                continue
            if absolute in seen:
                continue
            seen.add(absolute)
            label = clean_text(link.get_text(" ", strip=True)) or clean_text(link.get("title", ""))
            pdf_links.append(PdfLink(url=absolute, label=label, language=_pdf_language(label, absolute)))
    return pdf_links


def parse_detail_page(html: str, base_url: str) -> DetailMetadata:
    if is_failure_page(html):
        raise ParserError("UPC detail page appears to be a challenge or API-unavailable page")

    soup = BeautifulSoup(html, "html.parser")
    h1 = soup.find("h1")
    title_raw = clean_text(h1.get_text(" ", strip=True)) if h1 else ""
    lines = visible_lines(html)
    values = _extract_labeled_values(lines)

    headnote_raw = _extract_tile_section_text(soup, "Headnotes") or _extract_section(
        lines,
        "Headnotes",
        {"keywords", "order documents", "decision documents", "back to decisions and orders"},
    )
    keywords_raw = _extract_tile_section_text(soup, "Keywords") or _extract_section(
        lines,
        "Keywords",
        {"order documents", "decision documents", "back to decisions and orders"},
    )

    parties_raw = values.get("Parties", "")
    metadata = DetailMetadata(
        title_raw=title_raw,
        case_name_raw=parties_raw,
        parties_raw=parties_raw,
        division=values.get("Court - Division", ""),
        panel=values.get("Panel", ""),
        case_number=values.get("Case number", ""),
        registry_number=values.get("Registry number", ""),
        order_or_decision_number=values.get("Order/Decision reference", ""),
        decision_date=parse_date(values.get("Date", "")),
        document_type=title_raw,
        type_of_action=values.get("Type of action", ""),
        language=values.get("Language of Proceedings", ""),
        headnote_raw=headnote_raw,
        headnote_text=clean_text(headnote_raw.replace("\n", " ")),
        keywords_raw=keywords_raw,
        keywords_list=_keywords_list(keywords_raw),
        pdf_links=_extract_pdf_links(soup, base_url),
    )
    return metadata
