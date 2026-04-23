from __future__ import annotations

import hashlib
import re
from pathlib import Path
from urllib.parse import unquote, urlparse

from .parser import PdfLink, clean_text


class PdfDownloadError(RuntimeError):
    pass


def sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def sanitize_filename(value: str) -> str:
    value = unquote(value)
    value = re.sub(r"[^A-Za-z0-9._-]+", "-", value).strip("-._")
    return value[:120] or "document"


def node_folder(node_url: str) -> str:
    match = re.search(r"/node/(\d+)", node_url)
    if match:
        return f"node-{match.group(1)}"
    digest = hashlib.sha256(node_url.encode("utf-8")).hexdigest()[:12]
    return f"node-{digest}"


def stable_pdf_paths(
    public_dir: Path,
    decision_date: str,
    node_url: str,
    order_ref: str,
    pdf_url: str,
    language: str,
) -> tuple[Path, str]:
    year = decision_date[:4] if re.match(r"^\d{4}", decision_date or "") else "unknown"
    url_hash = hashlib.sha256(pdf_url.encode("utf-8")).hexdigest()[:10]
    suffix = f"_{language.lower()}" if language else ""
    parsed_name = Path(urlparse(pdf_url).path).name
    stem = sanitize_filename(order_ref or Path(parsed_name).stem or "upc-document")
    filename = f"{stem}{suffix}_{url_hash}.pdf"
    relative = Path("pdfs") / year / node_folder(node_url) / filename
    return public_dir / relative, "/" + relative.as_posix()


def validate_pdf_bytes(data: bytes, url: str) -> None:
    if not data.lstrip().startswith(b"%PDF"):
        raise PdfDownloadError(f"downloaded content from {url} is not a PDF")


async def download_pdf(
    context,
    link: PdfLink,
    public_dir: Path,
    decision_date: str,
    node_url: str,
    order_ref: str,
    downloaded_at: str,
    is_primary: bool,
) -> dict[str, str | bool]:
    file_path, mirror_url = stable_pdf_paths(
        public_dir,
        decision_date,
        node_url,
        order_ref,
        link.url,
        link.language,
    )
    file_path.parent.mkdir(parents=True, exist_ok=True)

    if file_path.exists():
        data = file_path.read_bytes()
        validate_pdf_bytes(data, str(file_path))
    else:
        response = await context.request.get(link.url, timeout=60000)
        if not response.ok:
            raise PdfDownloadError(f"failed to download {link.url}: HTTP {response.status}")
        data = await response.body()
        validate_pdf_bytes(data, link.url)
        tmp_path = file_path.with_suffix(".pdf.tmp")
        tmp_path.write_bytes(data)
        tmp_path.replace(file_path)

    return {
        "language": link.language,
        "pdf_url_official": link.url,
        "pdf_url_mirror": mirror_url,
        "pdf_sha256": sha256_bytes(data),
        "file_path": str(file_path),
        "is_primary": is_primary,
        "downloaded_at": downloaded_at,
    }


def _extract_between(text: str, start_pattern: str, stop_patterns: tuple[str, ...]) -> str:
    start = re.search(start_pattern, text, flags=re.I)
    if not start:
        return ""
    rest = text[start.end() :]
    stop_positions = [
        match.start()
        for pattern in stop_patterns
        if (match := re.search(pattern, rest, flags=re.I))
    ]
    if stop_positions:
        rest = rest[: min(stop_positions)]
    return clean_text(rest)


def extract_pdf_sections(path: Path) -> tuple[str, str]:
    try:
        from pypdf import PdfReader
    except Exception:
        return "", ""

    try:
        reader = PdfReader(str(path))
        text = "\n".join(page.extract_text() or "" for page in reader.pages)
    except Exception:
        return "", ""

    headnotes = _extract_between(text, r"\bHEADNOTES?\s*:\s*", (r"\bKEYWORDS?\s*:\s*", r"\bORDER\b", r"\bDECISION\b"))
    keywords = _extract_between(text, r"\bKEYWORDS?\s*:\s*", (r"\bAPPLICANT\b", r"\bCLAIMANT\b", r"\bDEFENDANT\b", r"\bORDER\b", r"\bDECISION\b"))
    return headnotes, keywords
