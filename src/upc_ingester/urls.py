from __future__ import annotations

import re
from urllib.parse import quote

from .config import DEFAULT_PUBLIC_BASE_URL


def absolute_public_url(value: str | None, public_base_url: str = DEFAULT_PUBLIC_BASE_URL) -> str:
    text = str(value or "").strip()
    if not text or re.match(r"^[a-z][a-z0-9+.-]*://", text, flags=re.IGNORECASE):
        return text
    if text.startswith("/"):
        return f"{str(public_base_url or DEFAULT_PUBLIC_BASE_URL).rstrip('/')}/{text.lstrip('/')}"
    return text


def public_item_path(item_key: str, suffix: str) -> str:
    safe_key = quote(str(item_key or ""), safe="")
    return f"/items/{safe_key}.{suffix}"


def public_related_path(item_key: str) -> str:
    safe_key = quote(str(item_key or ""), safe="")
    return f"/related/{safe_key}.json"


def public_item_json_url(item_key: str, public_base_url: str = DEFAULT_PUBLIC_BASE_URL) -> str:
    return absolute_public_url(public_item_path(item_key, "json"), public_base_url)


def public_item_html_url(item_key: str, public_base_url: str = DEFAULT_PUBLIC_BASE_URL) -> str:
    return absolute_public_url(public_item_path(item_key, "html"), public_base_url)


def public_related_json_url(item_key: str, public_base_url: str = DEFAULT_PUBLIC_BASE_URL) -> str:
    return absolute_public_url(public_related_path(item_key), public_base_url)
