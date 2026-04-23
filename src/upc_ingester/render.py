from __future__ import annotations

import json
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from jinja2 import Environment, FileSystemLoader, select_autoescape

from .db import Database


def utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def preview(value: str, limit: int = 220) -> str:
    value = " ".join((value or "").split())
    if len(value) <= limit:
        return value
    return value[: limit - 1].rstrip() + "..."


def atomic_write_text(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile("w", encoding="utf-8", delete=False, dir=path.parent) as handle:
        handle.write(content)
        tmp_name = handle.name
    Path(tmp_name).replace(path)


def latest_payload(decisions: list[dict[str, Any]], generated_at: str) -> dict[str, Any]:
    return {
        "generated_at": generated_at,
        "count": len(decisions),
        "items": decisions,
    }


def render_outputs(db: Database, public_dir: Path) -> None:
    decisions = db.get_decisions()
    generated_at = utc_now()

    json_content = json.dumps(
        latest_payload(decisions, generated_at),
        ensure_ascii=False,
        indent=2,
        sort_keys=True,
    )
    atomic_write_text(public_dir / "latest.json", json_content + "\n")

    template_dir = Path(__file__).parent / "templates"
    env = Environment(
        loader=FileSystemLoader(template_dir),
        autoescape=select_autoescape(("html", "xml")),
    )
    env.filters["preview"] = preview
    template = env.get_template("index.html.j2")
    html = template.render(decisions=decisions, generated_at=generated_at)
    atomic_write_text(public_dir / "index.html", html)
