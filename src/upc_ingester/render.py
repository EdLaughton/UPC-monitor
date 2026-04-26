from __future__ import annotations

import json
import tempfile
from html import escape
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from jinja2 import Environment, FileSystemLoader, select_autoescape

from .config import Settings
from .db import Database
from .stats import build_stats
from .urls import absolute_public_url, public_item_html_url, public_item_json_url, public_item_path, public_related_json_url, public_related_path


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


def latest_payload(decisions: list[dict[str, Any]], generated_at: str, total_count: int) -> dict[str, Any]:
    return {
        "generated_at": generated_at,
        "count": len(decisions),
        "total_database_count": total_count,
        "items": decisions,
    }


def status_payload(
    db: Database,
    generated_at: str,
    latest_count: int,
    total_count: int,
    settings: Settings,
) -> dict[str, Any]:
    latest_run = db.get_latest_run()
    counts = db.get_status_counts()
    last_success = None
    if latest_run and latest_run.get("status") in {"success", "partial_success"}:
        last_success = latest_run.get("finished_at")

    return {
        "generated_at": generated_at,
        "database_path": str(db.path),
        "decision_count_in_latest_json": latest_count,
        "decision_count_total": total_count,
        "latest_export_limit": settings.latest_export_limit,
        "write_all_json": settings.write_all_json,
        "exports": {
            "latest_json": "/latest.json",
            "status_json": "/status.json",
            "stats_json": "/stats.json",
            "all_ndjson": "/all.ndjson",
            "all_json": "/all.json" if settings.write_all_json else None,
            "item_context_json": "/items/{item_key}.json",
            "related_context_json": "/related/{item_key}.json",
        },
        "counts": counts,
        "latest_run": latest_run,
        "last_attempt_at": latest_run.get("started_at") if latest_run else None,
        "last_success_at": last_success,
        "last_status": latest_run.get("status") if latest_run else "never_run",
        "last_error": latest_run.get("failure_summary") if latest_run else "",
    }


def write_ndjson(path: Path, rows: list[dict[str, Any]]) -> None:
    content = "".join(json.dumps(row, ensure_ascii=False, sort_keys=True) + "\n" for row in rows)
    atomic_write_text(path, content)


PUBLIC_ITEM_FIELDS = [
    "item_key",
    "node_url",
    "decision_date",
    "first_seen_at",
    "case_number",
    "registry_number",
    "division",
    "document_type",
    "type_of_action",
    "language",
    "title_raw",
    "parties_raw",
    "party_names_all",
    "party_names_normalised",
    "primary_adverse_caption",
    "adverse_pair_key",
    "headnote_text",
    "keywords_raw",
    "keywords_list",
]


RELATED_ITEM_FIELDS = [
    "item_key",
    "decision_date",
    "case_number",
    "registry_number",
    "division",
    "document_type",
    "type_of_action",
    "language",
    "title_raw",
    "parties_raw",
    "primary_adverse_caption",
    "adverse_pair_key",
]


def public_item_payload(decision: dict[str, Any], public_base_url: str) -> dict[str, Any]:
    item = {field: decision.get(field, [] if field in {"party_names_all", "party_names_normalised", "keywords_list"} else "") for field in PUBLIC_ITEM_FIELDS}
    item["mirror_pdf_url"] = absolute_public_url(str(decision.get("pdf_url_mirror", "")), public_base_url)
    item["official_pdf_url"] = decision.get("pdf_url_official", "")
    item["local_item_html_url"] = public_item_html_url(str(decision.get("item_key", "")), public_base_url)
    item["item_context_url"] = public_item_json_url(str(decision.get("item_key", "")), public_base_url)
    item["related_context_url"] = public_related_json_url(str(decision.get("item_key", "")), public_base_url)
    return item


def related_item_payload(decision: dict[str, Any], public_base_url: str) -> dict[str, Any]:
    item = {field: decision.get(field, [] if field in {"party_names_all", "party_names_normalised", "keywords_list"} else "") for field in RELATED_ITEM_FIELDS}
    item["mirror_pdf_url"] = absolute_public_url(str(decision.get("pdf_url_mirror", "")), public_base_url)
    item["official_pdf_url"] = decision.get("pdf_url_official", "")
    item["item_context_url"] = public_item_json_url(str(decision.get("item_key", "")), public_base_url)
    item["related_context_url"] = public_related_json_url(str(decision.get("item_key", "")), public_base_url)
    return item


def _related_items(
    current: dict[str, Any],
    all_decisions: list[dict[str, Any]],
    predicate,
    public_base_url: str,
    limit: int = 20,
) -> list[dict[str, Any]]:
    current_key = current.get("item_key", "")
    rows = [row for row in all_decisions if row.get("item_key") != current_key and predicate(row)]
    rows.sort(key=lambda row: (str(row.get("decision_date") or ""), str(row.get("first_seen_at") or ""), str(row.get("item_key") or "")), reverse=True)
    return [related_item_payload(row, public_base_url) for row in rows[:limit]]


def related_context_payload(current: dict[str, Any], all_decisions: list[dict[str, Any]], public_base_url: str) -> dict[str, Any]:
    case_number = str(current.get("case_number") or "")
    registry_number = str(current.get("registry_number") or "")
    adverse_pair_key = str(current.get("adverse_pair_key") or "")
    party_names = {str(name) for name in current.get("party_names_normalised", []) if str(name)}
    return {
        "current_item": public_item_payload(current, public_base_url),
        "same_case_or_registry": _related_items(
            current,
            all_decisions,
            lambda row: bool((case_number and row.get("case_number") == case_number) or (registry_number and row.get("registry_number") == registry_number)),
            public_base_url,
        ),
        "same_adverse_party_pair": _related_items(
            current,
            all_decisions,
            lambda row: bool(adverse_pair_key and row.get("adverse_pair_key") == adverse_pair_key),
            public_base_url,
        ),
        "same_normalised_parties": _related_items(
            current,
            all_decisions,
            lambda row: bool(party_names.intersection({str(name) for name in row.get("party_names_normalised", []) if str(name)})),
            public_base_url,
        ),
    }


def item_html(decision: dict[str, Any], public_base_url: str) -> str:
    title = str(decision.get("title_raw") or decision.get("item_key") or "UPC item")
    item_key = str(decision.get("item_key") or "")
    item_json = public_item_json_url(item_key, public_base_url)
    related_json = public_related_json_url(item_key, public_base_url)
    mirror = absolute_public_url(str(decision.get("pdf_url_mirror", "")), public_base_url)
    return f"""<!doctype html>
<html lang="en">
<head><meta charset="utf-8"><title>{escape(title)}</title></head>
<body>
<main>
<h1>{escape(title)}</h1>
<p><strong>Item key:</strong> {escape(item_key)}</p>
<p><strong>Decision date:</strong> {escape(str(decision.get("decision_date") or ""))}</p>
<p><strong>Parties:</strong> {escape(str(decision.get("parties_raw") or ""))}</p>
<p><a href="{escape(item_json)}">Item JSON</a></p>
<p><a href="{escape(related_json)}">Related context JSON</a></p>
{f'<p><a href="{escape(mirror)}">Mirrored PDF</a></p>' if mirror else ''}
</main>
</body>
</html>
"""


def write_agent_context_files(public_dir: Path, decisions: list[dict[str, Any]], public_base_url: str) -> None:
    for decision in decisions:
        item_key = str(decision.get("item_key") or "")
        if not item_key:
            continue
        item_content = json.dumps(public_item_payload(decision, public_base_url), ensure_ascii=False, indent=2, sort_keys=True)
        atomic_write_text(public_dir / public_item_path(item_key, "json").lstrip("/"), item_content + "\n")
        related_content = json.dumps(related_context_payload(decision, decisions, public_base_url), ensure_ascii=False, indent=2, sort_keys=True)
        atomic_write_text(public_dir / public_related_path(item_key).lstrip("/"), related_content + "\n")
        atomic_write_text(public_dir / public_item_path(item_key, "html").lstrip("/"), item_html(decision, public_base_url))


def render_outputs(db: Database, settings_or_public_dir: Settings | Path) -> None:
    if isinstance(settings_or_public_dir, Settings):
        settings = settings_or_public_dir
        public_dir = settings.public_dir
        latest_limit = settings.latest_export_limit
        write_all_json = settings.write_all_json
        public_base_url = settings.public_base_url
    else:
        # Backwards-compatible path for older call sites/tests.
        settings = Settings.from_env()
        public_dir = settings_or_public_dir
        latest_limit = settings.latest_export_limit
        write_all_json = settings.write_all_json
        public_base_url = settings.public_base_url

    all_decisions = db.get_decisions()
    latest_decisions = db.get_decisions(limit=latest_limit)
    generated_at = utc_now()

    json_content = json.dumps(
        latest_payload(latest_decisions, generated_at, len(all_decisions)),
        ensure_ascii=False,
        indent=2,
        sort_keys=True,
    )
    atomic_write_text(public_dir / "latest.json", json_content + "\n")

    stats = build_stats(all_decisions, generated_at=generated_at)
    stats_content = json.dumps(stats, ensure_ascii=False, indent=2, sort_keys=True)
    atomic_write_text(public_dir / "stats.json", stats_content + "\n")

    status_content = json.dumps(
        status_payload(db, generated_at, len(latest_decisions), len(all_decisions), settings),
        ensure_ascii=False,
        indent=2,
        sort_keys=True,
    )
    atomic_write_text(public_dir / "status.json", status_content + "\n")

    write_ndjson(public_dir / "all.ndjson", all_decisions)
    write_agent_context_files(public_dir, all_decisions, public_base_url)
    if write_all_json:
        all_json_content = json.dumps(
            {"generated_at": generated_at, "count": len(all_decisions), "items": all_decisions},
            ensure_ascii=False,
            indent=2,
            sort_keys=True,
        )
        atomic_write_text(public_dir / "all.json", all_json_content + "\n")

    template_dir = Path(__file__).parent / "templates"
    env = Environment(
        loader=FileSystemLoader(template_dir),
        autoescape=select_autoescape(("html", "xml")),
    )
    env.filters["preview"] = preview
    template = env.get_template("index.html.j2")
    html = template.render(decisions=latest_decisions, generated_at=generated_at)
    atomic_write_text(public_dir / "index.html", html)

    stats_template = env.get_template("stats.html.j2")
    stats_html = stats_template.render(stats=stats, generated_at=generated_at)
    atomic_write_text(public_dir / "stats.html", stats_html)
