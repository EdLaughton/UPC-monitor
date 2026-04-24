import asyncio
import json
from pathlib import Path

import pytest

from upc_ingester.alerts import (
    MATCH_FIELDS,
    WatchProfile,
    _extract_pdf_text_if_available,
    _find_record_by_field,
    _match_sync_filter,
    _match_create_fields,
    _match_update_fields,
    _quote_airtable_formula_value,
    normalise_document_type,
    normalise_language,
    sync_matches_to_airtable,
    _upc_item_create_fields,
    _upc_item_update_fields,
    build_sync_key,
    estimate_airtable_records,
    match_alerts,
    split_terms,
    write_private_outputs,
)
from upc_ingester.config import Settings
from upc_ingester.scheduler import CronScheduler


def make_settings(tmp_path: Path) -> Settings:
    return Settings(
        data_dir=tmp_path,
        public_dir=tmp_path / "public",
        pdfs_dir=tmp_path / "public" / "pdfs",
        debug_dir=tmp_path / "debug",
        db_path=tmp_path / "upc.sqlite3",
        source_url="https://example.test",
        fallback_source_url="",
        poll_cron="0 * * * *",
        timezone="UTC",
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
        index_page_retry_delay_seconds=1,
        index_page_max_retries=1,
        latest_export_limit=50,
        write_all_json=False,
        alerts_enabled=False,
        alerts_sync_airtable=False,
        alerts_schedule_hour=10,
        alerts_schedule_minute=5,
        alerts_since_days=7,
        alerts_include_low_confidence=False,
        alerts_airtable_max_sync_records=100,
        airtable_base_id="appzaT3sgr7AfBKkn",
    )


def sample_decision() -> dict:
    return {
        "item_key": "node-1",
        "parties_raw": "Acme Corp v. Rival LLC",
        "party_names_all": ["Acme Corp", "Rival LLC"],
        "party_names_normalised": ["acme corp", "rival llc"],
        "case_name_raw": "Acme Corp v. Rival LLC",
        "title_raw": "Order on PI",
        "document_type": "Order",
        "type_of_action": "Infringement action",
        "division": "Local Division Paris",
        "headnote_text": "Urgent injunction for pharma sector",
        "keywords_raw": "injunction; urgency",
        "keywords_list": ["injunction", "urgency"],
        "pdf_url_official": "https://example.test/a.pdf",
        "pdf_url_mirror": "/pdfs/a.pdf",
        "first_seen_at": "2026-04-20T00:00:00+00:00",
        "decision_date": "2026-04-20",
    }


def test_split_terms_parsing() -> None:
    assert split_terms("Acme\nRival; pharma, Rule 212") == ["acme", "pharma", "rival", "rule 212"]


def test_party_match_is_high() -> None:
    profile = WatchProfile("recA", "BD", "BD", ["acme corp"], [], [], [])
    matches = match_alerts([sample_decision()], [profile])
    assert matches[0].confidence == "High"


def test_competitor_match_is_high() -> None:
    profile = WatchProfile("recA", "Comp", "Mixed", [], [], [], ["rival llc"])
    matches = match_alerts([sample_decision()], [profile])
    assert matches[0].confidence == "High"


def test_sector_term_match_is_medium() -> None:
    profile = WatchProfile("recA", "Sector", "Mixed", [], ["pharma"], [], [])
    matches = match_alerts([sample_decision()], [profile])
    assert matches[0].confidence == "Medium"


def test_legal_term_match_is_medium() -> None:
    profile = WatchProfile("recA", "Legal", "Legal interest", [], [], ["injunction"], [])
    matches = match_alerts([sample_decision()], [profile])
    assert matches[0].confidence == "Medium"


def test_absent_terms_no_match() -> None:
    profile = WatchProfile("recA", "None", "Mixed", ["nomatch"], ["never"], ["void"], ["ghost"])
    assert match_alerts([sample_decision()], [profile]) == []


def test_low_confidence_sync_toggle() -> None:
    profile = WatchProfile("recA", "Legal", "Legal", [], [], ["injunction"], [])
    match = match_alerts([sample_decision()], [profile])[0]
    assert not _match_sync_filter(type("M", (), {"confidence": "Low"})(), include_low_confidence=False)
    assert _match_sync_filter(type("M", (), {"confidence": "Low"})(), include_low_confidence=True)
    assert estimate_airtable_records([match], include_low_confidence=False) == 2


def test_sync_key_stable() -> None:
    assert build_sync_key("node-1", "rec123") == "node-1::rec123"


def test_private_json_under_private_only(tmp_path: Path) -> None:
    settings = make_settings(tmp_path)
    profile = WatchProfile("recA", "Legal", "Legal", [], [], ["injunction"], [])
    match = match_alerts([sample_decision()], [profile])[0]
    alerts_path, digest_path = write_private_outputs(settings, [match])
    assert alerts_path == tmp_path / "private" / "alerts.json"
    assert digest_path == tmp_path / "private" / "alerts-digest-source.json"
    assert not (tmp_path / "public" / "alerts.json").exists()


def test_airtable_payload_rules() -> None:
    d = sample_decision()
    create_fields = _upc_item_create_fields(d)
    update_fields = _upc_item_update_fields(d)
    assert any(k.startswith("fld") for k in create_fields)
    assert create_fields != update_fields

    profile = WatchProfile("recA", "Legal", "Legal", [], [], ["injunction"], [])
    match = match_alerts([d], [profile])[0]
    match_create = _match_create_fields(match, "recItem")
    match_update = _match_update_fields(match, "recItem")
    assert MATCH_FIELDS["match_id"] not in match_create
    assert MATCH_FIELDS["reviewer_decision"] not in match_update
    assert MATCH_FIELDS["ai_draft"] not in match_update
    assert MATCH_FIELDS["human_edited_draft"] not in match_update
    assert match_update[MATCH_FIELDS["upc_item"]] == ["recItem"]
    assert match_update[MATCH_FIELDS["watch_profile"]] == ["recA"]


def test_scheduler_defaults_from_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("ALERTS_ENABLED", raising=False)
    monkeypatch.delenv("ALERTS_SYNC_AIRTABLE", raising=False)
    monkeypatch.delenv("ALERTS_SCHEDULE_HOUR", raising=False)
    from upc_ingester.config import Settings as S

    s = S.from_env()
    assert s.alerts_enabled is False
    assert s.alerts_sync_airtable is False
    assert s.alerts_schedule_hour == 10
    assert s.alerts_schedule_minute == 5
    assert s.alerts_since_days == 7


def test_overlapping_alert_guard(tmp_path: Path) -> None:
    scheduler = CronScheduler(make_settings(tmp_path))

    async def run_test() -> None:
        await scheduler._alerts_lock.acquire()
        try:
            await scheduler._run_alerts_once_locked()
        finally:
            scheduler._alerts_lock.release()

    asyncio.run(run_test())


def test_airtable_cap_refuses_and_allows(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    from upc_ingester import alerts as a

    settings = make_settings(tmp_path)
    profile = WatchProfile("recA", "Legal", "Legal", [], [], ["injunction"], [])
    match = match_alerts([sample_decision()], [profile])[0]
    monkeypatch.setattr(a, "_airtable_token", lambda: "test-token")
    monkeypatch.setattr(a, "_record_count_hint", lambda base_id, token: 100)

    with pytest.raises(RuntimeError):
        a.sync_matches_to_airtable(settings, [match], include_low_confidence=False, max_sync_records=1, dry_run=True)

    result = a.sync_matches_to_airtable(settings, [match], include_low_confidence=False, max_sync_records=3, dry_run=True)
    assert result["estimated_records"] == 2


def test_scheduler_alert_failure_isolated(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    from upc_ingester import scheduler as sched

    settings = make_settings(tmp_path)
    scheduler = CronScheduler(settings)

    def boom(*args, **kwargs):
        raise RuntimeError("boom")

    monkeypatch.setattr(sched, "run_alerts", boom)

    async def run_test() -> None:
        await scheduler._run_alerts_once_locked()

    asyncio.run(run_test())


def test_formula_lookup_uses_valid_field_name(monkeypatch: pytest.MonkeyPatch) -> None:
    from upc_ingester import alerts as a

    seen_params: dict = {}

    def fake_http_json(method: str, url: str, *, token: str, params=None, payload=None):
        seen_params.update(params or {})
        return {"records": []}

    monkeypatch.setattr(a, "_http_json", fake_http_json)
    _find_record_by_field("appBase", "tblTable", "Item key", "O'Reilly", "tok")
    assert seen_params["filterByFormula"] == r"{Item key}='O\'Reilly'"


def test_select_normalisation_helpers() -> None:
    assert normalise_document_type("Order of the Court") == "Order"
    assert normalise_document_type("Strange label") == "Other"
    assert normalise_language("ENGLISH") == "English"
    assert normalise_language("Spanish") == "Other"
    assert normalise_language("") == "Unknown"


def test_extract_pdf_text_empty_path_safe() -> None:
    assert _extract_pdf_text_if_available({"documents": [{"file_path": ""}, {"file_path": None}]}) == ""


def test_repeated_sync_updates_not_duplicates(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    from upc_ingester import alerts as a

    settings = make_settings(tmp_path)
    profile = WatchProfile("recA", "Legal", "Legal", [], [], ["injunction"], [])
    match = match_alerts([sample_decision()], [profile])[0]
    state = {"items": {}, "matches": {}, "next": 1}

    def rec_id() -> str:
        value = f"rec{state['next']}"
        state["next"] += 1
        return value

    def fake_http_json(method: str, url: str, *, token: str, params=None, payload=None):
        if method == "GET" and params and "filterByFormula" in params:
            formula = params["filterByFormula"]
            if formula.startswith("{Item key}="):
                key = formula.split("=", 1)[1].strip("'").replace("\\'", "'")
                row = state["items"].get(key)
                return {"records": ([{"id": row["id"], "fields": row["fields"]}] if row else [])}
            if formula.startswith("{Sync key}="):
                key = formula.split("=", 1)[1].strip("'").replace("\\'", "'")
                row = state["matches"].get(key)
                return {"records": ([{"id": row["id"], "fields": row["fields"]}] if row else [])}
            return {"records": []}
        if method == "GET":
            return {"records": []}
        if url.endswith(a.UPC_ITEMS_TABLE_ID):
            fields = payload["fields"]
            key = fields[a.UPC_ITEM_FIELDS["item_key"]]
            row = {"id": rec_id(), "fields": fields}
            state["items"][key] = row
            return row
        if a.UPC_ITEMS_TABLE_ID in url and method == "PATCH":
            rid = url.rstrip("/").split("/")[-1]
            for row in state["items"].values():
                if row["id"] == rid:
                    row["fields"].update(payload["fields"])
                    return row
        if url.endswith(a.MATCHES_TABLE_ID):
            fields = payload["fields"]
            key = fields[a.MATCH_FIELDS["sync_key"]]
            row = {"id": rec_id(), "fields": fields}
            state["matches"][key] = row
            return row
        if a.MATCHES_TABLE_ID in url and method == "PATCH":
            rid = url.rstrip("/").split("/")[-1]
            for row in state["matches"].values():
                if row["id"] == rid:
                    row["fields"].update(payload["fields"])
                    return row
        return {"records": []}

    monkeypatch.setattr(a, "_airtable_token", lambda: "token")
    monkeypatch.setattr(a, "_record_count_hint", lambda base_id, token: 10)
    monkeypatch.setattr(a, "_http_json", fake_http_json)

    first = sync_matches_to_airtable(settings, [match], include_low_confidence=False, max_sync_records=10, dry_run=False)
    second = sync_matches_to_airtable(settings, [match], include_low_confidence=False, max_sync_records=10, dry_run=False)
    assert first["created_items"] == 1
    assert first["created_matches"] == 1
    assert second["updated_items"] == 1
    assert second["updated_matches"] == 1
    assert len(state["items"]) == 1
    assert len(state["matches"]) == 1
