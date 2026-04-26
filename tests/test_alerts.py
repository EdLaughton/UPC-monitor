import asyncio
import json
from pathlib import Path

import pytest

from upc_ingester.alerts import (
    MATCH_FIELDS,
    WATCH_PROFILE_FIELDS,
    WATCH_PROFILE_FIELD_NAMES,
    WatchProfile,
    _extract_pdf_text_if_available,
    _find_record_by_field,
    _load_watch_profiles_from_airtable,
    _match_sync_filter,
    _match_create_fields,
    _match_update_fields,
    _quote_airtable_formula_value,
    absolute_public_url,
    normalise_document_type,
    normalise_language,
    sync_matches_to_airtable,
    UPC_ITEM_FIELDS,
    _upc_item_create_fields,
    _upc_item_update_fields,
    build_sync_key,
    estimate_airtable_records,
    match_alerts,
    run_alerts,
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


def test_absolute_public_url_normalises_relative_mirror_paths() -> None:
    assert absolute_public_url("/pdfs/2026/node-1/order.pdf", "https://upc.edlaughton.uk") == "https://upc.edlaughton.uk/pdfs/2026/node-1/order.pdf"
    assert absolute_public_url("/pdfs/2026/node-1/order.pdf", "https://upc.edlaughton.uk/") == "https://upc.edlaughton.uk/pdfs/2026/node-1/order.pdf"


def test_absolute_public_url_leaves_absolute_and_empty_values() -> None:
    assert absolute_public_url("https://example.test/order.pdf", "https://upc.edlaughton.uk") == "https://example.test/order.pdf"
    assert absolute_public_url("", "https://upc.edlaughton.uk") == ""
    assert absolute_public_url(None, "https://upc.edlaughton.uk") == ""


def test_airtable_item_payload_uses_configured_public_base_url() -> None:
    fields = _upc_item_create_fields(sample_decision(), "https://mirror.example.test/")
    assert fields[UPC_ITEM_FIELDS["mirror_url"]] == "https://mirror.example.test/pdfs/a.pdf"
    assert fields[UPC_ITEM_FIELDS["official_pdf_url"]] == "https://example.test/a.pdf"


def test_airtable_item_payload_can_include_optional_context_urls() -> None:
    fields = _upc_item_create_fields(
        sample_decision(),
        "https://mirror.example.test/",
        {"item_context_url": "fldContext", "related_context_url": "fldRelated"},
    )
    assert fields["fldContext"] == "https://mirror.example.test/items/node-1.json"
    assert fields["fldRelated"] == "https://mirror.example.test/related/node-1.json"


def test_party_match_is_high() -> None:
    profile = WatchProfile("recA", "BD", "BD", ["acme corp"], [], [], [])
    matches = match_alerts([sample_decision()], [profile])
    assert matches[0].confidence == "High"


def test_competitor_match_alone_is_medium() -> None:
    profile = WatchProfile("recA", "Comp", "Mixed", [], [], [], ["rival llc"])
    matches = match_alerts([sample_decision()], [profile])
    assert matches[0].confidence == "Medium"
    assert matches[0].confidence_reason == "competitor match"
    assert matches[0].term_categories["competitor"] == ["rival llc"]


def test_competitor_plus_sector_is_high() -> None:
    profile = WatchProfile("recA", "Comp", "Mixed", [], ["pharma"], [], ["rival llc"])
    matches = match_alerts([sample_decision()], [profile])
    assert matches[0].confidence == "High"
    assert matches[0].confidence_reason == "competitor plus sector context"
    assert matches[0].term_categories["competitor"] == ["rival llc"]
    assert matches[0].term_categories["sector"] == ["pharma"]
    assert "competitor_terms=rival llc" in matches[0].private_reason
    assert "sector_terms=pharma" in matches[0].private_reason


def test_competitor_plus_party_to_watch_is_high() -> None:
    profile = WatchProfile("recA", "Comp", "Mixed", ["acme corp"], [], [], ["rival llc"])
    matches = match_alerts([sample_decision()], [profile])
    assert matches[0].confidence == "High"
    assert matches[0].confidence_reason == "watched party plus competitor"
    assert matches[0].term_categories["watched_party"] == ["acme corp"]
    assert matches[0].term_categories["competitor"] == ["rival llc"]


def test_competitor_plus_legal_only_is_medium() -> None:
    profile = WatchProfile("recA", "Comp", "Mixed", [], [], ["injunction"], ["rival llc"])
    matches = match_alerts([sample_decision()], [profile])
    assert matches[0].confidence == "Medium"
    assert matches[0].confidence_reason == "competitor plus legal context"
    assert matches[0].term_categories["competitor"] == ["rival llc"]
    assert matches[0].term_categories["legal"] == ["injunction"]


def test_sector_term_match_is_medium() -> None:
    profile = WatchProfile("recA", "Sector", "Mixed", [], ["pharma"], [], [])
    matches = match_alerts([sample_decision()], [profile])
    assert matches[0].confidence == "Medium"
    assert matches[0].confidence_reason == "sector term match"


def test_legal_term_match_is_medium() -> None:
    profile = WatchProfile("recA", "Legal", "Legal interest", [], [], ["injunction"], [])
    matches = match_alerts([sample_decision()], [profile])
    assert matches[0].confidence == "Medium"
    assert matches[0].confidence_reason == "legal term match"


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
    payload = json.loads(alerts_path.read_text(encoding="utf-8"))
    assert payload["items"][0]["confidence_reason"] == "legal term match"
    assert payload["items"][0]["term_categories"]["legal"] == ["injunction"]
    assert payload["items"][0]["decision"]["mirror_url"] == "https://upc.edlaughton.uk/pdfs/a.pdf"
    assert payload["items"][0]["decision"]["official_pdf_url"] == "https://example.test/a.pdf"


def test_airtable_payload_rules() -> None:
    d = sample_decision()
    create_fields = _upc_item_create_fields(d)
    update_fields = _upc_item_update_fields(d)
    assert any(k.startswith("fld") for k in create_fields)
    assert create_fields != update_fields
    assert create_fields[UPC_ITEM_FIELDS["mirror_url"]] == "https://upc.edlaughton.uk/pdfs/a.pdf"
    assert create_fields[UPC_ITEM_FIELDS["official_pdf_url"]] == "https://example.test/a.pdf"

    profile = WatchProfile("recA", "Legal", "Legal", [], [], ["injunction"], [])
    match = match_alerts([d], [profile])[0]
    match_create = _match_create_fields(match, "recItem")
    match_update = _match_update_fields(match, "recItem")
    assert MATCH_FIELDS["match_id"] not in match_create
    assert match_create[MATCH_FIELDS["confidence"]] == "Medium"
    assert MATCH_FIELDS["reviewer_decision"] not in match_update
    assert MATCH_FIELDS["ai_draft"] not in match_update
    assert MATCH_FIELDS["human_edited_draft"] not in match_update
    assert match_update[MATCH_FIELDS["upc_item"]] == ["recItem"]
    assert match_update[MATCH_FIELDS["watch_profile"]] == ["recA"]


def test_scheduler_defaults_from_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("ALERTS_ENABLED", raising=False)
    monkeypatch.delenv("ALERTS_SYNC_AIRTABLE", raising=False)
    monkeypatch.delenv("ALERTS_SCHEDULE_HOUR", raising=False)
    monkeypatch.delenv("PUBLIC_BASE_URL", raising=False)
    from upc_ingester.config import Settings as S

    s = S.from_env()
    assert s.alerts_enabled is False
    assert s.alerts_sync_airtable is False
    assert s.alerts_schedule_hour == 10
    assert s.alerts_schedule_minute == 5
    assert s.alerts_since_days == 7
    assert s.public_base_url == "https://upc.edlaughton.uk"


def test_public_base_url_from_env(monkeypatch: pytest.MonkeyPatch) -> None:
    from upc_ingester.config import Settings as S

    monkeypatch.setenv("PUBLIC_BASE_URL", "https://mirror.example.test/")
    assert S.from_env().public_base_url == "https://mirror.example.test/"


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


def test_airtable_profile_loader_requests_and_reads_field_ids(monkeypatch: pytest.MonkeyPatch) -> None:
    from upc_ingester import alerts as a

    seen_params: dict = {}

    def fake_http_json(method: str, url: str, *, token: str, params=None, payload=None):
        seen_params.update(params or {})
        return {
            "records": [
                {
                    "id": "recAbbott",
                    "fields": {
                        WATCH_PROFILE_FIELDS["profile_name"]: "Abbott / glucose monitoring / medtech",
                        WATCH_PROFILE_FIELDS["alert_type"]: "BD",
                        WATCH_PROFILE_FIELDS["parties_to_watch"]: "Abbott\nAbbott Diabetes Care",
                        WATCH_PROFILE_FIELDS["sector_terms"]: "glucose monitoring; medtech",
                        WATCH_PROFILE_FIELDS["legal_terms"]: "injunction\nFRAND",
                        WATCH_PROFILE_FIELDS["competitors"]: "Dexcom; Medtronic",
                        WATCH_PROFILE_FIELDS["active"]: True,
                    },
                }
            ]
        }

    monkeypatch.setattr(a, "_http_json", fake_http_json)
    profiles = _load_watch_profiles_from_airtable("appBase", "token")
    assert seen_params["returnFieldsByFieldId"] == "true"
    assert seen_params["fields[]"] == list(WATCH_PROFILE_FIELDS.values())
    assert profiles[0].parties_to_watch == ["abbott", "abbott diabetes care"]
    assert profiles[0].sector_terms == ["glucose monitoring", "medtech"]
    assert profiles[0].legal_terms == ["frand", "injunction"]
    assert profiles[0].competitors == ["dexcom", "medtronic"]


def test_airtable_profile_loader_skips_active_false(monkeypatch: pytest.MonkeyPatch) -> None:
    from upc_ingester import alerts as a

    def fake_http_json(method: str, url: str, *, token: str, params=None, payload=None):
        return {
            "records": [
                {
                    "id": "recInactive",
                    "fields": {
                        WATCH_PROFILE_FIELDS["profile_name"]: "Inactive profile",
                        WATCH_PROFILE_FIELDS["parties_to_watch"]: "Acme",
                        WATCH_PROFILE_FIELDS["active"]: False,
                    },
                }
            ]
        }

    monkeypatch.setattr(a, "_http_json", fake_http_json)
    assert _load_watch_profiles_from_airtable("appBase", "token") == []


def test_airtable_profile_loader_skips_missing_active(monkeypatch: pytest.MonkeyPatch) -> None:
    from upc_ingester import alerts as a

    def fake_http_json(method: str, url: str, *, token: str, params=None, payload=None):
        return {
            "records": [
                {
                    "id": "recMissingActive",
                    "fields": {
                        WATCH_PROFILE_FIELDS["profile_name"]: "Missing active profile",
                        WATCH_PROFILE_FIELDS["parties_to_watch"]: "Acme",
                    },
                }
            ]
        }

    monkeypatch.setattr(a, "_http_json", fake_http_json)
    assert _load_watch_profiles_from_airtable("appBase", "token") == []


def test_optional_airtable_context_fields_absent_is_backwards_compatible(monkeypatch: pytest.MonkeyPatch) -> None:
    from upc_ingester import alerts as a

    def fake_http_json(method: str, url: str, *, token: str, params=None, payload=None):
        return {"tables": [{"id": a.UPC_ITEMS_TABLE_ID, "fields": [{"id": "fldOther", "name": "Other"}]}]}

    monkeypatch.setattr(a, "_http_json", fake_http_json)
    assert a._load_optional_upc_item_fields("appBase", "token") == {}


def test_optional_airtable_context_fields_are_discovered(monkeypatch: pytest.MonkeyPatch) -> None:
    from upc_ingester import alerts as a

    def fake_http_json(method: str, url: str, *, token: str, params=None, payload=None):
        return {
            "tables": [
                {
                    "id": a.UPC_ITEMS_TABLE_ID,
                    "fields": [
                        {"id": "fldContext", "name": "Context URL"},
                        {"id": "fldRelated", "name": "Related context URL"},
                    ],
                }
            ]
        }

    monkeypatch.setattr(a, "_http_json", fake_http_json)
    assert a._load_optional_upc_item_fields("appBase", "token") == {
        "item_context_url": "fldContext",
        "related_context_url": "fldRelated",
    }


def test_airtable_profile_loader_accepts_field_name_keys(monkeypatch: pytest.MonkeyPatch) -> None:
    from upc_ingester import alerts as a

    def fake_http_json(method: str, url: str, *, token: str, params=None, payload=None):
        return {
            "records": [
                {
                    "id": "recAbbott",
                    "fields": {
                        WATCH_PROFILE_FIELD_NAMES["profile_name"]: "Abbott / glucose monitoring / medtech",
                        WATCH_PROFILE_FIELD_NAMES["alert_type"]: "BD",
                        WATCH_PROFILE_FIELD_NAMES["parties_to_watch"]: ["Abbott", "Abbott Diabetes Care"],
                        WATCH_PROFILE_FIELD_NAMES["sector_terms"]: "glucose monitoring; medtech",
                        WATCH_PROFILE_FIELD_NAMES["legal_terms"]: "injunction\nFRAND",
                        WATCH_PROFILE_FIELD_NAMES["competitors"]: "Dexcom; Medtronic",
                        WATCH_PROFILE_FIELD_NAMES["active"]: True,
                    },
                }
            ]
        }

    monkeypatch.setattr(a, "_http_json", fake_http_json)
    profiles = _load_watch_profiles_from_airtable("appBase", "token")
    assert profiles[0].name == "Abbott / glucose monitoring / medtech"
    assert profiles[0].parties_to_watch == ["abbott", "abbott diabetes care"]
    assert profiles[0].sector_terms == ["glucose monitoring", "medtech"]
    assert profiles[0].legal_terms == ["frand", "injunction"]
    assert profiles[0].competitors == ["dexcom", "medtronic"]


def test_dry_run_summary_includes_profile_term_counts(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    from upc_ingester import alerts as a

    profile = WatchProfile(
        "recAbbott",
        "Abbott / glucose monitoring / medtech",
        "BD",
        ["abbott", "abbott diabetes care"],
        ["glucose monitoring"],
        ["injunction", "frand"],
        ["dexcom"],
    )
    monkeypatch.setattr(a, "load_recent_decisions", lambda db, since_days: [])
    monkeypatch.setattr(a, "load_watch_profiles", lambda settings: [profile])

    summary = run_alerts(
        make_settings(tmp_path),
        since_days=7,
        include_low_confidence=False,
        write_json=False,
        sync_airtable=False,
        max_sync_records=100,
        dry_run=True,
    )
    assert summary["profiles_loaded_detail"] == [
        {
            "name": "Abbott / glucose monitoring / medtech",
            "party_terms": 2,
            "sector_terms": 1,
            "legal_terms": 2,
            "competitor_terms": 1,
        }
    ]


def test_dry_run_summary_counts_only_active_airtable_profiles(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    from upc_ingester import alerts as a

    def fake_http_json(method: str, url: str, *, token: str, params=None, payload=None):
        return {
            "records": [
                {
                    "id": "recActive",
                    "fields": {
                        WATCH_PROFILE_FIELDS["profile_name"]: "Active profile",
                        WATCH_PROFILE_FIELDS["alert_type"]: "BD",
                        WATCH_PROFILE_FIELDS["parties_to_watch"]: "Acme",
                        WATCH_PROFILE_FIELDS["active"]: True,
                    },
                },
                {
                    "id": "recInactive",
                    "fields": {
                        WATCH_PROFILE_FIELDS["profile_name"]: "Inactive profile",
                        WATCH_PROFILE_FIELDS["alert_type"]: "BD",
                        WATCH_PROFILE_FIELDS["parties_to_watch"]: "Rival",
                        WATCH_PROFILE_FIELDS["active"]: False,
                    },
                },
                {
                    "id": "recMissingActive",
                    "fields": {
                        WATCH_PROFILE_FIELDS["profile_name"]: "Missing active profile",
                        WATCH_PROFILE_FIELDS["alert_type"]: "BD",
                        WATCH_PROFILE_FIELDS["parties_to_watch"]: "Ghost",
                    },
                },
            ]
        }

    monkeypatch.setenv("AIRTABLE_TOKEN", "token")
    monkeypatch.setattr(a, "_http_json", fake_http_json)
    monkeypatch.setattr(a, "load_recent_decisions", lambda db, since_days: [])

    summary = run_alerts(
        make_settings(tmp_path),
        since_days=7,
        include_low_confidence=False,
        write_json=False,
        sync_airtable=False,
        max_sync_records=100,
        dry_run=True,
    )
    assert summary["profiles_loaded"] == 1
    assert summary["profiles_loaded_detail"] == [
        {
            "name": "Active profile",
            "party_terms": 1,
            "sector_terms": 0,
            "legal_terms": 0,
            "competitor_terms": 0,
        }
    ]


def test_dry_run_diagnostics_matches_by_profile_counts(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    from upc_ingester import alerts as a

    decisions = [
        sample_decision() | {"item_key": "node-1", "parties_raw": "Acme Corp v. Rival LLC", "case_name_raw": "Acme Corp v. Rival LLC"},
        sample_decision()
        | {
            "item_key": "node-2",
            "parties_raw": "Acme Corp v. Other LLC",
            "case_name_raw": "Acme Corp v. Other LLC",
            "headnote_text": "",
            "keywords_raw": "",
            "keywords_list": [],
        },
        sample_decision()
        | {
            "item_key": "node-3",
            "parties_raw": "Other v. Someone",
            "party_names_all": ["Other", "Someone"],
            "party_names_normalised": ["other", "someone"],
            "case_name_raw": "Other v. Someone",
            "headnote_text": "injunction question",
        },
    ]
    profiles = [
        WatchProfile("recParty", "Party Watch", "BD", ["acme corp"], [], [], []),
        WatchProfile("recLegal", "Legal Watch", "Legal", [], [], ["injunction"], []),
    ]
    monkeypatch.setattr(a, "load_recent_decisions", lambda db, since_days: decisions)
    monkeypatch.setattr(a, "load_watch_profiles", lambda settings: profiles)

    summary = run_alerts(
        make_settings(tmp_path),
        since_days=7,
        include_low_confidence=False,
        write_json=False,
        sync_airtable=False,
        max_sync_records=100,
        dry_run=True,
    )
    by_profile = {row["profile"]: row for row in summary["matches_by_profile"]}
    assert by_profile["Party Watch"]["matches_total"] == 2
    assert by_profile["Party Watch"]["confidence"] == {"High": 2, "Medium": 0, "Low": 0}
    assert by_profile["Party Watch"]["estimated_airtable_records"] == 4
    assert by_profile["Party Watch"]["top_matched_terms"] == [{"term": "acme corp", "count": 2}]
    assert by_profile["Party Watch"]["top_matched_fields"][0] == {"field": "parties_raw", "count": 2}
    assert by_profile["Legal Watch"]["matches_total"] == 2
    assert by_profile["Legal Watch"]["confidence"] == {"High": 0, "Medium": 2, "Low": 0}


def test_dry_run_diagnostics_matches_by_term_counts(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    from upc_ingester import alerts as a

    decisions = [
        sample_decision() | {"item_key": "node-1", "headnote_text": "injunction and pharma"},
        sample_decision()
        | {
            "item_key": "node-2",
            "parties_raw": "Neutral v. Other",
            "party_names_all": ["Neutral", "Other"],
            "party_names_normalised": ["neutral", "other"],
            "case_name_raw": "Neutral v. Other",
            "headnote_text": "injunction",
        },
    ]
    profiles = [
        WatchProfile("recLegal", "Legal Watch", "Legal", [], [], ["injunction"], []),
        WatchProfile("recSector", "Sector Watch", "BD", [], ["pharma"], [], []),
    ]
    monkeypatch.setattr(a, "load_recent_decisions", lambda db, since_days: decisions)
    monkeypatch.setattr(a, "load_watch_profiles", lambda settings: profiles)

    summary = run_alerts(
        make_settings(tmp_path),
        since_days=7,
        include_low_confidence=False,
        write_json=False,
        sync_airtable=False,
        max_sync_records=100,
        dry_run=True,
    )
    by_term = {row["term"]: row for row in summary["matches_by_term"]}
    assert by_term["injunction"] == {
        "term": "injunction",
        "count": 2,
        "profiles": ["Legal Watch"],
        "confidence": {"High": 0, "Medium": 2, "Low": 0},
    }
    assert by_term["pharma"]["count"] == 1
    assert by_term["pharma"]["profiles"] == ["Sector Watch"]


def test_dry_run_sample_match_includes_terms_fields_and_case_metadata(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    from upc_ingester import alerts as a

    decision = sample_decision() | {
        "case_number": "UPC_CFI_123/2026",
        "primary_adverse_caption": "Acme Corp v. Rival LLC",
        "node_url": "https://example.test/node/1",
        "title_raw": "A very long title " * 20,
    }
    profile = WatchProfile("recParty", "Party Watch", "BD", ["acme corp"], [], [], [])
    monkeypatch.setattr(a, "load_recent_decisions", lambda db, since_days: [decision])
    monkeypatch.setattr(a, "load_watch_profiles", lambda settings: [profile])

    summary = run_alerts(
        make_settings(tmp_path),
        since_days=7,
        include_low_confidence=False,
        write_json=False,
        sync_airtable=False,
        max_sync_records=100,
        dry_run=True,
    )
    sample = summary["sample_matches"][0]
    assert sample["item_key"] == "node-1"
    assert sample["decision_date"] == "2026-04-20"
    assert sample["case_number"] == "UPC_CFI_123/2026"
    assert sample["parties_raw"] == "Acme Corp v. Rival LLC"
    assert sample["profile"] == "Party Watch"
    assert sample["confidence_reason"] == "watched party match"
    assert sample["matched_terms"] == ["acme corp"]
    assert sample["terms"] == ["acme corp"]
    assert sample["term_categories"]["watched_party"] == ["acme corp"]
    assert "parties_raw" in sample["matched_fields"]
    assert sample["mirror_url"] == "https://upc.edlaughton.uk/pdfs/a.pdf"
    assert sample["node_url"] == "https://example.test/node/1"
    assert len(sample["title_raw"]) <= 160


def test_profile_option_restricts_diagnostics_output(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    from upc_ingester import alerts as a

    profiles = [
        WatchProfile("recParty", "Party Watch", "BD", ["acme corp"], [], [], []),
        WatchProfile("recLegal", "Legal Watch", "Legal", [], [], ["injunction"], []),
    ]
    monkeypatch.setattr(a, "load_recent_decisions", lambda db, since_days: [sample_decision()])
    monkeypatch.setattr(a, "load_watch_profiles", lambda settings: profiles)

    summary = run_alerts(
        make_settings(tmp_path),
        since_days=7,
        include_low_confidence=False,
        write_json=False,
        sync_airtable=False,
        max_sync_records=100,
        dry_run=True,
        profile="Legal Watch",
    )
    assert summary["profiles_loaded"] == 1
    assert summary["profile_filter"] == "Legal Watch"
    assert [row["profile"] for row in summary["matches_by_profile"]] == ["Legal Watch"]
    assert all(sample["profile"] == "Legal Watch" for sample in summary["sample_matches"])


def test_sample_limit_controls_dry_run_sample_matches(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    from upc_ingester import alerts as a

    decisions = [
        sample_decision() | {"item_key": f"node-{i}", "parties_raw": f"Acme Corp v. Other {i}", "case_name_raw": f"Acme Corp v. Other {i}"}
        for i in range(5)
    ]
    profile = WatchProfile("recParty", "Party Watch", "BD", ["acme corp"], [], [], [])
    monkeypatch.setattr(a, "load_recent_decisions", lambda db, since_days: decisions)
    monkeypatch.setattr(a, "load_watch_profiles", lambda settings: [profile])

    summary = run_alerts(
        make_settings(tmp_path),
        since_days=7,
        include_low_confidence=False,
        write_json=False,
        sync_airtable=False,
        max_sync_records=100,
        dry_run=True,
        sample_limit=2,
    )
    assert len(summary["sample_matches"]) == 2
    assert [sample["item_key"] for sample in summary["sample_matches"]] == ["node-0", "node-1"]


def test_loaded_abbott_airtable_profile_matches_decision(monkeypatch: pytest.MonkeyPatch) -> None:
    from upc_ingester import alerts as a

    def fake_http_json(method: str, url: str, *, token: str, params=None, payload=None):
        return {
            "records": [
                {
                    "id": "recAbbott",
                    "fields": {
                        WATCH_PROFILE_FIELDS["profile_name"]: "Abbott / glucose monitoring / medtech",
                        WATCH_PROFILE_FIELDS["alert_type"]: "BD",
                        WATCH_PROFILE_FIELDS["parties_to_watch"]: "Abbott\nAbbott Diabetes Care",
                        WATCH_PROFILE_FIELDS["sector_terms"]: "glucose monitoring",
                        WATCH_PROFILE_FIELDS["legal_terms"]: "FRAND",
                        WATCH_PROFILE_FIELDS["competitors"]: "",
                        WATCH_PROFILE_FIELDS["active"]: True,
                    },
                }
            ]
        }

    decision = sample_decision() | {
        "item_key": "node-abbott",
        "parties_raw": "Abbott Diabetes Care Inc. v. Example GmbH",
        "title_raw": "Order in Abbott glucose monitoring dispute",
        "headnote_text": "The panel considered Abbott arguments and FRAND issues.",
    }
    monkeypatch.setattr(a, "_http_json", fake_http_json)
    profiles = _load_watch_profiles_from_airtable("appBase", "token")
    matches = match_alerts([decision], profiles)
    assert len(matches) == 1
    assert matches[0].profile_name == "Abbott / glucose monitoring / medtech"
    assert matches[0].confidence == "High"
    assert "abbott" in matches[0].matched_terms


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
