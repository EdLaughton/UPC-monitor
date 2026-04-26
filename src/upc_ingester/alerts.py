from __future__ import annotations

import argparse
import json
import logging
import os
import re
import tempfile
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from .config import DEFAULT_PUBLIC_BASE_URL, Settings
from .db import Database
from .urls import absolute_public_url, public_item_json_url, public_related_json_url

logger = logging.getLogger(__name__)
AIRTABLE_API_URL = "https://api.airtable.com/v0"
AIRTABLE_META_API_URL = "https://api.airtable.com/v0/meta"
UPC_ITEMS_TABLE_ID = "tblXzaunrmB52AYAn"
WATCH_PROFILES_TABLE_ID = "tblj5iRsGKHucmaVe"
MATCHES_TABLE_ID = "tbluFfUUnYeI1GfmP"
UPC_ITEM_KEY_FIELD_NAME = "Item key"
MATCH_SYNC_KEY_FIELD_NAME = "Sync key"

UPC_ITEM_FIELDS = {"item_key": "fld7QIzJJn2TkdT0G", "decision_date": "fldHpBLNBbxlRrHr4", "first_seen_at": "fldy19CTje2vDn0uA", "case_number": "fldh0LqWPZA3udV8d", "registry_number": "fldmdRNrXoPtsFztV", "division": "fldbjL3hih0KbAJmM", "document_type": "fldUiEsARsQdyxK8l", "type_of_action": "fldYkaJlCPv49SX6q", "language": "fldCbQdvS6wsYZdBD", "parties_raw": "fldAtKQaJYOwZYYbV", "party_names": "fldRijRgrk4tNto75", "headnote": "fldoXWqS9FHIHuKMf", "keywords": "fldFDCoYsMZKlz01O", "mirror_url": "fldTWvWTAfzkAFXpT", "official_pdf_url": "fldHwpA6oVwT883ck", "pdf_text_available": "fldMMknYefGqluoJl", "status": "fldGi0z9O0nMSsrxb", "notes": "fldzFvx7X39KtolyE"}
OPTIONAL_UPC_ITEM_FIELD_NAMES = {"item_context_url": "Context URL", "related_context_url": "Related context URL"}
WATCH_PROFILE_FIELDS = {"profile_name": "fldHcGbPUISXVG30k", "alert_type": "fldIJvi2gwGWGOeDI", "parties_to_watch": "fldV8ZBIfHjNxN5ya", "sector_terms": "fldw0gyEpkhShsr9c", "legal_terms": "fldjOSms0Gxvethnn", "competitors": "fldu3IKj4RKCVwHsJ", "active": "fldokHp9rr98TxLuX"}
WATCH_PROFILE_FIELD_NAMES = {"profile_name": "Profile name", "alert_type": "Alert type", "parties_to_watch": "Parties to watch", "sector_terms": "Sector terms", "legal_terms": "Legal terms/rules", "competitors": "Competitors/related entities", "active": "Active"}
MATCH_FIELDS = {"match_id": "fld5ZiLZTJBQzVfxp", "upc_item": "fldVNi138U4L2kdxN", "watch_profile": "fldCTzGU1CzP5H4SH", "confidence": "fldhHhIHtN8ssNhLy", "matched_fields": "fldCTmcrGIh3SxI0h", "matched_terms": "fldP310q0LIQdaTI9", "private_reason": "fldkZJ3p43YF1Yg3M", "public_reason": "fldQKJteweTaafkqh", "recommended_action": "fldzdJ7YUg11CP3Re", "reviewer_decision": "fldClHnbVKV5dG4ym", "ai_draft": "fldJYP97bcClTfR3m", "human_edited_draft": "fld232as4RuA4RBAL", "sync_key": "fldGBNNeEfkk0HwZJ"}


@dataclass
class WatchProfile:
    id: str
    name: str
    alert_type: str
    parties_to_watch: list[str]
    sector_terms: list[str]
    legal_terms: list[str]
    competitors: list[str]
    active: bool = True


@dataclass
class MatchResult:
    sync_key: str
    profile_id: str
    profile_name: str
    item_key: str
    confidence: str
    matched_fields: list[str]
    matched_terms: list[str]
    term_categories: dict[str, list[str]]
    confidence_reason: str
    private_reason: str
    public_reason: str
    recommended_action: str
    decision: dict[str, Any]


def _http_json(method: str, url: str, *, token: str, params: dict[str, Any] | None = None, payload: dict[str, Any] | None = None) -> dict[str, Any]:
    query = f"?{urlencode(params, doseq=True)}" if params else ""
    data = json.dumps(payload).encode("utf-8") if payload is not None else None
    req = Request(url + query, data=data, headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"}, method=method)
    with urlopen(req, timeout=20) as resp:
        return json.loads(resp.read().decode("utf-8"))


def normalize_text(value: str) -> str:
    return " ".join(re.sub(r"[^a-z0-9\s]", " ", (value or "").lower()).split())


def split_terms(value: str) -> list[str]:
    return sorted({t for t in (normalize_text(x) for x in re.split(r"[\n,;]+", value or "")) if len(t) > 2})


def _stringify_airtable_value(value: Any) -> str:
    if isinstance(value, list):
        return "\n".join(str(item) for item in value if item is not None)
    if value is None:
        return ""
    return str(value)


def _watch_profile_field(fields: dict[str, Any], key: str, default: Any = "") -> Any:
    field_id = WATCH_PROFILE_FIELDS[key]
    field_name = WATCH_PROFILE_FIELD_NAMES[key]
    if field_id in fields:
        return fields[field_id]
    return fields.get(field_name, default)


def _watch_profile_active(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return True
    if isinstance(value, str):
        return normalize_text(value) not in {"false", "no", "inactive", "off", "0"}
    return bool(value)


def profile_term_detail(profile: WatchProfile) -> dict[str, Any]:
    return {
        "name": profile.name,
        "party_terms": len(profile.parties_to_watch),
        "sector_terms": len(profile.sector_terms),
        "legal_terms": len(profile.legal_terms),
        "competitor_terms": len(profile.competitors),
    }


def build_sync_key(item_key: str, profile_id_or_name: str) -> str:
    return f"{item_key}::{profile_id_or_name}"


def _term_in_text(term: str, text: str) -> bool:
    return (f" {term} " in f" {text} ") if len(term) <= 3 else (term in text)


def _parse_simple_yaml_profiles(raw: str) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    current: dict[str, Any] | None = None
    for line in raw.splitlines():
        s = line.strip()
        if not s or s.startswith("#"):
            continue
        if s.startswith("-"):
            if current:
                out.append(current)
            current = {}
            s = s[1:].strip()
        if ":" in s and current is not None:
            k, v = s.split(":", 1)
            current[k.strip()] = v.strip().strip('"\'')
    if current:
        out.append(current)
    return out


def _extract_pdf_text_if_available(decision: dict[str, Any]) -> str:
    for doc in decision.get("documents", []):
        raw_path = str(doc.get("file_path") or "").strip()
        if not raw_path or raw_path in {".", "/"}:
            continue
        try:
            txt = Path(raw_path).with_suffix(".txt")
        except (ValueError, OSError):
            continue
        try:
            if txt.exists():
                return normalize_text(txt.read_text(encoding="utf-8", errors="ignore"))
        except OSError:
            continue
    return ""


def _load_watch_profiles_from_airtable(base_id: str, token: str) -> list[WatchProfile]:
    out: list[WatchProfile] = []
    params: dict[str, Any] = {"fields[]": list(WATCH_PROFILE_FIELDS.values()), "returnFieldsByFieldId": "true"}
    offset = ""
    while True:
        if offset:
            params["offset"] = offset
        data = _http_json("GET", f"{AIRTABLE_API_URL}/{base_id}/{WATCH_PROFILES_TABLE_ID}", token=token, params=params)
        for rec in data.get("records", []):
            f = rec.get("fields", {})
            if not _watch_profile_active(_watch_profile_field(f, "active", False)):
                continue
            out.append(
                WatchProfile(
                    rec["id"],
                    _stringify_airtable_value(_watch_profile_field(f, "profile_name", "Unnamed")) or "Unnamed",
                    _stringify_airtable_value(_watch_profile_field(f, "alert_type")),
                    split_terms(_stringify_airtable_value(_watch_profile_field(f, "parties_to_watch"))),
                    split_terms(_stringify_airtable_value(_watch_profile_field(f, "sector_terms"))),
                    split_terms(_stringify_airtable_value(_watch_profile_field(f, "legal_terms"))),
                    split_terms(_stringify_airtable_value(_watch_profile_field(f, "competitors"))),
                    True,
                )
            )
        offset = data.get("offset", "")
        if not offset:
            return out


def load_watch_profiles(settings: Settings) -> list[WatchProfile]:
    token = os.getenv("AIRTABLE_TOKEN") or os.getenv("AIRTABLE_API_KEY")
    if token:
        try:
            profiles = _load_watch_profiles_from_airtable(settings.airtable_base_id, token)
            if profiles:
                return profiles
        except Exception:
            logger.exception("Airtable profile load failed; using fallback")
    path = settings.data_dir / "private" / "watch_profiles.yml"
    if not path.exists():
        return []
    return [WatchProfile(str(r.get("id") or f"local-{i}"), str(r.get("profile_name") or r.get("name") or "Unnamed"), str(r.get("alert_type") or ""), split_terms(str(r.get("parties_to_watch") or "")), split_terms(str(r.get("sector_terms") or "")), split_terms(str(r.get("legal_terms") or "")), split_terms(str(r.get("competitors") or "")), bool(r.get("active", True))) for i, r in enumerate(_parse_simple_yaml_profiles(path.read_text(encoding="utf-8"))) if bool(r.get("active", True))]


def load_recent_decisions(db: Database, since_days: int) -> list[dict[str, Any]]:
    all_rows = db.get_decisions()
    if since_days <= 0:
        return all_rows
    cutoff = datetime.now(timezone.utc) - timedelta(days=since_days)
    out: list[dict[str, Any]] = []
    for row in all_rows:
        try:
            seen = datetime.fromisoformat(str(row.get("first_seen_at", "")).replace("Z", "+00:00"))
        except Exception:
            seen = None
        if seen and seen >= cutoff:
            out.append(row)
    return out


def recommend_action(alert_type: str, confidence: str) -> str:
    t = normalize_text(alert_type)
    if "bd" in t and confidence == "High":
        return "Send BD alert"
    if "legal" in t and confidence in {"High", "Medium"}:
        return "Send legal alert"
    return "Review"


def match_decision(decision: dict[str, Any], profile: WatchProfile) -> MatchResult | None:
    f = {"parties_raw": normalize_text(str(decision.get("parties_raw", ""))), "party_names_all": " ".join(normalize_text(str(x)) for x in decision.get("party_names_all", [])), "party_names_normalised": " ".join(normalize_text(str(x)) for x in decision.get("party_names_normalised", [])), "case_name_raw": normalize_text(str(decision.get("case_name_raw", ""))), "title_raw": normalize_text(str(decision.get("title_raw", ""))), "document_type": normalize_text(str(decision.get("document_type", ""))), "type_of_action": normalize_text(str(decision.get("type_of_action", ""))), "division": normalize_text(str(decision.get("division", ""))), "headnote_text": normalize_text(str(decision.get("headnote_text", ""))), "keywords_raw": normalize_text(str(decision.get("keywords_raw", ""))), "keywords_list": " ".join(normalize_text(str(x)) for x in decision.get("keywords_list", [])), "pdf_text": _extract_pdf_text_if_available(decision)}
    party_fields = ["parties_raw", "party_names_all", "party_names_normalised", "case_name_raw", "title_raw"]
    topical_fields = ["headnote_text", "keywords_raw", "keywords_list", "title_raw", "type_of_action", "document_type", "division", "pdf_text"]
    sector_fields = list(dict.fromkeys(party_fields + topical_fields))
    fields_by_category: dict[str, list[str]] = {"watched_party": [], "competitor": [], "sector": [], "legal": []}
    term_categories: dict[str, list[str]] = {"watched_party": [], "competitor": [], "sector": [], "legal": []}

    def collect(category: str, terms: list[str], fields: list[str]) -> None:
        for term in terms:
            for field in fields:
                if _term_in_text(term, f[field]):
                    term_categories[category].append(term)
                    fields_by_category[category].append(field)
                    break

    collect("watched_party", profile.parties_to_watch, party_fields)
    collect("competitor", profile.competitors, party_fields)
    collect("sector", profile.sector_terms, sector_fields)
    collect("legal", profile.legal_terms, topical_fields)

    term_categories = {category: sorted(set(terms)) for category, terms in term_categories.items()}
    fields_by_category = {category: sorted(set(fields)) for category, fields in fields_by_category.items()}
    matched_terms = sorted({term for terms in term_categories.values() for term in terms})
    matched_fields = sorted({field for fields in fields_by_category.values() for field in fields})
    watched_party_terms = term_categories["watched_party"]
    competitor_terms = term_categories["competitor"]
    sector_terms = term_categories["sector"]
    legal_terms = term_categories["legal"]
    if watched_party_terms:
        confidence = "High"
        confidence_reason = "watched party match" if not competitor_terms else "watched party plus competitor"
    elif competitor_terms and sector_terms:
        confidence = "High"
        confidence_reason = "competitor plus sector context"
    elif competitor_terms:
        confidence = "Medium"
        confidence_reason = "competitor match" if not legal_terms else "competitor plus legal context"
    elif sector_terms:
        confidence = "Medium"
        confidence_reason = "sector term match"
    elif legal_terms:
        confidence = "Medium"
        confidence_reason = "legal term match"
    else:
        confidence = "Low"
        confidence_reason = ""
    if not matched_terms:
        return None
    profile_ref = profile.id or profile.name
    category_reason = "; ".join(f"{category}_terms={', '.join(terms)}" for category, terms in term_categories.items() if terms)
    private_reason = f"Matched {profile.name}; confidence_reason={confidence_reason}; {category_reason}"
    return MatchResult(build_sync_key(str(decision.get("item_key", "")), profile_ref), profile.id, profile.name, str(decision.get("item_key", "")), confidence, matched_fields, matched_terms, term_categories, confidence_reason, private_reason, f"Potential relevance for profile {profile.name}", recommend_action(profile.alert_type, confidence), decision)


def match_alerts(decisions: list[dict[str, Any]], profiles: list[WatchProfile]) -> list[MatchResult]:
    return [m for d in decisions for p in profiles if (m := match_decision(d, p)) is not None]


def atomic_write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile("w", encoding="utf-8", delete=False, dir=path.parent) as h:
        h.write(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n")
        tmp = h.name
    Path(tmp).replace(path)


def write_private_outputs(settings: Settings, matches: list[MatchResult]) -> tuple[Path, Path]:
    private_dir = settings.data_dir / "private"
    items = [{"item_key": m.item_key, "profile_name": m.profile_name, "profile_id": m.profile_id, "confidence": m.confidence, "confidence_reason": m.confidence_reason, "matched_fields": m.matched_fields, "matched_terms": m.matched_terms, "term_categories": m.term_categories, "private_reason": m.private_reason, "public_reason": m.public_reason, "recommended_action": m.recommended_action, "decision": {"decision_date": m.decision.get("decision_date", ""), "case_number": m.decision.get("case_number", ""), "registry_number": m.decision.get("registry_number", ""), "division": m.decision.get("division", ""), "document_type": m.decision.get("document_type", ""), "type_of_action": m.decision.get("type_of_action", ""), "mirror_url": absolute_public_url(str(m.decision.get("pdf_url_mirror", "")), settings.public_base_url), "official_pdf_url": m.decision.get("pdf_url_official", ""), "node_url": m.decision.get("node_url", "")}} for m in matches]
    alerts = private_dir / "alerts.json"
    digest = private_dir / "alerts-digest-source.json"
    atomic_write_json(alerts, {"count": len(items), "items": items})
    atomic_write_json(digest, {"generated_at": datetime.now(timezone.utc).replace(microsecond=0).isoformat(), "count": len(items), "by_confidence": {"High": sum(1 for m in matches if m.confidence == "High"), "Medium": sum(1 for m in matches if m.confidence == "Medium"), "Low": sum(1 for m in matches if m.confidence == "Low")}, "items": items})
    return alerts, digest


def _airtable_token() -> str:
    token = os.getenv("AIRTABLE_TOKEN") or os.getenv("AIRTABLE_API_KEY")
    if not token:
        raise RuntimeError("AIRTABLE_TOKEN or AIRTABLE_API_KEY is required for Airtable sync")
    return token


def _match_sync_filter(match: MatchResult, include_low_confidence: bool) -> bool:
    return match.confidence in {"High", "Medium"} or (include_low_confidence and match.confidence == "Low")


CONFIDENCE_ORDER = {"Low": 1, "Medium": 2, "High": 3}
CONFIDENCE_LABELS = ("High", "Medium", "Low")


def _confidence_at_least(confidence: str, min_confidence: str) -> bool:
    return CONFIDENCE_ORDER.get(confidence, 0) >= CONFIDENCE_ORDER[min_confidence]


def _display_match_filter(match: MatchResult, min_confidence: str) -> bool:
    return _confidence_at_least(match.confidence, min_confidence)


def _syncable_matches(matches: list[MatchResult], include_low_confidence: bool, min_confidence: str = "Low") -> list[MatchResult]:
    return [m for m in matches if _match_sync_filter(m, include_low_confidence) and _confidence_at_least(m.confidence, min_confidence)]


def _newest_first(matches: list[MatchResult]) -> list[MatchResult]:
    return sorted(
        matches,
        key=lambda match: (
            str(match.decision.get("decision_date") or ""),
            str(match.decision.get("first_seen_at") or ""),
            match.item_key,
            match.profile_name,
        ),
        reverse=True,
    )


def _limit_matches(matches: list[MatchResult], sync_limit: int = 0) -> list[MatchResult]:
    ordered = _newest_first(matches)
    if sync_limit and sync_limit > 0:
        return ordered[:sync_limit]
    return ordered


def _syncable_limited_matches(matches: list[MatchResult], include_low_confidence: bool, min_confidence: str = "Low", sync_limit: int = 0) -> list[MatchResult]:
    return _limit_matches(_syncable_matches(matches, include_low_confidence, min_confidence), sync_limit)


def estimate_airtable_records(matches: list[MatchResult], include_low_confidence: bool, min_confidence: str = "Low", sync_limit: int = 0) -> int:
    syncable = _syncable_limited_matches(matches, include_low_confidence, min_confidence, sync_limit)
    return len({m.item_key for m in syncable}) + len(syncable)


def _truncate(value: Any, limit: int = 160) -> str:
    text = " ".join(str(value or "").split())
    if len(text) <= limit:
        return text
    return text[: max(0, limit - 3)].rstrip() + "..."


def _count_by_confidence(matches: list[MatchResult]) -> dict[str, int]:
    return {label: sum(1 for m in matches if m.confidence == label) for label in CONFIDENCE_LABELS}


def _counter_items(counter: Counter[str], key: str, limit: int) -> list[dict[str, Any]]:
    return [{key: value, "count": count} for value, count in counter.most_common(limit)]


def _sample_match(match: MatchResult, public_base_url: str = DEFAULT_PUBLIC_BASE_URL) -> dict[str, Any]:
    decision = match.decision
    return {
        "item_key": match.item_key,
        "decision_date": decision.get("decision_date", ""),
        "case_number": decision.get("case_number", ""),
        "parties_raw": _truncate(decision.get("parties_raw") or decision.get("primary_adverse_caption", "")),
        "primary_adverse_caption": _truncate(decision.get("primary_adverse_caption", "")),
        "profile": match.profile_name,
        "confidence": match.confidence,
        "confidence_reason": match.confidence_reason,
        "terms": match.matched_terms,
        "matched_terms": match.matched_terms,
        "matched_fields": match.matched_fields,
        "term_categories": match.term_categories,
        "title_raw": _truncate(decision.get("title_raw", "")),
        "mirror_url": absolute_public_url(str(decision.get("pdf_url_mirror", "")), public_base_url),
        "node_url": decision.get("node_url", ""),
    }


def build_alert_diagnostics(matches: list[MatchResult], *, include_low_confidence: bool, min_confidence: str, sample_limit: int, public_base_url: str = DEFAULT_PUBLIC_BASE_URL) -> dict[str, Any]:
    display_matches = [m for m in matches if _display_match_filter(m, min_confidence)]
    by_profile: dict[tuple[str, str], list[MatchResult]] = defaultdict(list)
    for match in display_matches:
        by_profile[(match.profile_id, match.profile_name)].append(match)

    matches_by_profile = []
    for (profile_id, profile_name), profile_matches in sorted(by_profile.items(), key=lambda item: (-len(item[1]), item[0][1].lower())):
        term_counts: Counter[str] = Counter()
        field_counts: Counter[str] = Counter()
        for match in profile_matches:
            term_counts.update(match.matched_terms)
            field_counts.update(match.matched_fields)
        matches_by_profile.append(
            {
                "profile_id": profile_id,
                "profile": profile_name,
                "matches_total": len(profile_matches),
                "confidence": _count_by_confidence(profile_matches),
                "estimated_airtable_records": estimate_airtable_records(profile_matches, include_low_confidence, min_confidence),
                "top_matched_terms": _counter_items(term_counts, "term", 10),
                "top_matched_fields": _counter_items(field_counts, "field", 10),
            }
        )

    by_term: dict[str, dict[str, Any]] = {}
    for match in display_matches:
        for term in match.matched_terms:
            entry = by_term.setdefault(term, {"term": term, "count": 0, "profiles": set(), "confidence": Counter()})
            entry["count"] += 1
            entry["profiles"].add(match.profile_name)
            entry["confidence"].update([match.confidence])

    matches_by_term = []
    for entry in sorted(by_term.values(), key=lambda item: (-item["count"], item["term"]))[:20]:
        matches_by_term.append(
            {
                "term": entry["term"],
                "count": entry["count"],
                "profiles": sorted(entry["profiles"]),
                "confidence": {label: entry["confidence"].get(label, 0) for label in CONFIDENCE_LABELS},
            }
        )

    return {
        "matches_by_profile": matches_by_profile,
        "matches_by_term": matches_by_term,
        "sample_matches": [_sample_match(m, public_base_url) for m in display_matches[:sample_limit]],
    }


def _record_count_hint(base_id: str, token: str) -> int | None:
    total = 0
    try:
        for table in (UPC_ITEMS_TABLE_ID, WATCH_PROFILES_TABLE_ID, MATCHES_TABLE_ID):
            offset = ""
            while True:
                params: dict[str, Any] = {"pageSize": 100}
                if offset:
                    params["offset"] = offset
                payload = _http_json("GET", f"{AIRTABLE_API_URL}/{base_id}/{table}", token=token, params=params)
                total += len(payload.get("records", []))
                offset = payload.get("offset", "")
                if not offset:
                    break
    except Exception:
        return None
    return total




def normalise_document_type(value: str) -> str:
    text = normalize_text(value)
    if "final decision" in text:
        return "Final Decision"
    if "judgment" in text or "judgement" in text:
        return "Judgment"
    if "decision" in text:
        return "Decision"
    if "order" in text:
        return "Order"
    return "Other"


def normalise_language(value: str) -> str:
    text = normalize_text(value)
    if not text:
        return "Unknown"
    mapping = {
        "english": "English",
        "german": "German",
        "french": "French",
        "italian": "Italian",
        "dutch": "Dutch",
    }
    for key, label in mapping.items():
        if key in text:
            return label
    return "Other"

def _apply_optional_upc_item_fields(fields: dict[str, Any], decision: dict[str, Any], public_base_url: str, optional_fields: dict[str, str] | None) -> dict[str, Any]:
    optional_fields = optional_fields or {}
    item_key = str(decision.get("item_key", ""))
    if field_id := optional_fields.get("item_context_url"):
        fields[field_id] = public_item_json_url(item_key, public_base_url)
    if field_id := optional_fields.get("related_context_url"):
        fields[field_id] = public_related_json_url(item_key, public_base_url)
    return fields


def _upc_item_create_fields(decision: dict[str, Any], public_base_url: str = DEFAULT_PUBLIC_BASE_URL, optional_fields: dict[str, str] | None = None) -> dict[str, Any]:
    fields = {
        UPC_ITEM_FIELDS["item_key"]: decision.get("item_key", ""),
        UPC_ITEM_FIELDS["decision_date"]: decision.get("decision_date", ""),
        UPC_ITEM_FIELDS["first_seen_at"]: decision.get("first_seen_at", ""),
        UPC_ITEM_FIELDS["case_number"]: decision.get("case_number", ""),
        UPC_ITEM_FIELDS["registry_number"]: decision.get("registry_number", ""),
        UPC_ITEM_FIELDS["division"]: decision.get("division", ""),
        UPC_ITEM_FIELDS["document_type"]: normalise_document_type(str(decision.get("document_type", ""))),
        UPC_ITEM_FIELDS["type_of_action"]: decision.get("type_of_action", ""),
        UPC_ITEM_FIELDS["language"]: normalise_language(str(decision.get("language", ""))),
        UPC_ITEM_FIELDS["parties_raw"]: decision.get("parties_raw", ""),
        UPC_ITEM_FIELDS["party_names"]: ", ".join(decision.get("party_names_all", []) or []),
        UPC_ITEM_FIELDS["headnote"]: decision.get("headnote_text", ""),
        UPC_ITEM_FIELDS["keywords"]: decision.get("keywords_raw", ""),
        UPC_ITEM_FIELDS["mirror_url"]: absolute_public_url(str(decision.get("pdf_url_mirror", "")), public_base_url),
        UPC_ITEM_FIELDS["official_pdf_url"]: decision.get("pdf_url_official", ""),
        UPC_ITEM_FIELDS["pdf_text_available"]: bool(_extract_pdf_text_if_available(decision)),
        UPC_ITEM_FIELDS["status"]: "New",
    }
    return _apply_optional_upc_item_fields(fields, decision, public_base_url, optional_fields)


def _upc_item_update_fields(decision: dict[str, Any], public_base_url: str = DEFAULT_PUBLIC_BASE_URL, optional_fields: dict[str, str] | None = None) -> dict[str, Any]:
    fields = _upc_item_create_fields(decision, public_base_url, optional_fields)
    fields.pop(UPC_ITEM_FIELDS["status"], None)
    fields.pop(UPC_ITEM_FIELDS["notes"], None)
    return fields


def _match_create_fields(match: MatchResult, upc_item_record_id: str) -> dict[str, Any]:
    return {
        MATCH_FIELDS["upc_item"]: [upc_item_record_id],
        MATCH_FIELDS["watch_profile"]: [match.profile_id] if match.profile_id.startswith("rec") else [],
        MATCH_FIELDS["confidence"]: match.confidence,
        MATCH_FIELDS["matched_fields"]: ", ".join(match.matched_fields),
        MATCH_FIELDS["matched_terms"]: ", ".join(match.matched_terms),
        MATCH_FIELDS["private_reason"]: match.private_reason,
        MATCH_FIELDS["public_reason"]: match.public_reason,
        MATCH_FIELDS["recommended_action"]: match.recommended_action,
        MATCH_FIELDS["reviewer_decision"]: "Pending",
        MATCH_FIELDS["sync_key"]: match.sync_key,
    }


def _match_update_fields(match: MatchResult, upc_item_record_id: str) -> dict[str, Any]:
    fields = _match_create_fields(match, upc_item_record_id)
    fields.pop(MATCH_FIELDS["reviewer_decision"], None)
    fields.pop(MATCH_FIELDS["ai_draft"], None)
    fields.pop(MATCH_FIELDS["human_edited_draft"], None)
    return fields


def _quote_airtable_formula_value(value: str) -> str:
    escaped = value.replace("\\", "\\\\").replace("'", "\\'")
    return f"'{escaped}'"


def _find_record_by_field(base_id: str, table_id: str, field_name: str, value: str, token: str) -> dict[str, Any] | None:
    formula = f"{{{field_name}}}={_quote_airtable_formula_value(str(value))}"
    payload = _http_json("GET", f"{AIRTABLE_API_URL}/{base_id}/{table_id}", token=token, params={"filterByFormula": formula, "maxRecords": 1})
    rows = payload.get("records", [])
    return rows[0] if rows else None


def _load_optional_upc_item_fields(base_id: str, token: str) -> dict[str, str]:
    try:
        payload = _http_json("GET", f"{AIRTABLE_META_API_URL}/bases/{base_id}/tables", token=token)
    except Exception:
        logger.info("Airtable metadata unavailable; optional UPC Item context URL fields will be skipped", exc_info=True)
        return {}
    for table in payload.get("tables", []):
        if table.get("id") != UPC_ITEMS_TABLE_ID:
            continue
        fields_by_name = {field.get("name"): field.get("id") for field in table.get("fields", [])}
        return {key: fields_by_name[name] for key, name in OPTIONAL_UPC_ITEM_FIELD_NAMES.items() if fields_by_name.get(name)}
    return {}


def sync_matches_to_airtable(settings: Settings, matches: list[MatchResult], include_low_confidence: bool, max_sync_records: int, dry_run: bool, min_confidence: str = "Low", sync_limit: int = 0) -> dict[str, Any]:
    token = _airtable_token()
    syncable = _syncable_limited_matches(matches, include_low_confidence, min_confidence, sync_limit)
    estimated = estimate_airtable_records(matches, include_low_confidence, min_confidence, sync_limit)
    if estimated > max_sync_records:
        raise RuntimeError(f"Refusing Airtable sync: estimated {estimated} records (UPC Items + Matches) exceeds cap {max_sync_records}. Use --airtable-max-sync-records to raise the cap.")
    total_hint = _record_count_hint(settings.airtable_base_id, token)
    if dry_run:
        return {"syncable_matches": len(syncable), "estimated_records": estimated, "base_record_count_hint": total_hint, "created_items": 0, "updated_items": 0, "created_matches": 0, "updated_matches": 0}
    created_items = updated_items = created_matches = updated_matches = 0
    item_cache: dict[str, str] = {}
    optional_upc_item_fields = _load_optional_upc_item_fields(settings.airtable_base_id, token)
    for match in syncable:
        if match.item_key not in item_cache:
            existing = _find_record_by_field(settings.airtable_base_id, UPC_ITEMS_TABLE_ID, UPC_ITEM_KEY_FIELD_NAME, match.item_key, token)
            if existing:
                _http_json("PATCH", f"{AIRTABLE_API_URL}/{settings.airtable_base_id}/{UPC_ITEMS_TABLE_ID}/{existing['id']}", token=token, payload={"fields": _upc_item_update_fields(match.decision, settings.public_base_url, optional_upc_item_fields)})
                item_cache[match.item_key] = existing["id"]
                updated_items += 1
            else:
                rec = _http_json("POST", f"{AIRTABLE_API_URL}/{settings.airtable_base_id}/{UPC_ITEMS_TABLE_ID}", token=token, payload={"fields": _upc_item_create_fields(match.decision, settings.public_base_url, optional_upc_item_fields)})
                item_cache[match.item_key] = rec["id"]
                created_items += 1
        existing_match = _find_record_by_field(settings.airtable_base_id, MATCHES_TABLE_ID, MATCH_SYNC_KEY_FIELD_NAME, match.sync_key, token)
        if existing_match:
            _http_json("PATCH", f"{AIRTABLE_API_URL}/{settings.airtable_base_id}/{MATCHES_TABLE_ID}/{existing_match['id']}", token=token, payload={"fields": _match_update_fields(match, item_cache[match.item_key])})
            updated_matches += 1
        else:
            _http_json("POST", f"{AIRTABLE_API_URL}/{settings.airtable_base_id}/{MATCHES_TABLE_ID}", token=token, payload={"fields": _match_create_fields(match, item_cache[match.item_key])})
            created_matches += 1
    return {"syncable_matches": len(syncable), "estimated_records": estimated, "base_record_count_hint": total_hint, "created_items": created_items, "updated_items": updated_items, "created_matches": created_matches, "updated_matches": updated_matches}


def run_alerts(settings: Settings, *, since_days: int, include_low_confidence: bool, write_json: bool, sync_airtable: bool, max_sync_records: int, dry_run: bool, diagnostics: bool = False, sample_limit: int = 10, profile: str = "", min_confidence: str = "Low", sync_limit: int = 0) -> dict[str, Any]:
    db = Database(settings.db_path)
    db.init()
    decisions = load_recent_decisions(db, since_days)
    profiles = load_watch_profiles(settings)
    if profile:
        profile_key = normalize_text(profile)
        profiles = [p for p in profiles if normalize_text(p.name) == profile_key]
    matches = match_alerts(decisions, profiles)
    sample_limit = max(0, sample_limit)
    syncable = _syncable_limited_matches(matches, include_low_confidence, min_confidence, sync_limit)
    summary: dict[str, Any] = {"decisions_scanned": len(decisions), "profiles_loaded": len(profiles), "profiles_loaded_detail": [profile_term_detail(p) for p in profiles], "profile_filter": profile, "min_confidence": min_confidence, "sync_limit": sync_limit, "matches_total": len(matches), "matches_by_confidence": _count_by_confidence(matches), "matches_syncable": len(syncable), "estimated_airtable_records": estimate_airtable_records(matches, include_low_confidence, min_confidence, sync_limit)}
    if dry_run or diagnostics:
        summary.update(build_alert_diagnostics(matches, include_low_confidence=include_low_confidence, min_confidence=min_confidence, sample_limit=sample_limit, public_base_url=settings.public_base_url))
    else:
        summary["sample_matches"] = [_sample_match(m, settings.public_base_url) for m in matches[:sample_limit]]
    if write_json:
        a, d = write_private_outputs(settings, matches)
        summary["alerts_json"] = str(a)
        summary["alerts_digest_source_json"] = str(d)
    if sync_airtable:
        summary["airtable_sync"] = sync_matches_to_airtable(settings, matches, include_low_confidence, max_sync_records, dry_run, min_confidence, sync_limit)
    return summary


def register_alerts_subcommand(subparsers: argparse._SubParsersAction[argparse.ArgumentParser]) -> None:
    p = subparsers.add_parser("alerts", help="run private alert matching and optional Airtable sync")
    p.add_argument("--dry-run", action="store_true")
    p.add_argument("--write-json", action="store_true")
    p.add_argument("--sync-airtable", action="store_true")
    p.add_argument("--since-days", type=int, default=7)
    p.add_argument("--include-low-confidence", action="store_true")
    p.add_argument("--airtable-max-sync-records", type=int, default=100)
    p.add_argument("--diagnostics", action="store_true")
    p.add_argument("--sample-limit", type=int, default=10)
    p.add_argument("--profile", default="")
    p.add_argument("--min-confidence", choices=list(CONFIDENCE_LABELS), default="Low")
    p.add_argument("--sync-limit", type=int, default=0)
