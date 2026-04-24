from __future__ import annotations

import re
from collections import Counter, defaultdict
from datetime import date, datetime
from statistics import mean, median
from typing import Any, Iterable


LANGUAGE_PREFIX_RE = re.compile(r"\b(?:ENGLISH|GERMAN|FRENCH|ITALIAN|DUTCH|PORTUGUESE|FR|DE|EN|IT|NL|PT)\s*:\s*", re.I)
LEGAL_REFERENCE_PATTERNS = (
    re.compile(r"\bArt\.?\s*\d+[a-z]?(?:\(\d+\))*\.?(?:\s*\([^)]+\))*\s*(?:UPCA|EPG[UÜ]|AJUB|EPC)\b", re.I),
    re.compile(r"\bR\.?\s*\d+[a-z]?(?:\.\d+[a-z]?)?(?:\s*\([^)]+\))*\s*RoP\b", re.I),
    re.compile(r"\bRule\s+\d+[a-z]?(?:\.\d+[a-z]?)?(?:\s*\([^)]+\))*\s*(?:RoP)?\b", re.I),
    re.compile(r"\bBrussels\s+I(?:a| recast)?\b", re.I),
    re.compile(r"\bRegulation\s*(?:\(EU\)\s*)?(?:No\.?\s*)?1215/2012(?:/EU)?\b", re.I),
)
ACTION_WORDS = ("action", "application", "appeal", "counterclaim", "withdrawal", "interpretation")


def parse_date_safe(value: Any) -> date | None:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        return date.fromisoformat(text[:10])
    except ValueError:
        return None


def parse_datetime_safe(value: Any) -> datetime | None:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        return datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return None


def text_value(item: dict[str, Any], key: str) -> str:
    return str(item.get(key) or "").strip()


def has_text(item: dict[str, Any], key: str) -> bool:
    return bool(text_value(item, key))


def percent(part: int, total: int) -> float:
    return round((part / total) * 100, 1) if total else 0.0


def sorted_count_rows(counter: Counter[str], total: int | None = None, limit: int | None = None) -> list[dict[str, Any]]:
    rows = [
        {
            "value": value,
            "count": count,
            **({"percent": percent(count, total)} if total is not None else {}),
        }
        for value, count in sorted(counter.items(), key=lambda pair: (-pair[1], pair[0].lower()))
    ]
    return rows[:limit] if limit else rows


def classify_court_level(division: Any) -> str:
    text = str(division or "").strip().lower()
    if "court of appeal" in text or text.startswith("luxembourg"):
        return "Court of Appeal"
    if "central division" in text:
        return "Court of First Instance - Central Division"
    if "local division" in text:
        return "Court of First Instance - Local Division"
    return "Other/Unknown"


def normalise_keyword(value: Any) -> list[str]:
    text = str(value or "").replace("\r", "\n").strip()
    if not text:
        return []
    text = LANGUAGE_PREFIX_RE.sub("", text)
    parts = re.split(r"\s*(?:;|\n|(?:\s+[–—]\s+))\s*", text)
    return [" ".join(part.split()).strip(" .") for part in parts if " ".join(part.split()).strip(" .")]


def iter_keywords(item: dict[str, Any]) -> Iterable[str]:
    keywords_list = item.get("keywords_list")
    if isinstance(keywords_list, list):
        for keyword in keywords_list:
            yield from normalise_keyword(keyword)
    yield from normalise_keyword(item.get("keywords_raw", ""))


def extract_legal_references(*values: Any) -> list[str]:
    text = " ".join(str(value or "") for value in values)
    references: set[str] = set()
    for pattern in LEGAL_REFERENCE_PATTERNS:
        for match in pattern.finditer(text):
            reference = " ".join(match.group(0).split()).strip(" .")
            references.add(reference)
    return sorted(references, key=str.lower)


def compute_observed_lag_days(item: dict[str, Any]) -> dict[str, Any] | None:
    decision_date = parse_date_safe(item.get("decision_date"))
    seen_at = parse_datetime_safe(item.get("first_seen_at")) or parse_datetime_safe(item.get("ingested_at"))
    if not decision_date or not seen_at:
        return None
    return {
        "item_key": item.get("item_key", ""),
        "decision_date": decision_date.isoformat(),
        "seen_at": seen_at.isoformat(),
        "days": (seen_at.date() - decision_date).days,
        "title": item.get("title_raw") or item.get("document_type") or "",
        "case_number": item.get("case_number") or "",
        "node_url": item.get("node_url") or "",
    }


def item_label(item: dict[str, Any]) -> dict[str, Any]:
    return {
        "item_key": item.get("item_key", ""),
        "decision_date": item.get("decision_date", ""),
        "case_number": item.get("case_number", ""),
        "registry_number": item.get("registry_number", ""),
        "document_type": item.get("document_type", ""),
        "type_of_action": item.get("type_of_action", ""),
        "division": item.get("division", ""),
        "node_url": item.get("node_url", ""),
    }


def parties_by_role(item: dict[str, Any], role: str) -> list[str]:
    parties = item.get("parties_json")
    if not isinstance(parties, list):
        return []
    role_lower = role.lower()
    return [
        str(party.get("name") or "").strip()
        for party in parties
        if isinstance(party, dict)
        and str(party.get("role") or "").lower() == role_lower
        and str(party.get("name") or "").strip()
    ]


def party_names(item: dict[str, Any]) -> list[str]:
    names = item.get("party_names_all")
    if isinstance(names, list):
        return [str(name).strip() for name in names if str(name).strip()]
    return []


def normalised_party_key(item: dict[str, Any]) -> str:
    names = item.get("party_names_normalised")
    if not isinstance(names, list):
        return ""
    return " :: ".join(sorted(str(name).strip() for name in names if str(name).strip()))


def suspicious_party_strings(items: list[dict[str, Any]], limit: int = 20) -> list[dict[str, Any]]:
    markers = re.compile(r"\b(and other|and others|a\.\s*o\.|et al\.?)\b|[.]{2,}|,,", re.I)
    rows = []
    for item in items:
        raw = text_value(item, "parties_raw")
        if raw and markers.search(raw):
            rows.append({**item_label(item), "parties_raw": raw})
    return rows[:limit]


def cluster_rows(groups: dict[str, list[dict[str, Any]]], key_name: str, limit: int = 20) -> list[dict[str, Any]]:
    rows = []
    for key, grouped_items in groups.items():
        if not key or len(grouped_items) < 2:
            continue
        rows.append(
            {
                key_name: key,
                "caption": next((text_value(item, "primary_adverse_caption") for item in grouped_items if text_value(item, "primary_adverse_caption")), ""),
                "count": len(grouped_items),
                "case_numbers": sorted({text_value(item, "case_number") for item in grouped_items if text_value(item, "case_number")}),
                "registry_numbers": sorted({text_value(item, "registry_number") for item in grouped_items if text_value(item, "registry_number")}),
                "action_types": sorted({text_value(item, "type_of_action") for item in grouped_items if text_value(item, "type_of_action")}),
                "decision_dates": sorted({text_value(item, "decision_date") for item in grouped_items if text_value(item, "decision_date")}),
                "items": [item_label(item) for item in grouped_items],
            }
        )
    return sorted(rows, key=lambda row: (-row["count"], str(row.get(key_name, "")).lower()))[:limit]


def build_stats(items: list[dict[str, Any]], generated_at: str | None = None) -> dict[str, Any]:
    generated_at = generated_at or datetime.now().astimezone().replace(microsecond=0).isoformat()
    total = len(items)
    parsed_dates = [(item, parse_date_safe(item.get("decision_date"))) for item in items]
    valid_dates = [parsed for _, parsed in parsed_dates if parsed]
    invalid_date_items = [item_label(item) | {"decision_date": item.get("decision_date", "")} for item, parsed in parsed_dates if not parsed]

    official_pdf_count = sum(1 for item in items if has_text(item, "pdf_url_official"))
    mirror_pdf_count = sum(1 for item in items if has_text(item, "pdf_url_mirror"))
    no_pdf_count = sum(1 for item in items if not has_text(item, "pdf_url_official") and not has_text(item, "pdf_url_mirror"))
    headnote_items = [item for item in items if has_text(item, "headnote_text")]
    keyword_items = [item for item in items if list(iter_keywords(item))]
    error_items = [item for item in items if has_text(item, "last_error")]
    empty_documents = [item for item in items if not item.get("documents")]

    unique_pdf_hashes = {text_value(item, "pdf_sha256") for item in items if has_text(item, "pdf_sha256")}
    unique_adverse_pairs = {text_value(item, "adverse_pair_key") for item in items if has_text(item, "adverse_pair_key")}
    unique_parties = {name for item in items for name in party_names(item)}

    month_counter: Counter[str] = Counter()
    week_counter: Counter[str] = Counter()
    date_counter: Counter[str] = Counter()
    for _, parsed in parsed_dates:
        if parsed:
            month_counter[parsed.strftime("%Y-%m")] += 1
            week_counter[f"{parsed.isocalendar().year}-W{parsed.isocalendar().week:02d}"] += 1
            date_counter[parsed.isoformat()] += 1

    cumulative = []
    running = 0
    for month in sorted(month_counter):
        running += month_counter[month]
        cumulative.append({"value": month, "count": running})

    divisions = defaultdict(list)
    for item in items:
        divisions[text_value(item, "division") or "(blank)"].append(item)
    division_rows = []
    court_level_counter: Counter[str] = Counter()
    for division, division_items in divisions.items():
        action_counter = Counter(text_value(item, "type_of_action") or "(blank)" for item in division_items)
        court_level = classify_court_level(division)
        court_level_counter[court_level] += len(division_items)
        division_rows.append(
            {
                "division": division,
                "court_level": court_level,
                "count": len(division_items),
                "percent": percent(len(division_items), total),
                "with_pdfs": sum(1 for item in division_items if has_text(item, "pdf_url_official") or has_text(item, "pdf_url_mirror")),
                "with_headnotes": sum(1 for item in division_items if has_text(item, "headnote_text")),
                "mirror_data_quality_errors": sum(1 for item in division_items if has_text(item, "last_error")),
                "most_common_action_type": action_counter.most_common(1)[0][0] if action_counter else "",
            }
        )

    language_by_division = []
    for division, division_items in divisions.items():
        counter = Counter(text_value(item, "language") or "(blank)" for item in division_items)
        for language, count in sorted(counter.items(), key=lambda pair: (-pair[1], pair[0].lower())):
            language_by_division.append({"division": division, "language": language, "count": count})

    document_language_counter: Counter[str] = Counter()
    for item in items:
        for doc in item.get("documents") or []:
            if isinstance(doc, dict):
                document_language_counter[text_value(doc, "language") or "(blank)"] += 1

    headnote_lengths = [len(text_value(item, "headnote_text")) for item in headnote_items]
    keyword_counter: Counter[str] = Counter()
    for item in items:
        keyword_counter.update(iter_keywords(item))

    references_by_item: defaultdict[str, list[dict[str, Any]]] = defaultdict(list)
    for item in items:
        for reference in extract_legal_references(item.get("headnote_text"), item.get("keywords_raw")):
            references_by_item[reference].append(item)
    reference_rows = [
        {
            "reference": reference,
            "count": len(reference_items),
            "examples": [item_label(item) for item in reference_items[:5]],
        }
        for reference, reference_items in references_by_item.items()
    ]

    adverse_groups: defaultdict[str, list[dict[str, Any]]] = defaultdict(list)
    pdf_hash_groups: defaultdict[str, list[dict[str, Any]]] = defaultdict(list)
    party_key_groups: defaultdict[str, list[dict[str, Any]]] = defaultdict(list)
    for item in items:
        adverse_groups[text_value(item, "adverse_pair_key")].append(item)
        pdf_hash_groups[text_value(item, "pdf_sha256")].append(item)
        party_key_groups[normalised_party_key(item)].append(item)

    same_parties_across_actions = []
    for key, grouped_items in party_key_groups.items():
        actions = {text_value(item, "type_of_action") for item in grouped_items if text_value(item, "type_of_action")}
        if key and len(grouped_items) > 1 and len(actions) > 1:
            same_parties_across_actions.append(
                {
                    "party_key": key,
                    "count": len(grouped_items),
                    "action_types": sorted(actions),
                    "items": [item_label(item) for item in grouped_items],
                }
            )

    adverse_pairs_across_case_numbers = []
    for key, grouped_items in adverse_groups.items():
        case_numbers = {text_value(item, "case_number") for item in grouped_items if text_value(item, "case_number")}
        registry_numbers = {text_value(item, "registry_number") for item in grouped_items if text_value(item, "registry_number")}
        if key and (len(case_numbers) > 1 or len(registry_numbers) > 1):
            adverse_pairs_across_case_numbers.append(
                {
                    "adverse_pair_key": key,
                    "caption": next((text_value(item, "primary_adverse_caption") for item in grouped_items if text_value(item, "primary_adverse_caption")), ""),
                    "count": len(grouped_items),
                    "case_numbers": sorted(case_numbers),
                    "registry_numbers": sorted(registry_numbers),
                    "items": [item_label(item) for item in grouped_items],
                }
            )

    claimant_counter: Counter[str] = Counter()
    defendant_counter: Counter[str] = Counter()
    party_counter: Counter[str] = Counter()
    multiple_claimants = []
    multiple_defendants = []
    for item in items:
        party_counter.update(party_names(item))
        claimants = parties_by_role(item, "claimant")
        defendants = parties_by_role(item, "defendant")
        claimant_counter.update(claimants)
        defendant_counter.update(defendants)
        if len(claimants) > 1:
            multiple_claimants.append({**item_label(item), "claimants": claimants})
        if len(defendants) > 1:
            multiple_defendants.append({**item_label(item), "defendants": defendants})

    duplicate_hash_rows = []
    for hash_value, grouped_items in pdf_hash_groups.items():
        if hash_value and len(grouped_items) > 1:
            duplicate_hash_rows.append(
                {
                    "pdf_sha256": hash_value,
                    "count": len(grouped_items),
                    "case_numbers": sorted({text_value(item, "case_number") for item in grouped_items if text_value(item, "case_number")}),
                    "action_types": sorted({text_value(item, "type_of_action") for item in grouped_items if text_value(item, "type_of_action")}),
                    "decision_dates": sorted({text_value(item, "decision_date") for item in grouped_items if text_value(item, "decision_date")}),
                    "items": [item_label(item) for item in grouped_items],
                }
            )
    duplicate_hash_rows = sorted(duplicate_hash_rows, key=lambda row: (-row["count"], row["pdf_sha256"]))[:20]

    lags = [lag for item in items if (lag := compute_observed_lag_days(item))]
    lag_days = [lag["days"] for lag in lags]
    suspicious_document_types = []
    for item in items:
        document_type = text_value(item, "document_type").lower()
        type_of_action = text_value(item, "type_of_action").lower()
        if not document_type:
            continue
        if document_type == type_of_action or (
            any(word in document_type for word in ACTION_WORDS)
            and document_type not in {"order", "decision", "final order", "procedural order"}
        ):
            suspicious_document_types.append(item_label(item))

    return {
        "generated_at": generated_at,
        "record_count": total,
        "upc_stats": {
            "headline": {
                "total_records": total,
                "total_unique_adverse_pairs": len(unique_adverse_pairs),
                "total_unique_parties": len(unique_parties),
                "total_unique_pdf_hashes": len(unique_pdf_hashes),
                "latest_decision_date": max(valid_dates).isoformat() if valid_dates else "",
                "earliest_decision_date": min(valid_dates).isoformat() if valid_dates else "",
                "latest_ingested_at": max((text_value(item, "ingested_at") for item in items if has_text(item, "ingested_at")), default=""),
                "generated_at": generated_at,
            },
            "activity": {
                "by_month": [{"value": key, "count": month_counter[key]} for key in sorted(month_counter, reverse=True)],
                "by_week": [{"value": key, "count": week_counter[key]} for key in sorted(week_counter, reverse=True)],
                "cumulative_by_month": cumulative,
                "busiest_decision_dates": sorted_count_rows(date_counter, limit=20),
            },
            "divisions": {
                "by_division": sorted(division_rows, key=lambda row: (-row["count"], row["division"].lower())),
                "by_court_level": sorted_count_rows(court_level_counter, total),
            },
            "types": {
                "by_document_type": sorted_count_rows(Counter(text_value(item, "document_type") or "(blank)" for item in items), total, 50),
                "by_title_raw": sorted_count_rows(Counter(text_value(item, "title_raw") or "(blank)" for item in items), total, 50),
                "by_type_of_action": sorted_count_rows(Counter(text_value(item, "type_of_action") or "(blank)" for item in items), total, 50),
                "type_of_action_by_month": _type_by_month(items),
            },
            "languages": {
                "item_languages": sorted_count_rows(Counter(text_value(item, "language") or "(blank)" for item in items), total),
                "document_languages": sorted_count_rows(document_language_counter, sum(document_language_counter.values())),
                "unknown_item_language_count": sum(1 for item in items if not has_text(item, "language")),
                "language_by_division": sorted(language_by_division, key=lambda row: (row["division"].lower(), -row["count"], row["language"].lower())),
            },
            "headnotes_keywords": {
                "items_with_headnote": len(headnote_items),
                "headnote_coverage_percent": percent(len(headnote_items), total),
                "items_with_keywords": len(keyword_items),
                "keyword_coverage_percent": percent(len(keyword_items), total),
                "average_headnote_length": round(mean(headnote_lengths), 1) if headnote_lengths else 0,
                "median_headnote_length": round(median(headnote_lengths), 1) if headnote_lengths else 0,
                "headnote_coverage_by_division": _coverage_by_division(divisions, "headnote_text"),
                "top_keywords": sorted_count_rows(keyword_counter, limit=50),
            },
            "legal_references": {
                "top_references": sorted(reference_rows, key=lambda row: (-row["count"], row["reference"].lower()))[:50],
            },
            "related_cases": {
                "adverse_pair_clusters": cluster_rows(adverse_groups, "adverse_pair_key"),
                "duplicate_pdf_hash_clusters": duplicate_hash_rows,
                "same_parties_across_action_types": sorted(same_parties_across_actions, key=lambda row: (-row["count"], row["party_key"]))[:20],
                "adverse_pairs_across_case_numbers": sorted(adverse_pairs_across_case_numbers, key=lambda row: (-row["count"], row["adverse_pair_key"]))[:20],
            },
            "parties": {
                "top_parties": sorted_count_rows(party_counter, limit=50),
                "top_claimants": sorted_count_rows(claimant_counter, limit=50),
                "top_defendants": sorted_count_rows(defendant_counter, limit=50),
                "cases_with_multiple_claimants": multiple_claimants[:20],
                "cases_with_multiple_defendants": multiple_defendants[:20],
                "metadata_party_quality_checks": suspicious_party_strings(items),
            },
        },
        "data_quality": {
            "headline": {
                "items_with_official_pdf": official_pdf_count,
                "items_with_mirror_pdf": mirror_pdf_count,
                "items_with_no_pdf": no_pdf_count,
                "items_with_headnote": len(headnote_items),
                "items_with_keywords": len(keyword_items),
                "items_with_last_error": len(error_items),
                "items_with_blank_division": sum(1 for item in items if not has_text(item, "division")),
                "items_with_blank_language": sum(1 for item in items if not has_text(item, "language")),
                "items_with_empty_documents": len(empty_documents),
            },
            "pdfs": {
                "items_with_no_official_pdf": [item_label(item) for item in items if not has_text(item, "pdf_url_official")][:50],
                "items_with_no_mirror_pdf": [item_label(item) for item in items if not has_text(item, "pdf_url_mirror")][:50],
                "items_with_empty_documents": [item_label(item) for item in empty_documents][:50],
                "duplicate_pdf_hashes": duplicate_hash_rows,
            },
            "errors": {
                "count": len(error_items),
                "items": [{**item_label(item), "last_error": text_value(item, "last_error")} for item in error_items][:50],
            },
            "blank_fields": {
                "blank_language": [item_label(item) for item in items if not has_text(item, "language")][:50],
                "blank_division": [item_label(item) for item in items if not has_text(item, "division")][:50],
                "blank_headnote": [item_label(item) for item in items if not has_text(item, "headnote_text")][:50],
                "blank_keywords": [item_label(item) for item in items if not list(iter_keywords(item))][:50],
            },
            "metadata_checks": {
                "document_type_looks_like_action_type": suspicious_document_types[:50],
                "invalid_or_missing_decision_dates": invalid_date_items[:50],
                "party_quality_checks": suspicious_party_strings(items),
            },
            "observed_lag": {
                "label": "Observed lag: decision date to first seen by mirror",
                "known_count": len(lags),
                "unknown_count": total - len(lags),
                "mean_days": round(mean(lag_days), 1) if lag_days else 0,
                "median_days": round(median(lag_days), 1) if lag_days else 0,
                "max_days": max(lag_days) if lag_days else 0,
                "top_slowest_items": sorted(lags, key=lambda row: row["days"], reverse=True)[:20],
            },
        },
    }


def _coverage_by_division(divisions: dict[str, list[dict[str, Any]]], field: str) -> list[dict[str, Any]]:
    rows = []
    for division, items in divisions.items():
        covered = sum(1 for item in items if has_text(item, field))
        rows.append(
            {
                "division": division,
                "count": len(items),
                "covered": covered,
                "coverage_percent": percent(covered, len(items)),
            }
        )
    return sorted(rows, key=lambda row: (-row["count"], row["division"].lower()))


def _type_by_month(items: list[dict[str, Any]]) -> list[dict[str, Any]]:
    counter: Counter[tuple[str, str]] = Counter()
    for item in items:
        parsed = parse_date_safe(item.get("decision_date"))
        if not parsed:
            continue
        counter[(parsed.strftime("%Y-%m"), text_value(item, "type_of_action") or "(blank)")] += 1
    return [
        {"month": month, "type_of_action": action, "count": count}
        for (month, action), count in sorted(counter.items(), key=lambda row: (row[0][0], -row[1], row[0][1].lower()), reverse=False)
    ]
