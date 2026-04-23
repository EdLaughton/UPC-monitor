from __future__ import annotations

import re
import unicodedata
from dataclasses import dataclass


SUFFIX_FRAGMENTS = {
    "inc",
    "inc.",
    "ltd",
    "ltd.",
    "llc",
    "corp",
    "corp.",
    "co",
    "co.",
    "s.a",
    "s.a.",
    "sasu",
    "s.p.a",
    "s.p.a.",
    "s.r.l",
    "s.r.l.",
    "sp. z o. o",
    "sp. z o.o.",
}

LEGAL_SUFFIXES = (
    "incorporated",
    "corporation",
    "company",
    "limited",
    "holdings",
    "holding",
    "gmbh",
    "inc",
    "ltd",
    "llc",
    "corp",
    "b v",
    "n v",
    "s a",
    "sas",
    "sasu",
    "ag",
    "kg",
    "oy",
    "ab",
    "aps",
    "plc",
    "s p a",
    "s r l",
)


@dataclass(frozen=True)
class PartyParseResult:
    parties_json: list[dict[str, str]]
    party_names_all: list[str]
    party_names_normalised: list[str]
    primary_adverse_caption: str
    adverse_pair_key: str


def _strip_accents(value: str) -> str:
    normalised = unicodedata.normalize("NFKD", value)
    return "".join(ch for ch in normalised if not unicodedata.combining(ch))


def normalise_name(value: str) -> str:
    value = _strip_accents(value).lower()
    value = value.replace("&", " and ")
    value = re.sub(r"[^a-z0-9]+", " ", value)
    words = value.split()
    suffixes = set(LEGAL_SUFFIXES)
    while words:
        removed = False
        for width in (3, 2, 1):
            if len(words) >= width and " ".join(words[-width:]) in suffixes:
                del words[-width:]
                removed = True
                break
        if not removed:
            break
    words = [word for word in words if word not in suffixes]
    return " ".join(words).strip()


def _looks_like_suffix_fragment(value: str) -> bool:
    return value.strip().lower().rstrip(",") in SUFFIX_FRAGMENTS


def _ends_like_complete_name(value: str) -> bool:
    value = re.sub(r"[^a-z0-9. ]+", "", value.lower()).strip()
    endings = (
        "gmbh",
        " b.v",
        " b.v.",
        " n.v",
        " n.v.",
        " ag",
        " kg",
        " llc",
        " ltd",
        " ltd.",
        " limited",
        " inc.",
        " inc",
        " corporation",
        " corp.",
        " corp",
        " s.a.",
        " s.a",
        " sas",
        " sasu",
        " oy",
        " ab",
        " aps",
        " s.p.a.",
        " s.r.l.",
    )
    return value.endswith(endings)


def split_party_side(value: str) -> list[str]:
    lines = [line.strip(" ,") for line in re.split(r"[\r\n]+", value) if line.strip(" ,")]
    if len(lines) > 1:
        return lines

    value = lines[0] if lines else value.strip()
    if not value:
        return []

    tokens = [token.strip() for token in re.split(r",\s+", value) if token.strip()]
    if len(tokens) == 1:
        return tokens

    names: list[str] = []
    current = tokens[0]
    for token in tokens[1:]:
        if _looks_like_suffix_fragment(token) or not _ends_like_complete_name(current):
            current = f"{current}, {token}"
        else:
            names.append(current.strip(" ,"))
            current = token
    if current:
        names.append(current.strip(" ,"))
    return names


def _split_adverse_sides(raw: str) -> tuple[list[str], list[str]]:
    lines = [line.strip() for line in raw.replace("\r", "\n").split("\n") if line.strip()]
    for index, line in enumerate(lines):
        if re.fullmatch(r"v\.?|vs\.?|versus", line, flags=re.I):
            return split_party_side("\n".join(lines[:index])), split_party_side("\n".join(lines[index + 1 :]))

    match = re.search(r"\s+v\.?\s+", raw, flags=re.I)
    if match:
        return split_party_side(raw[: match.start()]), split_party_side(raw[match.end() :])

    return split_party_side(raw), []


def parse_parties(raw: str) -> PartyParseResult:
    claimants, defendants = _split_adverse_sides(raw or "")
    parties_json: list[dict[str, str]] = []
    if defendants:
        for name in claimants:
            parties_json.append({"role": "claimant", "name": name})
        for name in defendants:
            parties_json.append({"role": "defendant", "name": name})
    else:
        for name in claimants:
            parties_json.append({"role": "party", "name": name})

    if not parties_json and raw.strip():
        parties_json.append({"role": "party", "name": raw.strip()})

    party_names_all = [party["name"] for party in parties_json]
    party_names_normalised = sorted({normalise_name(name) for name in party_names_all if normalise_name(name)})

    primary_adverse_caption = ""
    adverse_pair_key = ""
    if claimants and defendants:
        primary_adverse_caption = f"{claimants[0]} v. {defendants[0]}"
        pair = sorted([normalise_name(claimants[0]), normalise_name(defendants[0])])
        adverse_pair_key = " :: ".join(pair)

    return PartyParseResult(
        parties_json=parties_json,
        party_names_all=party_names_all,
        party_names_normalised=party_names_normalised,
        primary_adverse_caption=primary_adverse_caption,
        adverse_pair_key=adverse_pair_key,
    )
