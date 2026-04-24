from upc_ingester.stats import (
    build_stats,
    classify_court_level,
    compute_observed_lag_days,
    extract_legal_references,
    normalise_keyword,
    parse_date_safe,
)


def make_item(
    item_key: str,
    *,
    decision_date: str = "2026-04-23",
    first_seen_at: str = "2026-04-24T00:00:00+00:00",
    ingested_at: str = "2026-04-24T00:00:00+00:00",
    division: str = "Court of First Instance - Düsseldorf (DE) Local Division",
    document_type: str = "Order",
    title_raw: str = "Order",
    type_of_action: str = "Infringement Action",
    language: str = "German",
    parties_raw: str = "QUANTIFICARE S.A..\nv.\nCanfield Scientific GmbH a. o.",
    parties_json: list[dict[str, str]] | None = None,
    party_names_all: list[str] | None = None,
    party_names_normalised: list[str] | None = None,
    adverse_pair_key: str = "canfield scientific a o :: quantificare",
    primary_adverse_caption: str = "QUANTIFICARE S.A.. v. Canfield Scientific GmbH a. o.",
    case_number: str = "UPC_CFI_559/2024",
    registry_number: str = "ACT_1/2026",
    pdf_sha256: str = "same-hash",
    pdf_url_official: str = "https://example.test/order.pdf",
    pdf_url_mirror: str = "/pdfs/2026/order.pdf",
    headnote_text: str = "GERMAN: Art. 34 EPGÜ. ENGLISH: Art. 34 UPCA and R. 220.1 RoP.",
    keywords_raw: str = "GERMAN:\nArt. 34 EPGÜ; carve out\nENGLISH:\nArt. 34 UPCA; carve out",
    keywords_list: list[str] | None = None,
    documents: list[dict[str, object]] | None = None,
    last_error: str = "",
) -> dict[str, object]:
    if parties_json is None:
        parties_json = [
            {"role": "claimant", "name": "QUANTIFICARE S.A."},
            {"role": "defendant", "name": "Canfield Scientific GmbH a. o."},
        ]
    if party_names_all is None:
        party_names_all = ["QUANTIFICARE S.A.", "Canfield Scientific GmbH a. o."]
    if party_names_normalised is None:
        party_names_normalised = ["quantificare", "canfield scientific a o"]
    if keywords_list is None:
        keywords_list = ["GERMAN: Art. 34 EPGÜ; carve out ENGLISH: Art. 34 UPCA; carve out"]
    if documents is None:
        documents = [
            {
                "language": "DE",
                "pdf_url_official": pdf_url_official,
                "pdf_url_mirror": pdf_url_mirror,
                "pdf_sha256": pdf_sha256,
                "is_primary": True,
            }
        ]
    return {
        "item_key": item_key,
        "decision_date": decision_date,
        "first_seen_at": first_seen_at,
        "ingested_at": ingested_at,
        "division": division,
        "document_type": document_type,
        "title_raw": title_raw,
        "type_of_action": type_of_action,
        "language": language,
        "parties_raw": parties_raw,
        "parties_json": parties_json,
        "party_names_all": party_names_all,
        "party_names_normalised": party_names_normalised,
        "adverse_pair_key": adverse_pair_key,
        "primary_adverse_caption": primary_adverse_caption,
        "case_number": case_number,
        "registry_number": registry_number,
        "pdf_sha256": pdf_sha256,
        "pdf_url_official": pdf_url_official,
        "pdf_url_mirror": pdf_url_mirror,
        "headnote_text": headnote_text,
        "keywords_raw": keywords_raw,
        "keywords_list": keywords_list,
        "documents": documents,
        "last_error": last_error,
        "node_url": f"https://example.test/{item_key}",
    }


def test_parse_and_helper_functions_are_tolerant() -> None:
    assert parse_date_safe("2026-04-23") is not None
    assert parse_date_safe("not a date") is None
    assert classify_court_level("Court of Appeal - Luxembourg (LU)") == "Court of Appeal"
    assert classify_court_level("Court of First Instance - Paris (FR) Central Division - Seat") == "Court of First Instance - Central Division"
    assert normalise_keyword("GERMAN:\nArt. 34 EPGÜ; carve out\nENGLISH:\nArt. 34 UPCA; carve out") == [
        "Art. 34 EPGÜ",
        "carve out",
        "Art. 34 UPCA",
        "carve out",
    ]
    assert "Art. 34 UPCA" in extract_legal_references("Art. 34 UPCA and R. 220.1 RoP")
    assert compute_observed_lag_days(make_item("node-1"))["days"] == 1


def test_build_stats_separates_upc_statistics_from_data_quality() -> None:
    items = [
        make_item("node-183097", type_of_action="Infringement Action", case_number="UPC_CFI_559/2024"),
        make_item("node-183098", type_of_action="Counterclaim for revocation", case_number="UPC_CFI_106/2025"),
        make_item(
            "node-182999",
            division="Court of First Instance - Paris (FR) Central Division - Seat",
            type_of_action="Revocation Action",
            language="French",
            parties_raw="Huntsman Europe BV\nv.\nBASF SE",
            parties_json=[
                {"role": "claimant", "name": "Huntsman Europe BV"},
                {"role": "defendant", "name": "BASF SE"},
            ],
            party_names_all=["Huntsman Europe BV", "BASF SE"],
            party_names_normalised=["huntsman europe", "basf"],
            adverse_pair_key="basf :: huntsman europe",
            primary_adverse_caption="Huntsman Europe BV v. BASF SE",
            pdf_sha256="primary-fr",
            documents=[
                {
                    "language": "FR",
                    "pdf_url_official": "https://example.test/fr.pdf",
                    "pdf_url_mirror": "/pdfs/fr.pdf",
                    "pdf_sha256": "primary-fr",
                    "is_primary": True,
                },
                {
                    "language": "EN",
                    "pdf_url_official": "https://example.test/en.pdf",
                    "pdf_url_mirror": "/pdfs/en.pdf",
                    "pdf_sha256": "translation-en",
                    "is_primary": False,
                },
            ],
        ),
        make_item(
            "node-182987",
            decision_date="2026-04-01",
            first_seen_at="2026-04-23T23:00:00+00:00",
            division="Düsseldorf (DE) Local Division",
            document_type="Application RoP262A",
            title_raw="Application RoP262A",
            type_of_action="Application RoP262A",
            language="",
            parties_raw="Example Claimant and other\nv.\nExample Defendant",
            parties_json=[
                {"role": "claimant", "name": "Example Claimant and other"},
                {"role": "defendant", "name": "Example Defendant"},
            ],
            party_names_all=["Example Claimant and other", "Example Defendant"],
            party_names_normalised=["example claimant and other", "example defendant"],
            adverse_pair_key="example claimant and other :: example defendant",
            primary_adverse_caption="Example Claimant and other v. Example Defendant",
            case_number="UPC_CFI_999/2026",
            pdf_sha256="",
            pdf_url_official="",
            pdf_url_mirror="",
            headnote_text="",
            keywords_raw="",
            keywords_list=[],
            documents=[],
            last_error="detail fetch/parse failed",
        ),
        make_item(
            "node-invalid",
            decision_date="not a date",
            language="English",
            parties_json=[
                {"role": "claimant", "name": "A"},
                {"role": "claimant", "name": "B"},
                {"role": "defendant", "name": "C"},
                {"role": "defendant", "name": "D"},
            ],
            party_names_all=["A", "B", "C", "D"],
            party_names_normalised=["a", "b", "c", "d"],
            adverse_pair_key="",
            primary_adverse_caption="",
            pdf_sha256="unique",
        ),
    ]

    stats = build_stats(items, generated_at="2026-04-24T00:00:00+00:00")

    assert stats["record_count"] == 5
    assert "upc_stats" in stats
    assert "data_quality" in stats
    assert stats["upc_stats"]["headline"]["total_unique_pdf_hashes"] == 3
    assert stats["data_quality"]["headline"]["items_with_last_error"] == 1
    assert stats["data_quality"]["headline"]["items_with_empty_documents"] == 1
    assert stats["data_quality"]["headline"]["items_with_blank_language"] == 1
    assert stats["data_quality"]["observed_lag"]["label"] == "Observed lag: decision date to first seen by mirror"
    assert stats["data_quality"]["observed_lag"]["max_days"] == 22

    duplicate_hashes = stats["upc_stats"]["related_cases"]["duplicate_pdf_hash_clusters"]
    assert duplicate_hashes[0]["pdf_sha256"] == "same-hash"
    assert duplicate_hashes[0]["count"] == 2

    adverse_pairs = stats["upc_stats"]["related_cases"]["adverse_pair_clusters"]
    assert adverse_pairs[0]["adverse_pair_key"] == "canfield scientific a o :: quantificare"
    assert set(adverse_pairs[0]["action_types"]) == {"Counterclaim for revocation", "Infringement Action"}

    references = {row["reference"] for row in stats["upc_stats"]["legal_references"]["top_references"]}
    assert "Art. 34 UPCA" in references
    assert "Art. 34 EPGÜ" in references
    assert "R. 220.1 RoP" in references

    keywords = {row["value"] for row in stats["upc_stats"]["headnotes_keywords"]["top_keywords"]}
    assert "Art. 34 UPCA" in keywords
    assert "carve out" in keywords

    quality_items = stats["data_quality"]["metadata_checks"]["party_quality_checks"]
    assert {row["item_key"] for row in quality_items} >= {"node-183097", "node-182987"}
    assert stats["data_quality"]["metadata_checks"]["invalid_or_missing_decision_dates"][0]["item_key"] == "node-invalid"
    assert stats["upc_stats"]["parties"]["cases_with_multiple_claimants"][0]["item_key"] == "node-invalid"
    assert stats["upc_stats"]["parties"]["cases_with_multiple_defendants"][0]["item_key"] == "node-invalid"
