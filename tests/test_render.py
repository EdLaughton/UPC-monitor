import json
from pathlib import Path

from upc_ingester.db import Database
from upc_ingester.render import render_outputs


def test_render_outputs_preserve_full_json_and_preview_html(tmp_path: Path) -> None:
    db = Database(tmp_path / "upc.sqlite3")
    db.init()
    full_headnote = " ".join(["full-headnote"] * 80) + " unique-tail"
    decision_id = db.upsert_decision(
        {
            "item_key": "node-1",
            "title_raw": "Order",
            "case_name_raw": "A v. B",
            "parties_raw": "A\nv.\nB",
            "parties_json": [{"role": "claimant", "name": "A"}, {"role": "defendant", "name": "B"}],
            "party_names_all": ["A", "B"],
            "party_names_normalised": ["a", "b"],
            "primary_adverse_caption": "A v. B",
            "adverse_pair_key": "a :: b",
            "division": "Local Division",
            "panel": "Panel",
            "case_number": "UPC_CFI_1/2026",
            "registry_number": "ACT_1/2026",
            "order_or_decision_number": "ORD_1/2026",
            "decision_date": "2026-04-20",
            "document_type": "Order",
            "type_of_action": "Infringement action",
            "language": "English",
            "headnote_raw": full_headnote,
            "headnote_text": full_headnote,
            "keywords_raw": "Alpha; Beta; Gamma",
            "keywords_list": ["Alpha", "Beta", "Gamma"],
            "pdf_url_official": "https://example.test/official.pdf",
            "pdf_url_mirror": "/pdfs/2026/node-1/official.pdf",
            "node_url": "https://example.test/en/node/1",
            "pdf_sha256": "abc",
            "first_seen_at": "2026-04-23T00:00:00+00:00",
            "last_seen_at": "2026-04-23T00:00:00+00:00",
            "ingested_at": "2026-04-23T00:00:00+00:00",
            "alerted_at": "2026-04-23T00:00:00+00:00",
            "last_error": "",
        }
    )
    db.replace_documents(
        decision_id,
        [
            {
                "language": "EN",
                "pdf_url_official": "https://example.test/official.pdf",
                "pdf_url_mirror": "/pdfs/2026/node-1/official.pdf",
                "pdf_sha256": "abc",
                "file_path": "/data/public/pdfs/2026/node-1/official.pdf",
                "is_primary": True,
                "downloaded_at": "2026-04-23T00:00:00+00:00",
            }
        ],
    )
    db.upsert_decision(
        {
            "item_key": "node-2",
            "title_raw": "Later order same case",
            "case_name_raw": "A v. C",
            "parties_raw": "A v. C",
            "party_names_all": ["A", "C"],
            "party_names_normalised": ["a", "c"],
            "primary_adverse_caption": "A v. C",
            "adverse_pair_key": "a :: c",
            "division": "Local Division",
            "case_number": "UPC_CFI_1/2026",
            "registry_number": "ACT_2/2026",
            "decision_date": "2026-04-22",
            "document_type": "Order",
            "type_of_action": "Infringement action",
            "language": "English",
            "headnote_text": "Related by case",
            "keywords_raw": "Delta",
            "keywords_list": ["Delta"],
            "pdf_url_official": "https://example.test/official-2.pdf",
            "pdf_url_mirror": "/pdfs/2026/node-2/official-2.pdf",
            "node_url": "https://example.test/en/node/2",
            "first_seen_at": "2026-04-24T00:00:00+00:00",
            "last_seen_at": "2026-04-24T00:00:00+00:00",
            "ingested_at": "2026-04-24T00:00:00+00:00",
        }
    )
    db.upsert_decision(
        {
            "item_key": "node-3",
            "title_raw": "Same adverse pair",
            "case_name_raw": "A v. B",
            "parties_raw": "A v. B",
            "party_names_all": ["A", "B"],
            "party_names_normalised": ["a", "b"],
            "primary_adverse_caption": "A v. B",
            "adverse_pair_key": "a :: b",
            "division": "Central Division",
            "case_number": "UPC_CFI_3/2026",
            "registry_number": "ACT_3/2026",
            "decision_date": "2026-04-21",
            "document_type": "Decision",
            "type_of_action": "Revocation action",
            "language": "English",
            "headnote_text": "Related by adverse pair",
            "keywords_raw": "Epsilon",
            "keywords_list": ["Epsilon"],
            "pdf_url_official": "https://example.test/official-3.pdf",
            "pdf_url_mirror": "/pdfs/2026/node-3/official-3.pdf",
            "node_url": "https://example.test/en/node/3",
            "first_seen_at": "2026-04-25T00:00:00+00:00",
            "last_seen_at": "2026-04-25T00:00:00+00:00",
            "ingested_at": "2026-04-25T00:00:00+00:00",
        }
    )

    public_dir = tmp_path / "public"
    render_outputs(db, public_dir)

    latest = json.loads((public_dir / "latest.json").read_text(encoding="utf-8"))
    item_json = json.loads((public_dir / "items" / "node-1.json").read_text(encoding="utf-8"))
    related_json = json.loads((public_dir / "related" / "node-1.json").read_text(encoding="utf-8"))
    stats = json.loads((public_dir / "stats.json").read_text(encoding="utf-8"))
    html = (public_dir / "index.html").read_text(encoding="utf-8")
    stats_html = (public_dir / "stats.html").read_text(encoding="utf-8")

    latest_node_1 = next(item for item in latest["items"] if item["item_key"] == "node-1")
    assert latest_node_1["headnote_raw"].endswith("unique-tail")
    assert latest_node_1["documents"][0]["pdf_url_mirror"] == "/pdfs/2026/node-1/official.pdf"
    assert item_json["item_key"] == "node-1"
    assert item_json["node_url"] == "https://example.test/en/node/1"
    assert item_json["decision_date"] == "2026-04-20"
    assert item_json["case_number"] == "UPC_CFI_1/2026"
    assert item_json["registry_number"] == "ACT_1/2026"
    assert item_json["party_names_all"] == ["A", "B"]
    assert item_json["party_names_normalised"] == ["a", "b"]
    assert item_json["headnote_text"].endswith("unique-tail")
    assert item_json["mirror_pdf_url"] == "https://upc.edlaughton.uk/pdfs/2026/node-1/official.pdf"
    assert item_json["official_pdf_url"] == "https://example.test/official.pdf"
    assert item_json["local_item_html_url"] == "https://upc.edlaughton.uk/items/node-1.html"
    assert item_json["item_context_url"] == "https://upc.edlaughton.uk/items/node-1.json"
    assert item_json["related_context_url"] == "https://upc.edlaughton.uk/related/node-1.json"
    assert "watch_profile" not in item_json
    assert "matched_terms" not in item_json
    assert "private_reason" not in item_json
    assert related_json["current_item"]["item_key"] == "node-1"
    assert [item["item_key"] for item in related_json["same_case_or_registry"]] == ["node-2"]
    assert [item["item_key"] for item in related_json["same_adverse_party_pair"]] == ["node-3"]
    assert "node-3" in [item["item_key"] for item in related_json["same_normalised_parties"]]
    assert stats["record_count"] == 3
    assert "upc_stats" in stats
    assert "data_quality" in stats
    assert stats["upc_stats"]["headline"]["total_records"] == 3
    assert stats["data_quality"]["headline"]["items_with_mirror_pdf"] == 3
    assert "Statistics dashboard" in html
    assert "/stats.html" in html
    assert "/stats.json" in html
    assert "UPC Decisions Mirror Statistics" in stats_html
    assert "Scraper/Data-Quality Health" in stats_html
    assert "unique-tail" not in html
    assert "/pdfs/2026/node-1/official.pdf" in html
