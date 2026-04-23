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

    public_dir = tmp_path / "public"
    render_outputs(db, public_dir)

    latest = json.loads((public_dir / "latest.json").read_text(encoding="utf-8"))
    html = (public_dir / "index.html").read_text(encoding="utf-8")

    assert latest["items"][0]["headnote_raw"].endswith("unique-tail")
    assert latest["items"][0]["documents"][0]["pdf_url_mirror"] == "/pdfs/2026/node-1/official.pdf"
    assert "unique-tail" not in html
    assert "/pdfs/2026/node-1/official.pdf" in html
