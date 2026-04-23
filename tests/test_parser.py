from pathlib import Path

from upc_ingester.parser import extract_last_page, is_failure_page, parse_detail_page, parse_index_page


FIXTURES = Path(__file__).parent / "fixtures"


def test_parse_index_rows_by_headers() -> None:
    html = (FIXTURES / "index.html").read_text(encoding="utf-8")
    items = parse_index_page(html, "https://www.unifiedpatentcourt.org/en/decisions-and-orders")

    assert len(items) == 1
    item = items[0]
    assert item.item_key == "node-123456"
    assert item.node_url == "https://www.unifiedpatentcourt.org/en/node/123456"
    assert item.decision_date == "2026-04-15"
    assert item.registry_number == "ACT_10138/2026"
    assert item.order_or_decision_number == "ORD_10339/2026"
    assert item.division == "Local Division Paris"
    assert item.type_of_action == "Infringement action"
    assert extract_last_page(html) == 4


def test_parse_index_rows_with_sort_text_in_headers() -> None:
    html = """
    <table>
      <thead>
        <tr>
          <th>Date <span>Sort ascending</span></th>
          <th>Registry number/Order reference/Case number</th>
          <th>Court</th>
          <th>Type of action</th>
          <th>Parties</th>
          <th>UPC Document</th>
        </tr>
      </thead>
      <tbody>
        <tr>
          <td>23 April 2026</td>
          <td>UPC_CFI_106/2025<br><a href="/en/node/183098">Full Details</a></td>
          <td>Dusseldorf (DE) Local Division</td>
          <td>Counterclaim for revocation</td>
          <td>QUANTIFICARE S.A..<br>v.<br>Canfield Scientific GmbH a. o.</td>
          <td><a href="/sites/default/files/files/api_order/example.pdf">PDF</a></td>
        </tr>
      </tbody>
    </table>
    """
    item = parse_index_page(html, "https://www.unifiedpatentcourt.org/en/decisions-and-orders")[0]

    assert item.decision_date == "2026-04-23"
    assert item.case_number == "UPC_CFI_106/2025"
    assert item.division == "Dusseldorf (DE) Local Division"
    assert item.type_of_action == "Counterclaim for revocation"
    assert item.parties_raw.startswith("QUANTIFICARE")


def test_parse_detail_page_full_metadata_and_multiple_pdfs() -> None:
    html = (FIXTURES / "detail.html").read_text(encoding="utf-8")
    detail = parse_detail_page(html, "https://www.unifiedpatentcourt.org/en/node/123456")

    assert detail.title_raw == "Order of the Court of First Instance"
    assert detail.case_number == "UPC_CFI_101/2026"
    assert detail.registry_number == "ACT_10138/2026"
    assert detail.order_or_decision_number == "ORD_10339/2026"
    assert detail.decision_date == "2026-04-15"
    assert detail.language == "English"
    assert "must not be shortened" in detail.headnote_raw
    assert detail.keywords_list == ["Rule 212 RoP", "Preliminary injunction", "Urgency"]
    assert len(detail.pdf_links) == 2
    assert detail.pdf_links[0].language == "EN"
    assert detail.pdf_links[1].url.endswith("ORD_10339_2026_DE.pdf")


def test_parse_detail_page_tolerates_empty_headnotes_and_keywords() -> None:
    html = """
    <html><body>
      <h1>Decision</h1>
      <div>Case number</div><div>UPC_CFI_1/2026</div>
      <h2>Order Documents</h2>
      <a href="/doc.pdf">PDF</a>
    </body></html>
    """
    detail = parse_detail_page(html, "https://example.test/en/node/1")

    assert detail.case_number == "UPC_CFI_1/2026"
    assert detail.headnote_raw == ""
    assert detail.keywords_list == []
    assert detail.pdf_links[0].url == "https://example.test/doc.pdf"


def test_cloudflare_preconnect_alone_is_not_failure_page() -> None:
    assert not is_failure_page('<link rel="preconnect" href="https://challenges.cloudflare.com">')
    assert is_failure_page("<html><title>Just a moment...</title><script>window._cf_chl_opt={}</script>")
