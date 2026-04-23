from pathlib import Path

from upc_ingester.parser import (
    extract_last_page,
    extract_next_page_url,
    is_failure_page,
    parse_detail_page,
    parse_index_page,
)


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


def test_extract_next_page_url_from_upc_pager() -> None:
    html = """
    <nav class="pager" role="navigation" aria-labelledby="pagination-heading">
      <ul class="pager__items js-pager__items">
        <li class="pager__item is-active">
          <a href="?judgement_type=All&amp;court_type=All&amp;division_1=125&amp;division_2=126&amp;division_3=139&amp;division_4=223&amp;proceedings_lang=All&amp;page=0" title="Current page" aria-current="page">1</a>
        </li>
        <li class="pager__item">
          <a href="?judgement_type=All&amp;court_type=All&amp;division_1=125&amp;division_2=126&amp;division_3=139&amp;division_4=223&amp;proceedings_lang=All&amp;page=1" title="Go to page 2">2</a>
        </li>
        <li class="pager__item pager__item--next">
          <a href="?judgement_type=All&amp;court_type=All&amp;division_1=125&amp;division_2=126&amp;division_3=139&amp;division_4=223&amp;proceedings_lang=All&amp;page=1" title="Go to next page" rel="next">Next</a>
        </li>
      </ul>
    </nav>
    """

    assert extract_next_page_url(html, "https://www.unifiedpatentcourt.org/en/decisions-and-orders") == (
        "https://www.unifiedpatentcourt.org/en/decisions-and-orders?"
        "judgement_type=All&court_type=All&division_1=125&division_2=126&division_3=139&"
        "division_4=223&proceedings_lang=All&page=1"
    )


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


def test_parse_detail_page_tolerates_empty_headnotes_keywords_and_missing_official_pdf() -> None:
    html = """
    <html><body>
      <h1>Decision</h1>
      <div>Case number</div><div>UPC_CFI_1/2026</div>
      <h2>Order Documents</h2>
      <a href="/doc.pdf">Generic PDF that must not be treated as an official UPC decision PDF</a>
    </body></html>
    """
    detail = parse_detail_page(html, "https://www.unifiedpatentcourt.org/en/node/1")

    assert detail.case_number == "UPC_CFI_1/2026"
    assert detail.headnote_raw == ""
    assert detail.keywords_list == []
    assert detail.pdf_links == []


def test_cloudflare_preconnect_alone_is_not_failure_page() -> None:
    assert not is_failure_page('<link rel="preconnect" href="https://challenges.cloudflare.com">')
    assert is_failure_page("<html><title>Just a moment...</title><script>window._cf_chl_opt={}</script>")
