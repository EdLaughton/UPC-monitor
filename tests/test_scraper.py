from upc_ingester.scraper import build_index_url


def test_build_index_url_uses_bare_source_for_first_page() -> None:
    assert (
        build_index_url("https://www.unifiedpatentcourt.org/en/decisions-and-orders", 0)
        == "https://www.unifiedpatentcourt.org/en/decisions-and-orders"
    )


def test_build_index_url_only_adds_page_for_later_pages() -> None:
    assert (
        build_index_url("https://www.unifiedpatentcourt.org/en/decisions-and-orders", 1)
        == "https://www.unifiedpatentcourt.org/en/decisions-and-orders?page=1"
    )
