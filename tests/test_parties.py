from upc_ingester.parties import normalise_name, parse_parties, split_party_side


def test_parse_adverse_parties_with_roles() -> None:
    result = parse_parties("Alexion Pharmaceuticals, Inc.\nv.\nSamsung Bioepis Co., Ltd.")

    assert result.parties_json == [
        {"role": "claimant", "name": "Alexion Pharmaceuticals, Inc."},
        {"role": "defendant", "name": "Samsung Bioepis Co., Ltd."},
    ]
    assert result.primary_adverse_caption == "Alexion Pharmaceuticals, Inc. v. Samsung Bioepis Co., Ltd."
    assert "alexion pharmaceuticals" in result.adverse_pair_key
    assert "samsung bioepis" in result.adverse_pair_key


def test_multiple_company_names_split_without_breaking_suffix_commas() -> None:
    names = split_party_side("Kodak Graphic Communications GmbH, Kodak Holding GmbH, Alexion Pharmaceuticals, Inc.")

    assert names == [
        "Kodak Graphic Communications GmbH",
        "Kodak Holding GmbH",
        "Alexion Pharmaceuticals, Inc.",
    ]


def test_single_party_application_and_normalisation() -> None:
    result = parse_parties("Müller Pharma GmbH")

    assert result.parties_json == [{"role": "party", "name": "Müller Pharma GmbH"}]
    assert result.primary_adverse_caption == ""
    assert result.party_names_normalised == ["muller pharma"]
    assert normalise_name("Example Holdings B.V.") == "example"
