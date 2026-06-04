import pandas as pd

import tools.report_cplus_compatible_grate_evidence as mod
import tools.report_assembly_gaps as gaps


def _protected_products():
    return pd.DataFrame([
        {
            "product_id": "aco-showerdrain-cplus-standard-h92",
            "product_family": "showerdrain_cplus",
            "flow_rate_lps": 0.91,
            "water_seal_mm": 50,
            "outlet_dn": "DN50",
            "height_adj_min_mm": 80,
            "height_adj_max_mm": 128,
            "source_url": "https://example.test/cplus/standard",
        },
        {
            "product_id": "aco-showerdrain-cplus-low-h69",
            "product_family": "showerdrain_cplus",
            "flow_rate_lps": 0.62,
            "water_seal_mm": 25,
            "outlet_dn": "DN40",
            "height_adj_min_mm": 57,
            "height_adj_max_mm": 128,
            "source_url": "https://example.test/cplus/low",
        },
    ])


def test_finds_two_protected_bases_and_preserves_hydraulic_fields():
    rows, missing = mod.locate_cplus_base_rows(_protected_products())

    assert missing == ()
    assert set(rows) == set(mod.PROTECTED_CPLUS_BASE_IDS)
    assert rows["aco-showerdrain-cplus-standard-h92"]["flow_rate_lps"] == "0.91"
    assert rows["aco-showerdrain-cplus-standard-h92"]["outlet_dn"] == "DN50"
    assert rows["aco-showerdrain-cplus-standard-h92"]["water_seal_mm"] == "50"
    assert rows["aco-showerdrain-cplus-standard-h92"]["height_adj_min_mm"] == "80"
    assert rows["aco-showerdrain-cplus-standard-h92"]["height_adj_max_mm"] == "128"
    assert rows["aco-showerdrain-cplus-low-h69"]["flow_rate_lps"] == "0.62"
    assert rows["aco-showerdrain-cplus-low-h69"]["outlet_dn"] == "DN40"
    assert rows["aco-showerdrain-cplus-low-h69"]["water_seal_mm"] == "25"
    assert rows["aco-showerdrain-cplus-low-h69"]["height_adj_min_mm"] == "57"
    assert rows["aco-showerdrain-cplus-low-h69"]["height_adj_max_mm"] == "128"


def test_detects_candidate_grate_rows_but_keeps_proposal_only_for_family_page_evidence(monkeypatch):
    def fake_find(source_urls, *frames):
        return (
            (mod.SourceInspection("https://example.test/cplus/", "ok", ("ACO ShowerDrain C+ with Design-Rost",)),),
            (
                mod.CandidateEvidence(
                    "9010.88.01",
                    "aco-90108801",
                    "https://example.test/cplus/",
                    "ACO ShowerDrain C+ kompatibel mit Design-Rost 9010.88.01",
                    "explicit_family_level",
                    "explicit",
                ),
            ),
        )

    monkeypatch.setattr(mod, "find_candidate_evidence", fake_find)
    diag = mod.build_diagnostic(pd.DataFrame(), _protected_products(), pd.DataFrame(), pd.DataFrame(), pd.DataFrame())
    df = mod.diagnostic_mappings_dataframe(diag)

    assert len(diag.diagnostic_mappings) == 2
    assert set(df["base_id"]) == set(mod.PROTECTED_CPLUS_BASE_IDS)
    assert set(df["grate_id"]) == {"aco-90108801"}
    assert set(df["compatibility_evidence_type"]) == {"page_level_family"}
    assert set(df["compatibility_confidence"]) == {"medium"}
    assert df["article_level_compatibility_found"].eq(False).all()
    assert df["safe_to_generate"].eq(False).all()
    assert df["ready_for_benchmark"].eq(False).all()
    assert df["ready_for_customer_view"].eq(False).all()
    assert df["production_status_note"].str.contains("diagnostic/proposal-only").all()
    assert diag.safe_to_add_compatible_grate_bom_rows is False
    assert "Do not add" in diag.recommendation


def test_missing_grate_evidence_emits_base_level_missing_rows():
    base_rows, _missing = mod.locate_cplus_base_rows(_protected_products())
    mappings = mod.build_diagnostic_mappings(base_rows, [])

    assert len(mappings) == 2
    assert all(mapping.compatibility_evidence_type == "missing" for mapping in mappings)
    assert all(mapping.grate_id == "" for mapping in mappings)
    assert all(mapping.safe_to_generate is False for mapping in mappings)
    assert all(mapping.ready_for_benchmark is False for mapping in mappings)
    assert all(mapping.ready_for_customer_view is False for mapping in mappings)


def test_explicit_article_evidence_is_only_case_safe_to_generate_but_still_not_ready_views():
    base_rows, _missing = mod.locate_cplus_base_rows(_protected_products())
    mappings = mod.build_diagnostic_mappings(
        base_rows,
        [
            mod.CandidateEvidence(
                "9010.88.01",
                "aco-90108801",
                "https://example.test/cplus/matrix",
                "ACO ShowerDrain C+ compatible Design-Rost Artikel 9010.88.01",
                "explicit_article_matrix",
                "explicit",
            )
        ],
    )

    assert len(mappings) == 2
    assert all(mapping.compatibility_evidence_type == "article_level_explicit" for mapping in mappings)
    assert all(mapping.compatibility_confidence == "high" for mapping in mappings)
    assert all(mapping.article_level_compatibility_found is True for mapping in mappings)
    assert all(mapping.safe_to_generate is True for mapping in mappings)
    assert all(mapping.ready_for_benchmark is False for mapping in mappings)
    assert all(mapping.ready_for_customer_view is False for mapping in mappings)


def test_diagnostic_mappings_do_not_create_production_cplus_assemblies_or_change_counts():
    products = _protected_products()
    base_rows, _missing = mod.locate_cplus_base_rows(products)
    mappings = mod.build_diagnostic_mappings(
        base_rows,
        [mod.CandidateEvidence("9010.88.01", "aco-90108801", "https://example.test/c", "Design-Rost passend für ShowerDrain C", "ambiguous", "ambiguous")],
    )
    cplus_df = pd.DataFrame([mapping.__dict__ for mapping in mappings])
    report = gaps.build_report(
        pd.DataFrame(),
        products,
        pd.DataFrame([{"product_id": "aco-90108801", "option_family": "showerdrain_cplus"}]),
        pd.DataFrame(),
        final_assemblies=pd.DataFrame(),
        final_set_details=pd.DataFrame(),
        cplus_compatible_grate_evidence=cplus_df,
    )
    by_family = {gap.family: gap for gap in report.families}

    assert report.sheet_counts["Products"] == 2
    assert report.sheet_counts["Final_Assemblies"] == 0
    assert report.sheet_counts["Final_Set_Details"] == 0
    assert by_family["showerdrain_cplus"].current_assembled_count == 0
    assert by_family["showerdrain_cplus"].compatible_grate_bom_rows_found == 0
    assert by_family["showerdrain_cplus"].status == "blocked_proposal_only_compatibility_evidence"
    assert by_family["showerdrain_cplus"].proposal_only_mapping_count == 2
    assert by_family["showerdrain_cplus"].proposal_safe_to_generate_count == 0


def test_candidate_grate_filter_excludes_assembled_body_and_nan_rows():
    evidence = [
        mod.CandidateEvidence(
            "9010.88.61",
            "aco-assembled-showerdrain-c-standard__aco-90108861",
            "https://www.aco.example/showerdrain-c/design-roste-aus-geschliffenem-edelstahl/",
            "Design-Rost Wave 9010.88.61",
            "ambiguous",
            "ambiguous",
        ),
        mod.CandidateEvidence(
            "9010.85.24",
            "aco-90108524",
            "https://www.aco.example/showerdrain-c/design-roste-aus-geschliffenem-edelstahl/",
            "Rinnenkörper ACO ShowerDrain C 9010.85.24",
            "ambiguous",
            "ambiguous",
        ),
        mod.CandidateEvidence(
            "nan",
            "aco-90108899",
            "https://www.aco.example/showerdrain-c/design-roste-aus-geschliffenem-edelstahl/",
            "Design-Rost Artikel nan",
            "ambiguous",
            "ambiguous",
        ),
        mod.CandidateEvidence(
            "9010.88.61",
            "aco-90108861",
            "https://www.aco.example/showerdrain-c/design-roste-aus-geschliffenem-edelstahl/",
            "Design-Rost Wave 9010.88.61",
            "ambiguous",
            "ambiguous",
        ),
        mod.CandidateEvidence(
            "9010.88.62",
            "",
            "https://www.aco.example/showerdrain-c/design-roste-aus-geschliffenem-edelstahl/",
            "Wave 9010.88.62",
            "ambiguous",
            "ambiguous",
        ),
    ]

    rows = mod._candidate_grate_rows(evidence)

    assert [(row.product_id, row.article_number) for row in rows] == [
        ("aco-90108861", "9010.88.61"),
        ("", "9010.88.62"),
    ]


def test_candidate_rows_from_frames_keeps_only_plausible_design_grates():
    frames = [
        pd.DataFrame([
            {
                "product_id": "aco-assembled-showerdrain-c-standard__aco-90108861",
                "article_no": "9010.88.61",
                "system_role": "grate",
                "product_name": "Assembled ShowerDrain C with Design-Rost",
                "source_url": "https://www.aco.example/showerdrain-c/design-roste-aus-geschliffenem-edelstahl/",
            },
            {
                "product_id": "aco-90108534",
                "article_no": "9010.85.34",
                "system_role": "body",
                "product_name": "Rinnenkörper ACO ShowerDrain C",
                "source_url": "https://www.aco.example/showerdrain-c/design-roste-aus-geschliffenem-edelstahl/",
            },
            {
                "product_id": "aco-90108862",
                "article_no": "nan",
                "system_role": "grate",
                "product_name": "Design-Rost with missing article attribution",
                "source_url": "https://www.aco.example/showerdrain-c/design-roste-aus-geschliffenem-edelstahl/",
            },
            {
                "product_id": "aco-90108863",
                "article_no": "9010.88.63",
                "system_role": "grate",
                "product_name": "Design-Rost Wave",
                "source_url": "https://www.aco.example/showerdrain-c/design-roste-aus-geschliffenem-edelstahl/",
            },
        ])
    ]

    candidates = mod._candidate_rows_from_frames(frames)

    assert set(candidates) == {"aco-90108863"}
    assert candidates["aco-90108863"]["article_number"] == "9010.88.63"


def test_find_candidate_evidence_filters_source_rows_to_design_grate_articles(monkeypatch):
    html = """
    <html><body><table>
      <tr><td>Design-Rost Wave</td><td>9010.88.61</td></tr>
      <tr><td>Design-Rost Square</td><td>9010.88.62</td></tr>
      <tr><td>Rinnenkörper ShowerDrain C body</td><td>9010.85.24</td></tr>
      <tr><td>Assembled ShowerDrain C set</td><td>9010.85.34</td></tr>
      <tr><td>Design-Rost broken attribution</td><td>nan</td></tr>
    </table></body></html>
    """

    def fake_get(url, timeout=35):
        return 200, url, html, None

    monkeypatch.setattr(mod.aco, "_safe_get_text", fake_get)

    _inspections, evidence = mod.find_candidate_evidence([
        "https://www.aco.example/showerdrain-c/design-roste-aus-geschliffenem-edelstahl/"
    ])
    rows = mod._candidate_grate_rows(evidence)

    assert [row.article_number for row in rows] == ["9010.88.61", "9010.88.62"]
    assert all(row.evidence_type == "ambiguous" for row in rows)
    assert not any(row.article_number.startswith("9010.85") for row in rows)
    assert not any(row.article_number == "nan" for row in rows)


def test_expected_showerdrain_c_design_grate_articles_remain_included():
    expected_articles = {
        "9010.88.61",
        "9010.88.62",
        "9010.88.63",
        "9010.88.64",
        "9010.88.66",
        "9010.88.68",
        "9010.88.69",
        "9010.88.70",
        "9010.88.71",
        "9010.88.73",
        "9010.88.89",
        "9010.88.90",
        "9010.88.91",
        "9010.88.92",
        "9010.88.94",
    }
    evidence = [
        mod.CandidateEvidence(
            article,
            f"aco-{article.replace('.', '')}",
            "https://www.aco.example/showerdrain-c/design-roste-aus-geschliffenem-edelstahl/",
            f"Design-Rost candidate {article}",
            "ambiguous",
            "ambiguous",
        )
        for article in sorted(expected_articles)
    ]

    rows = mod._candidate_grate_rows(evidence)

    assert {row.article_number for row in rows} == expected_articles
    assert all(row.evidence_type == "ambiguous" for row in rows)
