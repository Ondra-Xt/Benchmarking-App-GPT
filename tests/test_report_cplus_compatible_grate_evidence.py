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
                    "9010.88.61",
                    "aco-90108861",
                    "https://example.test/cplus/",
                    "ACO ShowerDrain C+ kompatibel mit Design-Rost 9010.88.61",
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
    assert set(df["grate_id"]) == {"aco-90108861"}
    assert set(df["compatibility_evidence_type"]) == {"page_level_family"}
    assert set(df["compatibility_confidence"]) == {"medium"}
    assert df["article_level_compatibility_found"].eq(False).all()
    assert df["safe_to_generate"].eq(False).all()
    assert df["ready_for_benchmark"].eq(False).all()
    assert df["ready_for_customer_view"].eq(False).all()
    assert df["production_status_note"].str.contains("diagnostic/evidence-only").all()
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
                "9010.88.61",
                "aco-90108861",
                "https://example.test/cplus/matrix",
                "ACO ShowerDrain C+ compatible Design-Rost Artikel 9010.88.61",
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
    assert all(mapping.ready_for_benchmark is True for mapping in mappings)
    assert all(mapping.ready_for_customer_view is False for mapping in mappings)


def test_export_matrix_detects_only_plausible_article_backed_c_grates():
    components = pd.DataFrame([
        {
            "product_id": f"aco-{article.replace('.', '')}",
            "product_family": "showerdrain_c_article_grate",
            "system_role": "grate",
            "article_no": article,
            "product_name": f"Design grate {article}",
            "source_url": "https://example.test/showerdrain-c/design-grates",
        }
        for article in mod.PLAUSIBLE_CPLUS_GRATE_ARTICLES
    ] + [
        {"product_id": "aco-90108544", "product_family": "showerdrain_c", "system_role": "base", "article_no": "9010.85.44"},
        {"product_id": "aco-assembled-bad", "product_family": "showerdrain_c_article_grate", "system_role": "grate", "article_no": "9010.88.61"},
        {"product_id": "aco-missing", "product_family": "showerdrain_c_article_grate", "system_role": "grate", "article_no": None},
        {"product_id": "aco-accessory", "product_family": "showerdrain_c_article_grate", "system_role": "accessory", "article_no": "9010.88.62"},
    ])

    evidence = mod.build_export_evidence_dataframe(_protected_products(), components)

    assert len(evidence) == 30
    assert set(evidence["base_id"]) == set(mod.PROTECTED_CPLUS_BASE_IDS)
    assert set(evidence["grate_article_number"]) == set(mod.PLAUSIBLE_CPLUS_GRATE_ARTICLES)
    assert set(evidence["compatibility_evidence_type"]) == {"inferred_from_shared_c_grate_page"}
    assert set(evidence["compatibility_confidence"]) == {"low"}
    assert evidence["safe_to_generate"].eq(False).all()
    assert evidence["ready_for_benchmark"].eq(False).all()
    assert evidence["ready_for_customer_view"].eq(False).all()
    assert evidence["source_text_or_reason"].str.contains("does not identify this grate article").all()


def test_diagnostic_mappings_do_not_create_production_cplus_assemblies_or_change_counts():
    products = _protected_products()
    base_rows, _missing = mod.locate_cplus_base_rows(products)
    mappings = mod.build_diagnostic_mappings(
        base_rows,
        [mod.CandidateEvidence("9010.88.61", "aco-90108861", "https://example.test/c", "Design-Rost passend für ShowerDrain C", "ambiguous", "ambiguous")],
    )
    cplus_df = pd.DataFrame([mapping.__dict__ for mapping in mappings])
    report = gaps.build_report(
        pd.DataFrame(),
        products,
        pd.DataFrame([{"product_id": "aco-90108861", "option_family": "showerdrain_cplus"}]),
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
