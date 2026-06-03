import pandas as pd

import tools.report_eplus_proposal_mappings as mod
from tools.diagnose_eplus_base_row_sources import EPlusCandidateRow

BODY_IDS = (
    "aco-showerdrain-eplus-rinnenkoerper-einbauhoehe-oberkante-estrich-25-128-mm",
    "aco-showerdrain-eplus-rinnenkoerper-einbauhoehe-oberkante-estrich-57-128-mm",
    "aco-showerdrain-eplus-rinnenkoerper-einbauhoehe-oberkante-estrich-80-128-mm-din-en-1253-1",
)
BODY_URLS = (
    "https://www.aco-haustechnik.de/produkte/badentwaesserung/duschrinnen/aco-showerdrain-eplus/rinnenkoerper-einbauhoehe-oberkante-estrich-25-128-mm/",
    "https://www.aco-haustechnik.de/produkte/badentwaesserung/duschrinnen/aco-showerdrain-eplus/rinnenkoerper-einbauhoehe-oberkante-estrich-57-128-mm/",
    "https://www.aco-haustechnik.de/produkte/badentwaesserung/duschrinnen/aco-showerdrain-eplus/rinnenkoerper-einbauhoehe-oberkante-estrich-80-128-mm-din-en-1253-1/",
)
GRATE_ID = "aco-showerdrain-eplus-design-roste-aus-elektropoliertem-edelstahl"
GRATE_URL = "https://www.aco-haustechnik.de/produkte/badentwaesserung/duschrinnen/aco-showerdrain-eplus/design-roste-aus-elektropoliertem-edelstahl/"
EXPECTED_SET_IDS = [f"diagnostic-eplus-{body_id}__{GRATE_ID}" for body_id in BODY_IDS]


def _body(body_id, url, height_min):
    return EPlusCandidateRow(
        article_number="",
        proposed_product_id=body_id,
        candidate_type="eplus_page_level_body_evidence",
        product_family="showerdrain_eplus",
        system_role="drain_unit",
        source_url=url,
        row_text="page-level body evidence",
        flow_rate_lps="0.70",
        water_seal_mm="50",
        outlet_dn="DN50",
        height_adj_min_mm=height_min,
        height_adj_max_mm="128",
        evidence_type="source_page_level_body_url",
        confidence="high",
    )


def _grate():
    return EPlusCandidateRow(
        article_number="",
        proposed_product_id=GRATE_ID,
        candidate_type="eplus_grate_component",
        product_family="showerdrain_eplus",
        system_role="grate",
        source_url=GRATE_URL,
        row_text="page-level grate evidence",
        evidence_type="source_page_level_grate_url",
        confidence="high",
    )


def _proposals():
    bodies = tuple(_body(body_id, url, height) for body_id, url, height in zip(BODY_IDS, BODY_URLS, ("25", "57", "80")))
    return mod.build_proposed_mappings(bodies, (_grate(),))


def test_build_proposed_mappings_has_required_eplus_proposal_only_rows():
    df = _proposals()

    assert list(df.columns) == list(mod.PROPOSAL_COLUMNS)
    assert len(df) == 3
    assert list(df["set_id"]) == EXPECTED_SET_IDS
    assert set(df["product_family"]) == {"showerdrain_eplus"}
    assert set(df["assembly_model"]) == {"base_x_grate"}
    assert list(df["body_id"]) == list(BODY_IDS)
    assert set(df["grate_id"]) == {GRATE_ID}
    assert set(df["body_article_number"]) == {""}
    assert set(df["grate_article_number"]) == {""}
    assert set(df["flow_rate_lps"]) == {"0.70"}
    assert set(df["water_seal_mm"]) == {"50"}
    assert set(df["outlet_dn"]) == {"DN50"}
    assert list(df["height_adj_min_mm"]) == ["25", "57", "80"]
    assert set(df["height_adj_max_mm"]) == {"128"}
    assert set(df["body_evidence_type"]) == {"source_page_level_body_url"}
    assert set(df["body_confidence"]) == {"high"}
    assert set(df["grate_evidence_type"]) == {"source_page_level_grate_url"}
    assert set(df["grate_confidence"]) == {"high"}
    assert set(df["compatibility_evidence_type"]) == {"page_level_family_bom_or_inferred_from_current_bom"}
    assert set(df["compatibility_confidence"]) == {"medium"}
    assert set(df["article_level_compatibility_found"]) == {False}
    assert set(df["data_quality_status"]) == {"proposal_only_partial"}
    assert all("explicit_article_level_base_to_grate_compatibility" in value for value in df["missing_evidence"])
    assert set(df["safe_to_generate"]) == {False}
    assert set(df["ready_for_benchmark"]) == {False}
    assert set(df["ready_for_customer_view"]) == {False}
    assert all("no explicit article-level base-to-grate compatibility matrix" in value for value in df["blocking_reason"])
    assert all("collect explicit article-level E+ base-to-grate compatibility before production generation" in value for value in df["recommended_next_action"])
    assert all("diagnostic/proposal-only; no Products/BOM/assembly generation change" in value for value in df["production_status_note"])
    assert not any("http" in set_id.lower() or "https" in set_id.lower() for set_id in df["set_id"])


def test_risk_checks_pass_for_clean_proposal_only_state():
    df = _proposals()
    risks = {risk.name: risk for risk in mod.build_risk_checks(
        df,
        products=pd.DataFrame([{"product_id": "existing"}]),
        final_assemblies=pd.DataFrame([{"product_id": "assembled"}]),
        final_set_details=pd.DataFrame([{"set_id": "set", "assembled_product_id": "assembled"}]),
    )}

    assert all(risk.passed for risk in risks.values())
    assert risks["duplicate proposed set IDs"].detail == "none"
    assert risks["proposed IDs already in Products"].detail == "none"
    assert risks["proposed IDs already in Final_Assemblies"].detail == "none"
    assert risks["proposed IDs already in Final_Set_Details"].detail == "none"
    assert risks["production behavior changed"].detail == "no"


def test_risk_checks_detect_invalid_or_production_overlapping_proposals():
    df = _proposals()
    risky_id = "https://example.test/diagnostic-eplus-risk"
    df.loc[0, "set_id"] = risky_id
    df.loc[1, "set_id"] = risky_id
    df.loc[0, "body_id"] = ""
    df.loc[1, "grate_id"] = ""
    df.loc[2, "flow_rate_lps"] = ""
    df.loc[0, "body_source_url"] = ""
    df.loc[1, "grate_source_url"] = ""
    df.loc[0, "article_level_compatibility_found"] = True
    df.loc[0, "safe_to_generate"] = True
    df.loc[1, "ready_for_benchmark"] = True
    df.loc[2, "ready_for_customer_view"] = True

    risks = {risk.name: risk for risk in mod.build_risk_checks(
        df,
        products=pd.DataFrame([{"product_id": risky_id}]),
        final_assemblies=pd.DataFrame([{"product_id": risky_id}]),
        final_set_details=pd.DataFrame([{"set_id": risky_id, "assembled_product_id": "other"}]),
    )}

    assert risks["duplicate proposed set IDs"].passed is False
    assert risks["missing body_id"].passed is False
    assert risks["missing grate_id"].passed is False
    assert risks["URL-bearing set IDs"].passed is False
    assert risks["proposed IDs already in Products"].passed is False
    assert risks["proposed IDs already in Final_Assemblies"].passed is False
    assert risks["proposed IDs already in Final_Set_Details"].passed is False
    assert risks["missing hydraulic body fields"].passed is False
    assert risks["missing body source URL"].passed is False
    assert risks["missing grate source URL"].passed is False
    assert risks["article_level_compatibility_found unexpectedly True"].passed is False
    assert risks["safe_to_generate unexpectedly True"].passed is False
    assert risks["ready_for_benchmark unexpectedly True"].passed is False
    assert risks["ready_for_customer_view unexpectedly True"].passed is False


def test_build_report_reuses_diagnostic_helper_and_keeps_production_blocked(monkeypatch):
    class FakeReadiness:
        eplus_page_level_body_rows = 3
        eplus_article_level_body_candidates = 0
        eplus_article_level_body_incomplete = 0
        eplus_grate_candidates = 1
        hydraulic_complete_candidates = 0
        likely_assembly_model = "base_x_grate"
        safe_to_generate_eplus_assemblies = False

    class FakeOutputRows:
        products = ()
        comparison = ()
        candidates_all = tuple({"product_id": f"candidate-{idx}"} for idx in range(6))
        components = tuple({"product_id": f"component-{idx}"} for idx in range(6))
        bom_options = tuple({"product_id": f"bom-{idx}"} for idx in range(10))
        final_assemblies = ()
        final_set_details = ()

    class FakeDiagnostic:
        output_rows = FakeOutputRows()
        page_level_body_evidence = tuple(_body(body_id, url, height) for body_id, url, height in zip(BODY_IDS, BODY_URLS, ("25", "57", "80")))
        grate_candidates = (_grate(),)
        readiness = FakeReadiness()
        production_behavior_changed = False

    monkeypatch.setattr(mod.eplus_sources, "build_diagnostic", lambda *args, **kwargs: FakeDiagnostic())
    report = mod.build_report(
        candidates_all=pd.DataFrame([{"product_id": "candidate"}] * 118),
        products=pd.DataFrame([{"product_id": "existing"}] * 46),
        comparison=pd.DataFrame([{"product_id": "existing"}] * 46),
        components=pd.DataFrame([{"product_id": "component"}] * 100),
        bom_options=pd.DataFrame([{"product_id": "bom"}] * 221),
        final_assemblies=pd.DataFrame([{"product_id": "assembled"}] * 28),
        final_set_details=pd.DataFrame([{"set_id": "set", "assembled_product_id": "assembled"}] * 28),
    )

    assert report.input_frame_counts == {
        "Products": 46,
        "Comparison": 46,
        "Candidates_All": 118,
        "Components": 100,
        "BOM_Options": 221,
        "Final_Assemblies": 28,
        "Final_Set_Details": 28,
    }
    assert report.proposed_mapping_count == 3
    assert report.safe_to_generate_count == 0
    assert report.blocked_count == 3
    assert report.production_ready is False
    assert report.production_behavior_changed is False
    assert list(report.proposed_mappings["set_id"]) == EXPECTED_SET_IDS
    assert all(risk.passed for risk in report.risk_checks)


def test_build_current_report_runs_discovery_and_pipeline_without_writing_xlsx(monkeypatch):
    calls = []

    def fake_discover_candidates(target_length_mm, tolerance_mm):
        calls.append((target_length_mm, tolerance_mm))
        return ([{"product_id": "candidate"}], [])

    def fake_run_update(candidates_all, config):
        return (
            pd.DataFrame([{"product_id": "existing"}]),
            pd.DataFrame([{"product_id": "existing"}]),
            pd.DataFrame([{"product_id": "component"}]),
            pd.DataFrame(),
            pd.DataFrame([{"product_id": "bom"}]),
        )

    class FakeReadiness:
        eplus_page_level_body_rows = 0
        eplus_article_level_body_candidates = 0
        eplus_article_level_body_incomplete = 0
        eplus_grate_candidates = 0
        hydraulic_complete_candidates = 0
        likely_assembly_model = "unknown"
        safe_to_generate_eplus_assemblies = False

    class FakeOutputRows:
        products = ()
        comparison = ()
        candidates_all = ()
        components = ()
        bom_options = ()
        final_assemblies = ()
        final_set_details = ()

    class FakeDiagnostic:
        output_rows = FakeOutputRows()
        page_level_body_evidence = ()
        grate_candidates = ()
        readiness = FakeReadiness()
        production_behavior_changed = False

    monkeypatch.setattr(mod.aco, "discover_candidates", fake_discover_candidates)
    monkeypatch.setattr(mod.pipeline, "run_update", fake_run_update)
    monkeypatch.setattr(mod.eplus_sources, "build_diagnostic", lambda *args, **kwargs: FakeDiagnostic())

    report = mod.build_current_report()

    assert calls == [(1200, 100)]
    assert report.input_frame_counts["Candidates_All"] == 1
    assert report.proposed_mapping_count == 0
    assert report.production_behavior_changed is False


def test_print_report_includes_required_cli_sections(monkeypatch, capsys):
    df = _proposals()

    class FakeReadiness:
        eplus_page_level_body_rows = 3
        eplus_article_level_body_candidates = 0
        eplus_article_level_body_incomplete = 0
        eplus_grate_candidates = 1
        hydraulic_complete_candidates = 0
        likely_assembly_model = "base_x_grate"
        safe_to_generate_eplus_assemblies = False

    class FakeOutputRows:
        products = ()
        comparison = ()
        candidates_all = tuple({"product_id": f"candidate-{idx}"} for idx in range(6))
        components = tuple({"product_id": f"component-{idx}"} for idx in range(6))
        bom_options = tuple({"product_id": f"bom-{idx}"} for idx in range(10))
        final_assemblies = ()
        final_set_details = ()

    class FakeDiagnostic:
        output_rows = FakeOutputRows()
        readiness = FakeReadiness()

    report = mod.EPlusProposalReport(
        input_frame_counts={
            "Products": 46,
            "Comparison": 46,
            "Candidates_All": 118,
            "Components": 100,
            "BOM_Options": 221,
            "Final_Assemblies": 28,
            "Final_Set_Details": 28,
        },
        diagnostic=FakeDiagnostic(),
        proposed_mappings=df,
        risk_checks=mod.build_risk_checks(df, pd.DataFrame(), pd.DataFrame(), pd.DataFrame()),
        proposed_mapping_count=3,
        safe_to_generate_count=0,
        blocked_count=3,
        production_ready=False,
        blocking_reason=mod.BLOCKING_REASON,
        recommended_next_action=mod.RECOMMENDED_NEXT_ACTION,
    )

    mod.print_report(report)
    out = capsys.readouterr().out

    assert "Input frame counts" in out
    assert "Current E+ diagnostic evidence summary" in out
    assert "Proposed E+ mapping table" in out
    assert "Risk checks" in out
    assert "Readiness summary" in out
    assert "proposed_mappings = 3" in out
    assert "safe_to_generate = 0" in out
    assert "blocked = 3" in out
    assert "production_ready = False" in out
    assert "no explicit article-level base-to-grate compatibility matrix" in out
    assert "collect explicit article-level E+ base-to-grate compatibility before production generation" in out
    assert "production behavior changed: no" in out
    assert EXPECTED_SET_IDS[0] in out
