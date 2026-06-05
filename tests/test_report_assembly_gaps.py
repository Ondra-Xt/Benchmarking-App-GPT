import pandas as pd

import tools.report_assembly_gaps as mod


def _mplus_compound_mappings_dataframe():
    return pd.DataFrame([
        {
            "product_family": "showerdrain_mplus",
            "assembly_model": "channel_body_x_drain_body_x_grate",
            "drain_body_article_number": article,
            "flow_rate_lps": "",
            "flow_rate_lps_10mm_head": 0.4,
            "flow_rate_lps_20mm_head": 0.46,
            "selected_default_flow_rate_lps": "",
            "flow_policy": "split_fields_only",
            "safe_to_generate": False,
            "ready_for_benchmark": False,
            "ready_for_customer_view": False,
            "blocking_reason": "blocked_pending_conditional_parameter_scoring",
        }
        for article in ("9010.81.20", "9010.81.21", "9010.81.22", "9010.81.23")
    ])


def _eplus_proposal_mappings_dataframe():
    return pd.DataFrame([
        {
            "product_family": "showerdrain_eplus",
            "assembly_model": "base_x_grate",
            "body_id": body_id,
            "grate_id": "aco-showerdrain-eplus-design-roste-aus-elektropoliertem-edelstahl",
            "flow_rate_lps": 0.70,
            "water_seal_mm": 50,
            "outlet_dn": "DN50",
            "height_adj_min_mm": height_min,
            "height_adj_max_mm": 128,
            "article_level_compatibility_found": False,
            "data_quality_status": "proposal_only_partial",
            "safe_to_generate": False,
            "ready_for_benchmark": False,
            "ready_for_customer_view": False,
            "blocking_reason": "no explicit article-level base-to-grate compatibility matrix",
            "recommended_next_action": "collect explicit article-level E+ base-to-grate compatibility before production generation",
        }
        for body_id, height_min in (
            ("aco-showerdrain-eplus-rinnenkoerper-einbauhoehe-oberkante-estrich-25-128-mm", 25),
            ("aco-showerdrain-eplus-rinnenkoerper-einbauhoehe-oberkante-estrich-57-128-mm", 57),
            ("aco-showerdrain-eplus-rinnenkoerper-einbauhoehe-oberkante-estrich-80-128-mm-din-en-1253-1", 80),
        )
    ])


def test_build_report_classifies_active_ready_and_blocked_families():
    candidates = pd.DataFrame([
        {"product_id": "aco-cplus-base", "product_family": "showerdrain_cplus", "product_url": "https://example.test/cplus"},
        {"product_id": "aco-mplus-base", "product_family": "showerdrain_mplus"},
        {"product_id": "aco-eplus-base", "product_family": "showerdrain_eplus"},
        {"product_id": "aco-b-base", "product_family": "showerdrain_b"},
        {"product_id": "aco-other-base", "product_family": "linear_x"},
    ])
    products = pd.DataFrame([
        {"product_id": "aco-assembled-showerdrain-splus-a__grate", "product_family": "showerdrain_splus", "flow_rate_lps": 0.8, "water_seal_mm": 50, "outlet_dn": "DN50", "height_adj_min_mm": 90, "height_adj_max_mm": 120},
        {"product_id": "aco-assembled-showerdrain-c-a__grate", "product_family": "showerdrain_c", "flow_rate_lps": 0.7, "water_seal_mm": 50, "outlet_dn": "DN50", "height_adj_min_mm": 80, "height_adj_max_mm": 110},
        {"product_id": "aco-assembled-easyflow-a__grate", "product_family": "easyflow", "flow_rate_lps": 0.6, "water_seal_mm": 50, "outlet_dn": "DN50", "height_adj_min_mm": 70, "height_adj_max_mm": 100},
        {"product_id": "aco-assembled-easyflowplus-a__grate", "product_family": "easyflowplus", "flow_rate_lps": 0.6, "water_seal_mm": 50, "outlet_dn": "DN50", "height_adj_min_mm": 70, "height_adj_max_mm": 100},
        {"product_id": "aco-cplus-base", "product_family": "showerdrain_cplus", "flow_rate_lps": 0.8, "water_seal_mm": 50, "outlet_dn": "DN50", "height_adj_min_mm": 90, "height_adj_max_mm": 120, "source_url": "https://example.test/cplus/base"},
        {"product_id": "aco-mplus-base", "product_family": "showerdrain_mplus", "flow_rate_lps": 0.8, "water_seal_mm": 50, "outlet_dn": "DN50", "height_adj_min_mm": 90, "height_adj_max_mm": 120},
        {"product_id": "aco-eplus-base", "product_family": "showerdrain_eplus", "flow_rate_lps": "", "water_seal_mm": 50, "outlet_dn": "DN50", "height_adj_min_mm": 90, "height_adj_max_mm": 120},
        {"product_id": "aco-b-base", "product_family": "showerdrain_b", "flow_rate_lps": 0.4, "water_seal_mm": 30, "outlet_dn": "DN50", "height_adj_min_mm": 55, "height_adj_max_mm": 95},
        {"product_id": "aco-other-base", "product_family": "linear_x", "flow_rate_lps": 0.5, "water_seal_mm": 30, "outlet_dn": "DN50", "height_adj_min_mm": 55, "height_adj_max_mm": 95},
    ])
    components = pd.DataFrame([
        {"product_id": "aco-cplus-grate", "option_family": "showerdrain_cplus", "product_url": "https://example.test/cplus/grate"},
        {"product_id": "aco-mplus-unused", "option_family": "showerdrain_mplus"},
        {"product_id": "aco-b-grate", "option_family": "showerdrain_b"},
        {"product_id": "aco-other-grate", "option_family": "linear_x"},
    ])
    bom = pd.DataFrame([
        {"product_id": "aco-cplus-base", "parent_family": "showerdrain_cplus", "component_id": "aco-cplus-grate", "option_family": "showerdrain_cplus", "option_type": "compatible_grate", "source_url": "https://example.test/cplus/bom"},
        {"product_id": "aco-cplus-base", "parent_family": "showerdrain_cplus", "component_id": "aco-cplus-legs", "option_family": "showerdrain_cplus", "option_type": "optional_accessory"},
        {"product_id": "aco-mplus-base", "parent_family": "showerdrain_mplus", "component_id": "aco-mplus-missing-grate", "option_family": "showerdrain_mplus", "option_type": "compatible_grate"},
        {"product_id": "aco-b-base", "parent_family": "showerdrain_b", "component_id": "aco-b-grate", "option_family": "showerdrain_b", "option_type": "optional_accessory"},
        {"product_id": "aco-other-base", "parent_family": "linear_x", "component_id": "aco-other-grate", "option_family": "linear_x", "option_type": "compatible_grate"},
    ])

    report = mod.build_report(
        candidates,
        products,
        components,
        bom,
        final_assemblies=pd.DataFrame([{"product_id": "aco-assembled-showerdrain-splus-a__grate"}]),
        final_set_details=pd.DataFrame(),
        article_variants=pd.DataFrame(),
    )
    by_family = {gap.family: gap for gap in report.families}

    assert report.current_assembled_counts == {
        "showerdrain_splus": 1,
        "showerdrain_c": 1,
        "easyflow": 1,
        "easyflowplus": 1,
        "showerdrain_mplus": 0,
    }
    assert by_family["showerdrain_splus"].status == "already_active"
    assert by_family["showerdrain_cplus"].status == "ready_candidate"
    assert by_family["showerdrain_cplus"].proposed_assembled_product_count == 1
    assert by_family["showerdrain_cplus"].optional_accessory_rows_found == 1
    assert by_family["showerdrain_mplus"].status == "blocked_no_valid_components"
    assert by_family["showerdrain_mplus"].missing_component_ids == ("aco-mplus-missing-grate",)
    assert by_family["showerdrain_mplus"].dangling_component_ids == ("aco-mplus-unused",)
    assert by_family["showerdrain_eplus"].status == "blocked_incomplete_hydraulic_data"
    assert by_family["showerdrain_b"].status == "blocked_no_compatible_grate_evidence"
    assert by_family["linear_x"].status == "ready_candidate"
    assert set(report.ready_candidate_families) == {"showerdrain_cplus", "linear_x"}


def test_build_report_accounts_for_mplus_proposal_only_mappings(capsys):
    products = pd.DataFrame([
        {"product_id": "aco-assembled-showerdrain-splus-a__grate", "product_family": "showerdrain_splus", "flow_rate_lps": 0.8, "water_seal_mm": 50, "outlet_dn": "DN50", "height_adj_min_mm": 90, "height_adj_max_mm": 120},
        {"product_id": "aco-assembled-showerdrain-c-a__grate", "product_family": "showerdrain_c", "flow_rate_lps": 0.7, "water_seal_mm": 50, "outlet_dn": "DN50", "height_adj_min_mm": 80, "height_adj_max_mm": 110},
        {"product_id": "aco-assembled-easyflow-a__grate", "product_family": "easyflow", "flow_rate_lps": 0.6, "water_seal_mm": 50, "outlet_dn": "DN50", "height_adj_min_mm": 70, "height_adj_max_mm": 100},
        {"product_id": "aco-assembled-easyflowplus-a__grate", "product_family": "easyflowplus", "flow_rate_lps": 0.6, "water_seal_mm": 50, "outlet_dn": "DN50", "height_adj_min_mm": 70, "height_adj_max_mm": 100},
        {"product_id": "aco-cplus-base", "product_family": "showerdrain_cplus", "flow_rate_lps": 0.8, "water_seal_mm": 50, "outlet_dn": "DN50", "height_adj_min_mm": 90, "height_adj_max_mm": 120},
    ])
    components = pd.DataFrame([
        {"product_id": "aco-cplus-grate", "option_family": "showerdrain_cplus"},
    ])
    bom = pd.DataFrame([
        {"product_id": "aco-cplus-base", "parent_family": "showerdrain_cplus", "component_id": "aco-cplus-grate", "option_family": "showerdrain_cplus", "option_type": "optional_accessory"},
    ])

    report = mod.build_report(
        pd.DataFrame(),
        products,
        components,
        bom,
        final_assemblies=pd.DataFrame(),
        final_set_details=pd.DataFrame(),
        article_variants=pd.DataFrame(),
        mplus_compound_mappings=_mplus_compound_mappings_dataframe(),
    )
    by_family = {gap.family: gap for gap in report.families}
    mplus = by_family["showerdrain_mplus"]

    assert report.sheet_counts["Mplus_Compound_Mappings"] == 4
    assert mplus.current_assembled_count == 0
    assert mplus.status == "blocked_pending_conditional_parameter_scoring"
    assert mplus.proposal_only_mapping_count == 4
    assert mplus.proposal_assembly_model == "channel_body_x_drain_body_x_grate"
    assert mplus.proposal_safe_to_generate_count == 0
    assert mplus.proposal_blocked_count == 4
    assert mplus.proposal_flow_policy == "split_fields_only"
    assert mplus.proposal_flow_rate_lps_10mm_head == "0.4"
    assert mplus.proposal_flow_rate_lps_20mm_head == "0.46"
    assert mplus.proposal_selected_default_flow_rate_lps == ""
    assert mplus.proposal_blocking_reason == "blocked_pending_conditional_parameter_scoring"
    assert mplus.next_required_action == "implement scoring/export handling for conditional parameter values before production M+ assemblies"
    assert "showerdrain_mplus" not in report.ready_candidate_families
    assert report.proposal_only_mapping_families == ("showerdrain_mplus",)
    assert by_family["showerdrain_cplus"].status == "blocked_no_compatible_grate_evidence"
    assert by_family["showerdrain_cplus"].next_required_action == "find explicit C+ compatible grate evidence / article matrix"

    mod.print_report(report)
    out = capsys.readouterr().out
    assert "- Mplus_Compound_Mappings: 4" in out
    assert "showerdrain_mplus | 0 | 0 | 0 | 0 | 0 | 0 | blocked_pending_conditional_parameter_scoring | 0 | 4 | implement scoring/export handling for conditional parameter values" in out
    assert "Ready candidate families:\n- none" in out
    assert "Missing component IDs by family" in out
    assert "Diagnostic unmatched component IDs by family" in out
    assert "not the XLSX validator aco_dangling_component_id check" in out
    assert "Dangling component IDs by family" not in out
    assert "Proposal-only diagnostic mappings:" in out
    assert "- showerdrain_mplus: 4 mappings, safe_to_generate=0, blocked=4, reason=blocked_pending_conditional_parameter_scoring" in out
    assert "assembly_model=channel_body_x_drain_body_x_grate" in out
    assert "flow_policy=split_fields_only" in out
    assert "flow_rate_lps_10mm_head=0.4" in out
    assert "flow_rate_lps_20mm_head=0.46" in out
    assert "selected_default_flow_rate_lps=empty" in out
    assert "Production behavior changed: no" in out


def test_build_report_accounts_for_eplus_proposal_only_mappings(capsys):
    products = pd.DataFrame([
        {"product_id": "aco-assembled-showerdrain-splus-a__grate", "product_family": "showerdrain_splus", "flow_rate_lps": 0.8, "water_seal_mm": 50, "outlet_dn": "DN50", "height_adj_min_mm": 90, "height_adj_max_mm": 120},
        {"product_id": "aco-assembled-showerdrain-c-a__grate", "product_family": "showerdrain_c", "flow_rate_lps": 0.7, "water_seal_mm": 50, "outlet_dn": "DN50", "height_adj_min_mm": 80, "height_adj_max_mm": 110},
        {"product_id": "aco-assembled-easyflow-a__grate", "product_family": "easyflow", "flow_rate_lps": 0.6, "water_seal_mm": 50, "outlet_dn": "DN50", "height_adj_min_mm": 70, "height_adj_max_mm": 100},
        {"product_id": "aco-assembled-easyflowplus-a__grate", "product_family": "easyflowplus", "flow_rate_lps": 0.6, "water_seal_mm": 50, "outlet_dn": "DN50", "height_adj_min_mm": 70, "height_adj_max_mm": 100},
        {"product_id": "aco-cplus-base", "product_family": "showerdrain_cplus", "flow_rate_lps": 0.8, "water_seal_mm": 50, "outlet_dn": "DN50", "height_adj_min_mm": 90, "height_adj_max_mm": 120},
    ])
    components = pd.DataFrame([
        {"product_id": "aco-cplus-grate", "option_family": "showerdrain_cplus"},
    ])
    bom = pd.DataFrame([
        {"product_id": "aco-cplus-base", "parent_family": "showerdrain_cplus", "component_id": "aco-cplus-grate", "option_family": "showerdrain_cplus", "option_type": "optional_accessory"},
    ])

    report = mod.build_report(
        pd.DataFrame(),
        products,
        components,
        bom,
        final_assemblies=pd.DataFrame(),
        final_set_details=pd.DataFrame(),
        article_variants=pd.DataFrame(),
        mplus_compound_mappings=_mplus_compound_mappings_dataframe(),
        eplus_proposal_mappings=_eplus_proposal_mappings_dataframe(),
    )
    by_family = {gap.family: gap for gap in report.families}
    eplus = by_family["showerdrain_eplus"]

    assert report.sheet_counts["Eplus_Proposal_Mappings"] == 3
    assert eplus.current_assembled_count == 0
    assert eplus.status == "blocked_proposal_only_compatibility_evidence"
    assert eplus.proposal_only_mapping_count == 3
    assert eplus.proposal_assembly_model == "base_x_grate"
    assert eplus.proposal_safe_to_generate_count == 0
    assert eplus.proposal_blocked_count == 3
    assert eplus.proposal_article_level_compatibility_found == "False"
    assert eplus.proposal_data_quality_status == "proposal_only_partial"
    assert eplus.proposal_blocking_reason == "no explicit article-level base-to-grate compatibility matrix"
    assert eplus.next_required_action == "collect explicit article-level E+ base-to-grate compatibility before production generation"
    assert "showerdrain_eplus" not in report.ready_candidate_families
    assert report.proposal_only_mapping_families == ("showerdrain_mplus", "showerdrain_eplus")
    assert by_family["showerdrain_mplus"].status == "blocked_pending_conditional_parameter_scoring"
    assert by_family["showerdrain_cplus"].status == "blocked_no_compatible_grate_evidence"
    assert by_family["showerdrain_cplus"].next_required_action == "find explicit C+ compatible grate evidence / article matrix"

    mod.print_report(report)
    out = capsys.readouterr().out
    assert "- Eplus_Proposal_Mappings: 3" in out
    assert "showerdrain_eplus | 0 | 0 | 0 | 0 | 0 | 0 | blocked_proposal_only_compatibility_evidence | 0 | 3 | collect explicit article-level E+ base-to-grate compatibility" in out
    assert "Ready candidate families:\n- none" in out
    assert "Missing component IDs by family" in out
    assert "Diagnostic unmatched component IDs by family" in out
    assert "not the XLSX validator aco_dangling_component_id check" in out
    assert "Dangling component IDs by family" not in out
    assert "- showerdrain_mplus: 4 mappings, safe_to_generate=0, blocked=4, reason=blocked_pending_conditional_parameter_scoring" in out
    assert "- showerdrain_eplus: 3 mappings, safe_to_generate=0, blocked=3, reason=no explicit article-level base-to-grate compatibility matrix" in out
    assert "assembly_model=base_x_grate" in out
    assert "article_level_compatibility_found=False" in out
    assert "data_quality_status=proposal_only_partial" in out
    assert "Production behavior changed: no" in out


def test_build_report_blocks_article_variant_ambiguity_and_prints(monkeypatch, capsys):
    products = pd.DataFrame([
        {"product_id": "aco-cplus-base", "product_family": "showerdrain_cplus", "flow_rate_lps": 0.8, "water_seal_mm": 50, "outlet_dn": "DN50", "height_adj_min_mm": 90, "height_adj_max_mm": 120},
    ])
    components = pd.DataFrame([
        {"product_id": "aco-cplus-grate", "option_family": "showerdrain_cplus"},
    ])
    bom = pd.DataFrame([
        {"product_id": "aco-cplus-base", "parent_family": "showerdrain_cplus", "component_id": "aco-cplus-grate", "option_family": "showerdrain_cplus", "option_type": "compatible_grate"},
    ])
    variants = pd.DataFrame([
        {"product_family": "showerdrain_cplus", "variant_type": "candidate_body_variant", "attribution_status": "pending_attribution_resolution", "why_not_promoted": "pending_attribution_resolution"},
    ])

    report = mod.build_report(pd.DataFrame(), products, components, bom, article_variants=variants)
    by_family = {gap.family: gap for gap in report.families}
    assert by_family["showerdrain_cplus"].status == "blocked_article_variant_ambiguous"
    assert "keep blocked because ambiguity remains" in by_family["showerdrain_cplus"].next_required_action

    mod.print_report(report)
    out = capsys.readouterr().out
    assert "ACO assembly gap diagnostic" in out
    assert "showerdrain_cplus | 0 | 1 | 1 | 1 | 1 | 0 | blocked_article_variant_ambiguous" in out
    assert "Production behavior changed: no" in out


def test_main_runs_discovery_and_pipeline_without_writing_xlsx(monkeypatch, capsys):
    products = pd.DataFrame([
        {"product_id": "aco-assembled-showerdrain-splus-a__grate", "product_family": "showerdrain_splus", "flow_rate_lps": 0.8, "water_seal_mm": 50, "outlet_dn": "DN50", "height_adj_min_mm": 90, "height_adj_max_mm": 120},
    ])
    monkeypatch.setattr(mod.aco, "discover_candidates", lambda target_length_mm, tolerance_mm: ([{"product_id": "x", "product_family": "showerdrain_splus"}], {"ok": True}))
    monkeypatch.setattr(mod.pipeline, "run_update", lambda registry, cfg: (products, pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), pd.DataFrame()))
    monkeypatch.setattr(mod.excel_export, "_extract_mplus_compound_mappings", lambda *args: pd.DataFrame())
    monkeypatch.setattr(mod.excel_export, "_extract_eplus_proposal_mappings", lambda *args: pd.DataFrame())

    rc = mod.main()
    out = capsys.readouterr().out

    assert rc == 0
    assert "showerdrain_splus: 1" in out
    assert "blocked_no_base_rows" in out
