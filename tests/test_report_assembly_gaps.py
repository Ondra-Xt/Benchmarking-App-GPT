import pandas as pd

import tools.report_assembly_gaps as mod


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

    rc = mod.main()
    out = capsys.readouterr().out

    assert rc == 0
    assert "showerdrain_splus: 1" in out
    assert "blocked_no_base_rows" in out
