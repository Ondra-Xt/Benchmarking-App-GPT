import pandas as pd

import tools.report_mplus_production_readiness_gate as mod


def _mplus_rows():
    rows = []
    defaults = {
        "9010.81.20": ("50", "DN40/DN50"),
        "9010.81.21": ("30", "DN40/DN50"),
        "9010.81.22": ("25", "DN40"),
        "9010.81.23": ("50", "DN50"),
    }
    for article, (ws, dn) in defaults.items():
        drain_id = f"aco-{article.replace('.', '')}"
        rows.append(
            {
                "set_id": f"diagnostic-mplus-channel-body-25-128__{drain_id}__mplus-design-roste-elektropoliert",
                "drain_body_article_number": article,
                "channel_body_id": "channel-body-25-128",
                "drain_body_id": drain_id,
                "grate_id": "mplus-design-roste-elektropoliert",
                "water_seal_mm": ws,
                "outlet_dn": dn,
                "height_adj_min_mm": "25",
                "height_adj_max_mm": "128",
                "flow_rate_lps": "",
                "flow_rate_lps_10mm_head": "0.4",
                "flow_rate_lps_20mm_head": "0.46",
                "selected_default_flow_rate_lps": "",
                "flow_policy": "split_fields_only",
                "missing_technical_fields": "flow_rate_lps",
                "safe_to_generate": False,
                "ready_for_benchmark": False,
                "ready_for_customer_view": False,
                "blocking_reason": "blocked_pending_conditional_parameter_scoring",
            }
        )
    return pd.DataFrame(rows)


def _report(mplus=None, products=None, final_assemblies=None, final_set_details=None):
    return mod.build_readiness_gate_report(
        candidates_all=pd.DataFrame([{"product_id": "candidate"}] * 118),
        products=products if products is not None else pd.DataFrame([{"product_id": "existing"}] * 46),
        comparison=pd.DataFrame([{"product_id": "existing"}] * 46),
        components=pd.DataFrame([{"product_id": "component"}] * 100),
        bom_options=pd.DataFrame([{"product_id": "bom"}] * 221),
        final_assemblies=final_assemblies if final_assemblies is not None else pd.DataFrame([{"product_id": "assembled"}] * 28),
        final_set_details=final_set_details if final_set_details is not None else pd.DataFrame([{"set_id": "set", "assembled_product_id": "assembled"}] * 28),
        mplus_mappings=mplus if mplus is not None else _mplus_rows(),
    )


def test_build_readiness_gate_blocks_current_policy_only_state():
    report = _report()

    assert report.input_frame_counts == {
        "Products": 46,
        "Comparison": 46,
        "Candidates_All": 118,
        "Components": 100,
        "BOM_Options": 221,
        "Final_Assemblies": 28,
        "Final_Set_Details": 28,
        "Mplus_Compound_Mappings": 4,
        "Conditional_Technical_Values": 8,
    }
    assert report.proposal_mapping_count == 4
    assert report.conditional_technical_value_count == 8
    assert report.production_ready is False
    assert report.readiness_status == "blocked_pending_conditional_parameter_scoring"
    assert report.blocking_reason == "blocked_pending_conditional_parameter_scoring"
    assert report.next_required_action == "implement scoring/export handling for conditional parameter values before production M+ assemblies"
    assert report.safe_to_generate_count == 0
    assert report.blocked_count == 4

    conditions = {condition.condition: condition for condition in report.conditions}
    assert conditions["exactly 4 proposal mappings exist"].passed is True
    assert conditions["all rows have channel_body_id"].passed is True
    assert conditions["all rows have drain_body_id"].passed is True
    assert conditions["all rows have grate_id"].passed is True
    assert conditions["all rows have water_seal_mm"].passed is True
    assert conditions["all rows have outlet_dn"].passed is True
    assert conditions["all rows have height_adj_min_mm"].passed is True
    assert conditions["all rows have height_adj_max_mm"].passed is True
    assert conditions["all rows have flow_rate_lps or an accepted policy that selects a canonical flow value"].passed is False
    assert conditions["flow_policy is accepted for production"].passed is False
    assert conditions["selected_default_flow_rate_lps is non-empty only after policy acceptance"].passed is True
    assert conditions["safe_to_generate is True only after all required fields and policy are complete"].passed is True
    assert conditions["conditional technical values exist for all M+ proposal mappings"].passed is True

    assert {row.drain_body_article_number for row in report.per_mapping_rows} == set(mod.EXPECTED_MPLUS_DRAIN_ARTICLES)
    assert all(row.flow_rate_lps == "" for row in report.per_mapping_rows)
    assert all(row.selected_default_flow_rate_lps == "" for row in report.per_mapping_rows)
    assert all(row.flow_rate_lps_10mm_head == "0.4" for row in report.per_mapping_rows)
    assert all(row.flow_rate_lps_20mm_head == "0.46" for row in report.per_mapping_rows)
    assert all(row.flow_policy == "split_fields_only" for row in report.per_mapping_rows)
    assert all(row.safe_to_generate is False for row in report.per_mapping_rows)
    assert all(row.readiness_status == "blocked_pending_conditional_parameter_scoring" for row in report.per_mapping_rows)


def test_risk_checks_detect_overlaps_urls_and_unaccepted_policy_violations():
    mplus = _mplus_rows()
    risky_set_id = "https://example.test/diagnostic-mplus-risk"
    mplus.loc[0, "set_id"] = risky_set_id
    mplus.loc[0, "channel_body_id"] = ""
    mplus.loc[1, "drain_body_id"] = ""
    mplus.loc[2, "grate_id"] = ""
    mplus.loc[3, "water_seal_mm"] = ""
    mplus.loc[0, "flow_rate_lps"] = "0.4"
    mplus.loc[1, "selected_default_flow_rate_lps"] = "0.4"
    mplus.loc[2, "safe_to_generate"] = True
    mplus.loc[2, "ready_for_benchmark"] = True
    mplus.loc[3, "ready_for_customer_view"] = True

    report = _report(
        mplus=mplus,
        products=pd.DataFrame([{"product_id": risky_set_id}]),
        final_assemblies=pd.DataFrame([{"product_id": risky_set_id}]),
        final_set_details=pd.DataFrame([{"set_id": "x", "assembled_product_id": risky_set_id}]),
    )

    risks = {risk.risk_check: risk for risk in report.risk_checks}
    assert risks["M+ set IDs promoted to Products.product_id"].passed is False
    assert risks["M+ set IDs promoted to Final_Assemblies.product_id"].passed is False
    assert risks["M+ set IDs promoted to Final_Set_Details"].passed is False
    assert risks["URL-bearing set IDs"].passed is False
    assert risks["missing channel body ID"].passed is False
    assert risks["missing drain body ID"].passed is False
    assert risks["missing grate ID"].passed is False
    assert risks["missing WS/DN/height fields"].passed is False
    assert risks["flow_rate_lps filled without accepted policy"].passed is False
    assert risks["selected_default_flow_rate_lps filled without accepted policy"].passed is False
    assert risks["safe_to_generate True while policy is unaccepted"].passed is False
    assert risks["ready_for_benchmark True while policy is unaccepted"].passed is False
    assert risks["ready_for_customer_view True while policy is unaccepted"].passed is False
    assert risks["production behavior changed"].passed is True


def test_build_current_report_runs_discovery_pipeline_and_export_diagnostic(monkeypatch):
    mplus = _mplus_rows()
    monkeypatch.setattr(mod.aco, "discover_candidates", lambda **kwargs: ([{"product_id": "candidate"}], {"debug": True}))
    monkeypatch.setattr(
        mod.pipeline,
        "run_update",
        lambda candidates_all, config: (
            pd.DataFrame([{"product_id": "existing"}]),
            pd.DataFrame([{"product_id": "existing"}]),
            pd.DataFrame([{"product_id": "component"}]),
            pd.DataFrame(),
            pd.DataFrame([{"product_id": "bom"}]),
        ),
    )
    monkeypatch.setattr(mod, "_extract_final_assemblies", lambda products: pd.DataFrame([{"product_id": "assembled"}]))
    monkeypatch.setattr(
        mod,
        "_extract_final_set_details",
        lambda final_assemblies, bom_options, components: pd.DataFrame([{"set_id": "set", "assembled_product_id": "assembled"}]),
    )
    monkeypatch.setattr(mod, "_extract_mplus_compound_mappings", lambda *args: mplus)

    report = mod.build_current_report()

    assert report.input_frame_counts["Candidates_All"] == 1
    assert report.input_frame_counts["Mplus_Compound_Mappings"] == 4
    assert report.production_ready is False
    assert report.production_behavior_changed is True


def test_print_report_includes_required_sections(capsys):
    report = _report()

    mod.print_report(report)
    out = capsys.readouterr().out

    assert "Input frame counts" in out
    assert "Products = 46" in out
    assert "M+ proposal mapping count = 4" in out
    assert "M+ conditional technical value count = 8" in out
    assert "Readiness gate condition table" in out
    assert "Per-mapping readiness rows" in out
    assert "production_ready = False" in out
    assert "readiness_status = blocked_pending_conditional_parameter_scoring" in out
    assert "safe_to_generate = 0" in out
    assert "blocked = 4" in out
    assert "blocked_pending_conditional_parameter_scoring" in out
    assert "implement scoring/export handling for conditional parameter values before production M+ assemblies" in out
    assert "Risk checks" in out
    assert "production behavior changed: yes" in out
