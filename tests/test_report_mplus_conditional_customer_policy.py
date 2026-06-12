from __future__ import annotations

import hashlib
import subprocess
import sys
from pathlib import Path

import pandas as pd
from pandas.testing import assert_frame_equal

from src.excel_export import (
    _append_bline_direct_finished_set_rows,
    _append_mplus_final_assembly_rows,
    _extract_conditional_technical_values,
    _extract_final_assemblies,
)
from src.scenario_scoring import build_scenario_comparison, scoring_scenarios_dataframe
from tools.report_bline_source_evidence import build_export_evidence_dataframe
from tools.report_mplus_conditional_customer_policy import (
    CUSTOMER_APPROVAL_REQUIRED,
    DEFAULT_BLOCKED,
    EXPECTED_ARTICLES,
    INVALID,
    POLICY_GATE,
    SCENARIO_10_READY,
    SCENARIO_20_READY,
    build_mplus_conditional_customer_policy_report,
    policy_gate,
    workbook_diagnostics,
)


def _mplus_mappings() -> pd.DataFrame:
    rows = []
    for article in sorted(EXPECTED_ARTICLES):
        drain_id = f"aco-{article.replace('.', '')}"
        set_id = (
            "aco-assembled-showerdrain-mplus-channel-body-25-128__"
            f"{drain_id}__mplus-design-roste-elektropoliert"
        )
        rows.append({
            "set_id": set_id,
            "product_family": "showerdrain_mplus",
            "assembly_model": "channel_body_x_drain_body_x_grate",
            "channel_body_id": "channel-body-25-128",
            "channel_body_article_number": "",
            "drain_body_id": drain_id,
            "drain_body_article_number": article,
            "grate_id": "mplus-design-roste-elektropoliert",
            "grate_article_number": "",
            "source_url_channel_body": "https://example.test/mplus/channel",
            "source_url_drain_body": "https://example.test/mplus/drain",
            "source_url_grate": "https://example.test/mplus/grate",
            "water_seal_mm": 50,
            "outlet_dn": "DN50",
            "height_adj_min_mm": 25,
            "height_adj_max_mm": 128,
            "flow_rate_lps": "",
            "flow_rate_lps_10mm_head": 0.40,
            "flow_rate_lps_20mm_head": 0.46,
            "selected_default_flow_rate_lps": "",
            "flow_evidence_type": "explicit_drain_body_family_level",
            "flow_confidence": "medium",
            "flow_attribution_scope": "drain_body_family_level",
            "flow_article_specific": False,
            "safe_to_generate": False,
            "ready_for_benchmark": False,
            "ready_for_customer_view": False,
            "blocking_reason": "blocked_pending_conditional_parameter_scoring",
        })
    return pd.DataFrame(rows)


def _canonical_sheets() -> dict[str, pd.DataFrame]:
    base_rows: list[dict[str, object]] = []
    family_counts = {
        "showerdrain_cplus": 30,
        "easyflow": 2,
        "showerdrain_splus": 16,
        "showerdrain_c": 4,
        "easyflowplus": 6,
        "catalog_other": 17,
    }
    for family, count in family_counts.items():
        for index in range(count):
            assembled = family != "catalog_other"
            product_id = (
                f"aco-assembled-{family.replace('_', '-')}-{index}"
                if assembled else f"aco-catalog-other-{index}"
            )
            is_easyflow = family == "easyflow"
            base_rows.append({
                "manufacturer": "aco",
                "product_id": product_id,
                "product_name": product_id,
                "product_family": family,
                "family": family,
                "assembled_family": family if assembled else "",
                "flow_rate_lps": "" if is_easyflow else 0.5,
                "water_seal_mm": 50,
                "outlet_dn": "DN50",
                "height_adj_min_mm": "" if is_easyflow else 25,
                "height_adj_max_mm": "" if is_easyflow else 100,
                "ready_for_benchmark": not is_easyflow,
                "ready_for_customer_view": not is_easyflow,
                "customer_view_enabled": not is_easyflow,
                "blocked_reason": "missing_technical_fields" if is_easyflow else "",
                "assembly_model": "base_x_grate" if assembled else "",
                "assembled_from_bom": assembled,
            })
    products = pd.DataFrame(base_rows)
    comparison = products.copy(deep=True)
    mappings = _mplus_mappings()
    products, comparison = _append_mplus_final_assembly_rows(products, comparison, mappings)

    bline_evidence = build_export_evidence_dataframe()
    products, comparison = _append_bline_direct_finished_set_rows(
        products, comparison, bline_evidence
    )
    extra_bline_family_row = pd.DataFrame([{
        "manufacturer": "aco",
        "product_id": "aco-showerdrain-b-family-discovery",
        "product_name": "ACO ShowerDrain B family discovery row",
        "product_family": "showerdrain_b",
        "family": "showerdrain_b",
        "assembled_family": "",
        "product_article_number": "",
        "article_number": "",
        "assembly_model": "",
        "flow_rate_lps": 0.90,
        "ready_for_benchmark": False,
        "ready_for_customer_view": False,
        "customer_view_enabled": False,
        "candidate_type": "family_navigation",
        "body_article_number": "",
        "grate_article_number": "",
    }])
    products = pd.concat([products, extra_bline_family_row], ignore_index=True)
    comparison = pd.concat([comparison, extra_bline_family_row], ignore_index=True)
    conditional = _extract_conditional_technical_values(mappings, bline_evidence)
    final = _extract_final_assemblies(products)
    details = final.copy(deep=True)
    details["set_id"] = details["product_id"]
    details["assembled_product_id"] = details["product_id"]

    return {
        "Products": products,
        "Comparison": comparison,
        "Scoring_Field_Coverage": pd.DataFrame({"product_id": products["product_id"]}),
        "BOM_Options": pd.DataFrame({"option_id": range(251)}),
        "Final_Assemblies": final,
        "Final_Set_Details": details,
        "Mplus_Compound_Mappings": mappings,
        "Conditional_Technical_Values": conditional,
        "Scoring_Scenarios": scoring_scenarios_dataframe(),
        "Comparison_flow_head_10mm": build_scenario_comparison(
            comparison, conditional, "flow_head_10mm"
        ),
        "Comparison_flow_head_20mm": build_scenario_comparison(
            comparison, conditional, "flow_head_20mm"
        ),
        "Eplus_Compatible_Grate_Evidence": pd.DataFrame({
            "product_family": ["showerdrain_eplus"] * 3,
            "ready_for_benchmark": [False] * 3,
            "ready_for_customer_view": [False] * 3,
        }),
        "Bline_Source_Evidence": bline_evidence,
    }


def _write_workbook(path: Path, sheets: dict[str, pd.DataFrame]) -> None:
    with pd.ExcelWriter(path, engine="openpyxl") as writer:
        for name, frame in sheets.items():
            frame.to_excel(writer, sheet_name=name, index=False)


def test_policy_report_validates_all_four_assemblies_without_mutating_frames() -> None:
    sheets = _canonical_sheets()
    originals = {name: frame.copy(deep=True) for name, frame in sheets.items()}

    review = build_mplus_conditional_customer_policy_report(sheets)
    diagnostics = workbook_diagnostics(sheets, review)

    assert len(review) == 4
    assert review["assembly_id"].nunique() == 4
    assert review["classification"].eq(CUSTOMER_APPROVAL_REQUIRED).all()
    assert review["policy_gate"].eq(POLICY_GATE).all()
    assert review["blocking_reasons"].eq("").all()
    assert review["policy_states"].map(lambda states: set(states.split(","))).eq({
        DEFAULT_BLOCKED, SCENARIO_10_READY, SCENARIO_20_READY, CUSTOMER_APPROVAL_REQUIRED
    }).all()
    assert policy_gate(review) == POLICY_GATE

    assert diagnostics["Mplus_assemblies_reviewed"] == 4
    assert diagnostics["default_blocked_rows"] == 4
    assert diagnostics["scenario_10mm_ready_rows"] == 4
    assert diagnostics["scenario_20mm_ready_rows"] == 4
    assert diagnostics["canonical_customer_ready_rows"] == 0
    assert diagnostics["no_selection_customer_ready_rows"] == 0
    assert diagnostics["runtime_10mm_customer_ready_rows"] == 4
    assert diagnostics["runtime_20mm_customer_ready_rows"] == 4
    assert diagnostics["runtime_10mm_0_40_rows"] == 4
    assert diagnostics["runtime_20mm_0_46_rows"] == 4
    assert diagnostics["unconditional_scalar_defaults"] == 0
    assert diagnostics["Bline_canonical_rows"] == 8
    assert diagnostics["Bline_canonical_customer_ready_rows"] == 0
    assert diagnostics["Bline_no_selection_customer_ready_rows"] == 0
    assert diagnostics["Bline_runtime_10mm_customer_ready_rows"] == 8
    assert diagnostics["Bline_runtime_20mm_customer_ready_rows"] == 8
    assert diagnostics["Bline_runtime_10mm_0_40_rows"] == 8
    assert diagnostics["Bline_runtime_20mm_0_46_rows"] == 8
    assert diagnostics["invalid_conditional_rows"] == 0
    assert diagnostics["selected_scalar_defaults"] == 0

    for name, original in originals.items():
        assert_frame_equal(sheets[name], original)


def test_canonical_counts_and_adjacent_family_states_remain_unchanged() -> None:
    sheets = _canonical_sheets()
    assert len(sheets["Products"]) == 88
    assert len(sheets["Comparison"]) == 88
    assert len(sheets["Scoring_Field_Coverage"]) == 88
    assert len(sheets["BOM_Options"]) == 251
    assert len(sheets["Final_Assemblies"]) == 62
    assert len(sheets["Final_Set_Details"]) == 62
    assert len(sheets["Conditional_Technical_Values"]) == 24

    products = sheets["Products"]
    mplus = products[products["product_family"].eq("showerdrain_mplus")]
    conditions = sheets["Conditional_Technical_Values"]
    mplus_conditions = conditions[conditions["product_family"].eq("showerdrain_mplus")]
    assert len(mplus_conditions) == 8
    assert mplus["flow_rate_lps"].fillna("").eq("").all()
    assert sheets["Mplus_Compound_Mappings"]["selected_default_flow_rate_lps"].fillna("").eq("").all()
    assert mplus["ready_for_benchmark"].eq(False).all()
    assert mplus["ready_for_customer_view"].eq(False).all()
    assert not mplus.get("customer_view_enabled", pd.Series(False, index=mplus.index)).fillna(False).astype(bool).any()

    for sheet_name, expected in (("Comparison_flow_head_10mm", 0.40), ("Comparison_flow_head_20mm", 0.46)):
        scenario = sheets[sheet_name]
        rows = scenario[scenario["product_family"].eq("showerdrain_mplus")]
        assert len(rows) == 4
        assert rows["flow_rate_lps"].eq(expected).all()
        assert rows["flow_rate_resolution_source"].eq("Conditional_Technical_Values").all()
        assert rows["flow_rate_resolution_status"].eq("resolved_from_condition").all()
        assert rows["scenario_ready_for_benchmark"].eq(True).all()

    cplus = products[products["product_family"].eq("showerdrain_cplus")]
    easyflow = products[products["product_family"].eq("easyflow")]
    bline = products[products["product_family"].eq("showerdrain_b")]
    eplus = sheets["Eplus_Compatible_Grate_Evidence"]
    assert len(cplus) == 30 and cplus["ready_for_customer_view"].eq(True).all()
    assert len(bline) == 9
    approved_bline = bline[bline["product_article_number"].fillna("").ne("")]
    assert len(approved_bline) == 8
    assert approved_bline["assembly_model"].eq("integral_all_in_one_set").all()
    assert approved_bline["flow_rate_lps"].fillna("").eq("").all()
    extra_bline = bline[bline["product_article_number"].fillna("").eq("")]
    assert len(extra_bline) == 1 and extra_bline["flow_rate_lps"].eq(0.90).all()
    assert len(easyflow) == 2 and easyflow["ready_for_benchmark"].eq(False).all()
    assert eplus["ready_for_benchmark"].eq(False).all()
    assert eplus["ready_for_customer_view"].eq(False).all()


def test_invalid_or_selected_default_is_blocked() -> None:
    sheets = _canonical_sheets()
    sheets["Mplus_Compound_Mappings"].loc[0, "selected_default_flow_rate_lps"] = "0.40"
    review = build_mplus_conditional_customer_policy_report(sheets)
    invalid = review[review["classification"].eq(INVALID)]
    assert len(invalid) == 1
    assert not invalid.iloc[0]["check_selected_default_empty"]
    assert policy_gate(review) == INVALID


def test_hidden_customer_scenario_enablement_is_blocked() -> None:
    sheets = _canonical_sheets()
    scenarios = sheets["Scoring_Scenarios"]
    scenarios.loc[scenarios["scenario_id"].eq("flow_head_10mm"), "customer_view_enabled"] = True
    review = build_mplus_conditional_customer_policy_report(sheets)
    assert review["classification"].eq(INVALID).all()
    assert review["check_scenario_registry_requires_explicit_selection"].eq(False).all()
    assert policy_gate(review) == INVALID


def test_cli_is_read_only_and_prints_approved_runtime_policy(tmp_path: Path) -> None:
    workbook = tmp_path / "benchmark_output.xlsx"
    _write_workbook(workbook, _canonical_sheets())
    before = hashlib.sha256(workbook.read_bytes()).hexdigest()

    result = subprocess.run(
        [sys.executable, "tools/report_mplus_conditional_customer_policy.py", "--xlsx", str(workbook)],
        cwd=Path(__file__).resolve().parents[1],
        check=False,
        capture_output=True,
        text=True,
    )

    assert result.returncode == 0, result.stderr
    assert "Mode: read-only policy evaluation" in result.stdout
    assert "M+ canonical rows: 4" in result.stdout
    assert "B-line canonical rows: 8" in result.stdout
    assert "canonical M+ customer-ready rows: 0" in result.stdout
    assert "canonical B-line customer-ready rows: 0" in result.stdout
    assert "no-selection M+ customer-ready rows: 0" in result.stdout
    assert "no-selection B-line customer-ready rows: 0" in result.stdout
    assert "10 mm selected M+ customer-ready rows: 4" in result.stdout
    assert "10 mm selected B-line customer-ready rows: 8" in result.stdout
    assert "20 mm selected M+ customer-ready rows: 4" in result.stdout
    assert "20 mm selected B-line customer-ready rows: 8" in result.stdout
    assert "10 mm M+ resolved flow values (0.40): 4" in result.stdout
    assert "10 mm B-line resolved flow values (0.40): 8" in result.stdout
    assert "20 mm M+ resolved flow values (0.46): 4" in result.stdout
    assert "20 mm B-line resolved flow values (0.46): 8" in result.stdout
    assert "unconditional scalar defaults: 0" in result.stdout
    assert "invalid conditional rows: 0" in result.stdout
    assert f"OVERALL: {POLICY_GATE}" in result.stdout
    assert "keep canonical scalar flow empty" in result.stdout
    assert hashlib.sha256(workbook.read_bytes()).hexdigest() == before


def test_bline_runtime_policy_fails_closed_without_changing_mplus_review() -> None:
    sheets = _canonical_sheets()
    bline_condition = sheets["Conditional_Technical_Values"]["product_family"].eq("showerdrain_b")
    first_bline_index = sheets["Conditional_Technical_Values"].index[bline_condition][0]
    sheets["Conditional_Technical_Values"].loc[first_bline_index, "condition_unit"] = "cm"

    review = build_mplus_conditional_customer_policy_report(sheets)
    diagnostics = workbook_diagnostics(sheets, review)

    assert policy_gate(review) == POLICY_GATE
    assert diagnostics["invalid_conditional_rows"] == 1
    assert diagnostics["Bline_runtime_10mm_customer_ready_rows"] == 7
    assert policy_gate(review, diagnostics) == INVALID


def test_report_ignores_extra_family_level_bline_row_with_scalar_flow() -> None:
    sheets = _canonical_sheets()
    products = sheets["Products"]
    raw_bline = products[products["product_family"].eq("showerdrain_b")]
    assert len(products) == 88
    assert len(raw_bline) == 9
    extra = raw_bline[raw_bline["product_article_number"].fillna("").eq("")]
    assert len(extra) == 1
    assert extra.iloc[0]["flow_rate_lps"] == 0.90

    review = build_mplus_conditional_customer_policy_report(sheets)
    diagnostics = workbook_diagnostics(sheets, review)

    assert diagnostics["Bline_canonical_rows"] == 8
    assert diagnostics["Bline_canonical_customer_ready_rows"] == 0
    assert diagnostics["Bline_no_selection_customer_ready_rows"] == 0
    assert diagnostics["Bline_runtime_10mm_customer_ready_rows"] == 8
    assert diagnostics["Bline_runtime_20mm_customer_ready_rows"] == 8
    assert diagnostics["Bline_runtime_10mm_0_40_rows"] == 8
    assert diagnostics["Bline_runtime_20mm_0_46_rows"] == 8
    assert diagnostics["unconditional_scalar_defaults"] == 0
    assert diagnostics["invalid_conditional_rows"] == 0
    assert policy_gate(review, diagnostics) == POLICY_GATE
