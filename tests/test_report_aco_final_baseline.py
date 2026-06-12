from __future__ import annotations

import hashlib
from copy import deepcopy
from pathlib import Path

import pandas as pd
from pandas.testing import assert_frame_equal

from tools import report_aco_final_baseline as mod


def _assembly_rows() -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    specs = (
        ("easyflow", 2, False, "partial"),
        ("easyflowplus", 6, True, "complete"),
        ("showerdrain_splus", 16, True, "complete"),
        ("showerdrain_c", 4, True, "complete"),
        ("showerdrain_cplus", 30, True, "explicit_source_ready_production_assembly"),
        ("showerdrain_mplus", 4, False, "conditional_parameter_available_production_blocked"),
    )
    for family, count, ready, status in specs:
        for index in range(count):
            row: dict[str, object] = {
                "product_id": f"aco-assembled-{family}-{index}",
                "assembled_family": family,
                "assembled_from_bom": "TRUE",
                "data_quality_status": status,
                "is_complete_technical_data": "TRUE" if ready else "FALSE",
                "ready_for_benchmark": "TRUE" if ready else "FALSE",
                "ready_for_customer_view": "TRUE" if ready and family != "showerdrain_mplus" else "FALSE",
                "customer_view_enabled": "TRUE" if ready and family != "showerdrain_mplus" else "FALSE",
                "flow_rate_lps": 0.7 if ready else "",
                "height_adj_min_mm": 10 if ready else "",
                "height_adj_max_mm": 100 if ready else "",
            }
            if family == "showerdrain_cplus":
                base_id = (
                    "aco-showerdrain-cplus-standard-h92"
                    if index < 15 else "aco-showerdrain-cplus-low-h69"
                )
                hydraulics = mod.CPLUS_BASE_HYDRAULICS[base_id]
                row.update({
                    "product_id": f"aco-assembled-showerdrain-cplus-{index}",
                    "product_family": "showerdrain_cplus",
                    "family": "showerdrain_cplus",
                    "assembly_model": "base_x_grate",
                    "base_id": base_id,
                    "base_article_number": "9010.85.10" if index < 15 else "9010.85.20",
                    "grate_id": f"aco-cplus-design-grate-{index % 15}",
                    "grate_article_number": f"9010.88.{index % 15:02d}",
                    "product_name": f"C+ approved grate {index}",
                    **hydraulics,
                })
            if family == "easyflow":
                row.update({
                    "flow_rate_lps": "", "height_adj_min_mm": "", "height_adj_max_mm": "",
                    "is_complete_technical_data": "FALSE",
                    "missing_technical_fields": "flow_rate_lps,height_adj_min_mm,height_adj_max_mm",
                })
            rows.append(row)
    return rows


def _products(assemblies: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for _, assembly in assemblies.iterrows():
        family = assembly["assembled_family"]
        rows.append({
            "product_id": assembly["product_id"],
            "product_family": family,
            "assembly_model": "channel_body_x_drain_body_x_grate" if family == "showerdrain_mplus" else "base_x_grate",
            "flow_rate_lps": assembly["flow_rate_lps"],
            "selected_default_flow_rate_lps": "",
            "ready_for_benchmark": assembly["ready_for_benchmark"],
            "ready_for_customer_view": assembly["ready_for_customer_view"],
            "customer_view_enabled": assembly["customer_view_enabled"],
            "blocked_reason": "blocked_pending_conditional_parameter_scoring" if family == "showerdrain_mplus" else "",
        })
    for article in sorted(mod.BLINE_ARTICLES):
        rows.append({
            "product_id": f"aco-showerdrain-b-finished-set-{article.replace('.', '-')}",
            "product_family": "showerdrain_b",
            "product_article_number": article,
            "article_number": article,
            "assembly_model": "integral_all_in_one_set",
            "flow_rate_lps": "",
            "selected_default_flow_rate_lps": "",
            "ready_for_benchmark": False,
            "ready_for_customer_view": False,
            "customer_view_enabled": False,
            "body_article_number": "",
            "grate_article_number": "",
            "blocked_reason": "blocked_pending_conditional_parameter_scoring",
        })
    rows.append({
        "product_id": "aco-showerdrain-b-family-discovery",
        "product_family": "showerdrain_b",
        "family": "showerdrain_b",
        "candidate_type": "family_navigation",
        "assembly_model": "",
        "flow_rate_lps": "0.90",
        "selected_default_flow_rate_lps": "",
        "ready_for_benchmark": "FALSE",
        "ready_for_customer_view": "FALSE",
        "customer_view_enabled": "FALSE",
        "product_article_number": "",
        "body_article_number": "",
        "grate_article_number": "",
    })
    while len(rows) < 88:
        index = len(rows)
        rows.append({
            "product_id": f"aco-canonical-filler-{index}",
            "product_family": "other",
            "flow_rate_lps": 0.5,
            "ready_for_benchmark": True,
            "ready_for_customer_view": True,
            "customer_view_enabled": True,
        })
    return pd.DataFrame(rows)


def _details(assemblies: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for _, assembly in assemblies.iterrows():
        family = assembly["assembled_family"]
        ready = family not in {"easyflow", "showerdrain_mplus"}
        rows.append({
            "set_id": assembly["product_id"],
            "assembled_product_id": assembly["product_id"],
            "assembled_family": family,
            "ready_for_benchmark": "TRUE" if ready else "FALSE",
            "ready_for_customer_view": "TRUE" if ready else "FALSE",
            "base_product_id": "aco-easyflow-complete-dn50-ws50" if family == "easyflow" else "",
            "base_article_number": "",
            "article_number": "",
            "selected_article_number": "",
            "data_quality_status": assembly["data_quality_status"],
        })
    return pd.DataFrame(rows)


def _conditions(products: pd.DataFrame) -> pd.DataFrame:
    rows = []
    conditional = products[
        products["product_family"].eq("showerdrain_mplus")
        | products.apply(mod.is_approved_bline_finished_set_candidate, axis=1)
    ]
    for _, product in conditional.iterrows():
        for head, flow in ((10, 0.40), (20, 0.46)):
            rows.append({
                "set_id": product["product_id"],
                "product_family": product["product_family"],
                "assembly_model": product["assembly_model"],
                "parameter_name": "flow_rate_lps",
                "value": flow,
                "unit": "l/s",
                "condition_type": "head_water_level",
                "condition_value": head,
                "condition_unit": "mm",
                "condition_label": f"{head} mm head water level",
            })
    return pd.DataFrame(rows)


def _canonical_sheets() -> dict[str, pd.DataFrame]:
    assemblies = pd.DataFrame(_assembly_rows())
    products = _products(assemblies)
    details = _details(assemblies)
    cplus_evidence = []
    for base_id, values in mod.CPLUS_BASE_HYDRAULICS.items():
        for index in range(15):
            cplus_evidence.append({
                "set_id": f"{base_id}-{index}",
                "product_family": "showerdrain_cplus",
                "assembly_model": "base_x_grate",
                "base_id": base_id,
                "grate_id": f"aco-cplus-design-grate-{index}",
                "grate_article_number": f"9010.88.{index:02d}",
                "flow_rate_lps": values["flow_rate_lps"],
                "water_seal_mm": values["water_seal_mm"],
                "outlet_dn": values["outlet_dn"],
                "height_adj_min_mm": values["height_adj_min_mm"],
                "height_adj_max_mm": values["height_adj_max_mm"],
                "article_level_compatibility_found": True,
                "safe_to_generate": True,
                "ready_for_benchmark": True,
                "ready_for_customer_view": False,
            })
    bom = pd.DataFrame([
        {
            "product_id": row["set_id"],
            "component_id": row["grate_id"],
            "product_family": "showerdrain_cplus",
            "option_type": "compatible_grate",
        }
        for row in cplus_evidence
    ] + [
        {"product_id": f"other-{i}", "component_id": f"accessory-{i}",
         "product_family": "other", "option_type": "optional_accessory"}
        for i in range(221)
    ])
    raw_easyflow_articles = (
        "2500.00.00", "2500.00.77", "2500.05.00", "2500.05.77", "2500.55.00",
        "2500.55.77", "2505.00.00", "2505.00.77", "2505.05.00", "2505.05.77",
    )
    variants = pd.DataFrame([
        {
            "product_family": "easyflow",
            "variant_type": "candidate_body_variant",
            "article_number": article,
            "base_product_id": (
                "aco-easyflow-complete-dn50-ws50"
                if article in mod.EASYFLOW_ARTICLES else "aco-easyflow-other-variant-scope"
            ),
            "source_url": f"https://example.test/easyflow/{article}",
            "attribution_status": "candidate_variant",
            "water_seal_mm": 50,
            "outlet_dn": "DN50",
            "flow_rate_lps": 1.0 if article == "2500.05.00" else 1.5,
            "height_adj_min_mm": 7 if article == "2500.05.00" else 15,
            "height_adj_max_mm": 75 if article == "2500.05.00" else 96,
        }
        for article in raw_easyflow_articles
    ] + [
        {"product_family": "other", "variant_type": "other", "article_number": f"9999.{i:02d}.00"}
        for i in range(66)
    ])
    eplus_evidence = pd.DataFrame([
        {
            "evidence_id": f"eplus-{i}",
            "product_family": "showerdrain_eplus",
            "article_level_compatibility_found": False,
            "ready_for_customer_view": False,
            "production_status_note": "diagnostic-only evidence; no production generation change.",
        }
        for i in range(3)
    ])
    sheets: dict[str, pd.DataFrame] = {
        "Products": products,
        "Comparison": products.copy(deep=True),
        "Scoring_Field_Coverage": pd.DataFrame({"product_id": products["product_id"]}),
        "Candidates_All": pd.DataFrame({"candidate_id": range(118)}),
        "Components": pd.DataFrame({"component_id": [f"component-{i}" for i in range(100)]}),
        "BOM_Options": bom,
        "Final_Assemblies": assemblies,
        "Final_Set_Details": details,
        "Article_Variants": variants,
        "Mplus_Compound_Mappings": pd.DataFrame({"set_id": range(4)}),
        "Eplus_Proposal_Mappings": pd.DataFrame({"set_id": range(3)}),
        "Eplus_Compatible_Grate_Evidence": eplus_evidence,
        "Cplus_Compatible_Grate_Evidence": pd.DataFrame(cplus_evidence),
        "Bline_Source_Evidence": pd.DataFrame({"evidence_id": range(8)}),
        "Conditional_Technical_Values": _conditions(products),
        "Scoring_Scenarios": pd.DataFrame({"scenario_id": ["no_scenario_selected", "flow_head_10mm", "flow_head_20mm"]}),
        "Comparison_flow_head_10mm": products.copy(deep=True),
        "Comparison_flow_head_20mm": products.copy(deep=True),
    }
    return sheets


def _write_workbook(path: Path, sheets: dict[str, pd.DataFrame]) -> None:
    with pd.ExcelWriter(path, engine="openpyxl") as writer:
        for name, frame in sheets.items():
            frame.to_excel(writer, sheet_name=name, index=False)


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def test_final_baseline_counts_classifications_and_customer_states() -> None:
    sheets = _canonical_sheets()
    raw_easyflow = sheets["Article_Variants"][
        sheets["Article_Variants"]["product_family"].eq("easyflow")
    ]
    raw_bline = sheets["Products"][
        sheets["Products"]["product_family"].eq("showerdrain_b")
    ]
    report = mod.audit_frames(sheets)

    assert len(raw_easyflow) == 10
    assert len(raw_bline) == 9
    assert report.overall == mod.STABLE
    assert report.sheet_counts == mod.EXPECTED_SHEET_COUNTS
    assert report.family_counts == mod.EXPECTED_FINAL_FAMILY_COUNTS
    assert report.customer_counts == {
        "canonical_final_set_details": 56,
        "cplus": 30,
        "easyflow": 0,
        "mplus": 0,
        "bline": 0,
        "eplus": 0,
        "runtime_mplus_10mm": 4,
        "runtime_mplus_20mm": 4,
        "runtime_bline_10mm": 8,
        "runtime_bline_20mm": 8,
    }
    assert all(check.passed for check in report.checks)


def test_cplus_approved_state_and_protected_hydraulics_are_required() -> None:
    sheets = _canonical_sheets()
    sheets["Final_Assemblies"].loc[sheets["Final_Assemblies"]["assembled_family"].eq("showerdrain_cplus").idxmax(), "flow_rate_lps"] = 9.99

    report = mod.audit_frames(sheets)

    assert not next(check for check in report.checks if check.name == "cplus_protected_hydraulics").passed
    assert report.overall == mod.UNSTABLE


def test_mplus_and_bline_conditional_runtime_readiness_and_zero_defaults() -> None:
    sheets = _canonical_sheets()
    report = mod.audit_frames(sheets)
    checks = {check.name: check.passed for check in report.checks}

    for name in (
        "mplus_zero_scalar_defaults", "bline_zero_scalar_defaults",
        "runtime_mplus_no_selection_blocked", "runtime_mplus_10mm", "runtime_mplus_20mm",
        "runtime_bline_no_selection_blocked", "runtime_bline_10mm", "runtime_bline_20mm",
        "bline_direct_integral_finished_sets",
    ):
        assert checks[name]

    products = sheets["Products"]
    index = products[products["product_family"].eq("showerdrain_mplus")].index[0]
    products.loc[index, "selected_default_flow_rate_lps"] = "0.40"
    broken = mod.audit_frames(sheets)
    assert not next(check for check in broken.checks if check.name == "mplus_zero_scalar_defaults").passed


def test_malformed_approved_bline_identity_is_in_scope_and_fails_closed() -> None:
    sheets = _canonical_sheets()
    products = sheets["Products"]
    approved = products["product_article_number"].eq("9010.78.70")
    products.loc[approved, "assembly_model"] = "base_x_grate"

    report = mod.audit_frames(sheets)
    checks = {check.name: check.passed for check in report.checks}

    assert not checks["bline_assembly_model"]
    assert not checks["bline_direct_integral_finished_sets"]
    assert not checks["runtime_bline_10mm"]
    assert report.overall == mod.UNSTABLE


def test_easyflow_unresolved_and_eplus_diagnostic_only_states_are_required() -> None:
    sheets = _canonical_sheets()
    report = mod.audit_frames(sheets)
    checks = {check.name: check.passed for check in report.checks}
    assert checks["easyflow_partial_blocked"]
    assert checks["easyflow_protected_scalars_empty"]
    assert checks["easyflow_candidate_articles"]
    assert checks["easyflow_no_silent_article_selection"]
    assert checks["eplus_diagnostic_only"]

    sheets["Final_Set_Details"].loc[0, "selected_article_number"] = "2500.05.00"
    sheets["Eplus_Compatible_Grate_Evidence"].loc[0, "article_level_compatibility_found"] = True
    broken = mod.audit_frames(sheets)
    broken_checks = {check.name: check.passed for check in broken.checks}
    assert not broken_checks["easyflow_no_silent_article_selection"]
    assert not broken_checks["eplus_diagnostic_only"]


def test_audit_frames_does_not_mutate_any_dataframe() -> None:
    sheets = _canonical_sheets()
    before = deepcopy(sheets)

    mod.audit_frames(sheets)

    assert sheets.keys() == before.keys()
    for name in sheets:
        assert_frame_equal(sheets[name], before[name])


def test_audit_workbook_is_byte_for_byte_read_only_and_cli_reports_handoff(tmp_path: Path, capsys) -> None:
    path = tmp_path / "canonical.xlsx"
    _write_workbook(path, _canonical_sheets())
    before = _sha256(path)

    report = mod.audit_workbook(path)
    exit_code = mod.main(["--xlsx", str(path)])
    output = capsys.readouterr().out

    assert exit_code == 0
    assert report.overall == mod.STABLE
    assert report.workbook_sha256_before == before
    assert report.workbook_sha256_after == before
    assert _sha256(path) == before
    assert "OVERALL: ACO_BASELINE_STABLE" in output
    assert "Easyflow article-level attribution" in output
    assert "E+ explicit compatibility evidence" in output
    assert "freeze ACO baseline and begin next manufacturer integration" in output
