import importlib.util
from pathlib import Path

import numpy as np
import pandas as pd

from src.scenario_scoring import build_scenario_comparison, scoring_scenarios_dataframe


def _load_validator_module():
    spec = importlib.util.spec_from_file_location("validate_xlsx_export", "tools/validate_xlsx_export.py")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod



def _eplus_proposal_mappings_dataframe():
    body_rows = [
        (
            "aco-showerdrain-eplus-rinnenkoerper-einbauhoehe-oberkante-estrich-25-128-mm",
            "https://example.test/eplus/body-25",
            25,
        ),
        (
            "aco-showerdrain-eplus-rinnenkoerper-einbauhoehe-oberkante-estrich-57-128-mm",
            "https://example.test/eplus/body-57",
            57,
        ),
        (
            "aco-showerdrain-eplus-rinnenkoerper-einbauhoehe-oberkante-estrich-80-128-mm-din-en-1253-1",
            "https://example.test/eplus/body-80",
            80,
        ),
    ]
    grate_id = "aco-showerdrain-eplus-design-roste-aus-elektropoliertem-edelstahl"
    return pd.DataFrame([
        {
            "set_id": f"diagnostic-eplus-{body_id}__{grate_id}",
            "product_family": "showerdrain_eplus",
            "assembly_model": "base_x_grate",
            "body_id": body_id,
            "body_article_number": "",
            "body_source_url": body_url,
            "grate_id": grate_id,
            "grate_article_number": "",
            "grate_source_url": "https://example.test/eplus/grate",
            "flow_rate_lps": 0.70,
            "water_seal_mm": 50,
            "outlet_dn": "DN50",
            "height_adj_min_mm": height_min,
            "height_adj_max_mm": 128,
            "body_evidence_type": "source_page_level_body_url",
            "body_confidence": "high",
            "grate_evidence_type": "source_page_level_grate_url",
            "grate_confidence": "high",
            "compatibility_evidence_type": "page_level_family_bom_or_inferred_from_current_bom",
            "compatibility_confidence": "medium",
            "article_level_compatibility_found": False,
            "data_quality_status": "proposal_only_partial",
            "missing_evidence": "explicit_article_level_base_to_grate_compatibility",
            "safe_to_generate": False,
            "ready_for_benchmark": False,
            "ready_for_customer_view": False,
            "blocking_reason": "no explicit article-level base-to-grate compatibility matrix",
            "recommended_next_action": "collect explicit article-level E+ base-to-grate compatibility before production generation",
            "production_status_note": "diagnostic/proposal-only; no Products/BOM/assembly generation change",
        }
        for body_id, body_url, height_min in body_rows
    ])

def _mplus_compound_mappings_dataframe():
    rows = []
    for article, water_seal, outlet_dn in [
        ("9010.81.20", "50", "DN40/DN50"),
        ("9010.81.21", "30", "DN40/DN50"),
        ("9010.81.22", "25", "DN40"),
        ("9010.81.23", "50", "DN50"),
    ]:
        drain_id = f"aco-{article.replace('.', '')}"
        rows.append({
            "set_id": f"diagnostic-mplus-channel-body-25-128__{drain_id}__mplus-design-roste-elektropoliert",
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
            "water_seal_mm": water_seal,
            "outlet_dn": outlet_dn,
            "height_adj_min_mm": "25",
            "height_adj_max_mm": "128",
            "flow_rate_lps": "",
            "flow_rate_lps_10mm_head": 0.4,
            "flow_rate_lps_20mm_head": 0.46,
            "selected_default_flow_rate_lps": "",
            "accessory_flow_reduction_lps": 0.1,
            "flow_policy": "split_fields_only",
            "flow_evidence_type": "explicit_drain_body_family_level",
            "flow_confidence": "medium",
            "flow_article_specific": False,
            "flow_attribution_scope": "drain_body_family_level",
            "missing_technical_fields": "flow_rate_lps",
            "data_quality_status": "partial",
            "safe_to_generate": False,
            "ready_for_benchmark": False,
            "ready_for_customer_view": False,
            "blocking_reason": "blocked_pending_conditional_parameter_scoring",
            "recommended_next_action": "implement scoring/export handling for conditional parameter values before production M+ assemblies",
            "production_status_note": "conditional flow values available in Conditional_Technical_Values; scenario scoring not implemented",
        })
    return pd.DataFrame(rows)




def _conditional_technical_values_dataframe():
    rows = []
    for _, mapping in _mplus_compound_mappings_dataframe().iterrows():
        for value, condition_value, condition_label in [
            (0.4, 10, "10 mm head water level"),
            (0.46, 20, "20 mm head water level"),
        ]:
            rows.append({
                "set_id": mapping["set_id"],
                "product_family": mapping["product_family"],
                "assembly_model": mapping["assembly_model"],
                "parameter_name": "flow_rate_lps",
                "value": value,
                "unit": "l/s",
                "condition_type": "head_water_level",
                "condition_value": condition_value,
                "condition_unit": "mm",
                "condition_label": condition_label,
                "channel_body_id": mapping["channel_body_id"],
                "drain_body_id": mapping["drain_body_id"],
                "grate_id": mapping["grate_id"],
                "source_url_channel_body": mapping["source_url_channel_body"],
                "source_url_drain_body": mapping["source_url_drain_body"],
                "source_url_grate": mapping["source_url_grate"],
                "evidence_type": mapping["flow_evidence_type"],
                "confidence": mapping["flow_confidence"],
                "attribution_scope": mapping["flow_attribution_scope"],
                "article_specific": False,
                "data_quality_status": "conditional_parameter_available_production_blocked",
                "safe_to_generate": False,
                "ready_for_benchmark": False,
                "ready_for_customer_view": False,
                "blocking_reason": "blocked_pending_conditional_parameter_scoring",
                "recommended_next_action": "implement scoring/export handling for conditional parameter values before production M+ assemblies",
                "production_status_note": "conditional flow values available in Conditional_Technical_Values; scenario scoring not implemented",
            })
    return pd.DataFrame(rows)

def _cplus_evidence_dataframe():
    rows = []
    expected = {
        "aco-showerdrain-cplus-standard-h92": (0.91, 50, "DN50", 80, 128),
        "aco-showerdrain-cplus-low-h69": (0.62, 25, "DN40", 57, 128),
    }
    for base_id, values in expected.items():
        rows.append({
            "set_id": f"diag-{base_id}-90108861",
            "product_family": "showerdrain_cplus",
            "assembly_model": "base_x_grate",
            "base_id": base_id,
            "base_article_number": "",
            "base_source_url": "https://example.test/cplus",
            "grate_id": "aco-90108861",
            "grate_article_number": "9010.88.61",
            "grate_source_url": "https://example.test/showerdrain-c/grates",
            "flow_rate_lps": values[0],
            "water_seal_mm": values[1],
            "outlet_dn": values[2],
            "height_adj_min_mm": values[3],
            "height_adj_max_mm": values[4],
            "compatibility_evidence_type": "inferred_from_shared_c_grate_page",
            "compatibility_confidence": "low",
            "article_level_compatibility_found": False,
            "source_text_or_reason": "C grate article; no explicit C+ article mapping",
            "data_quality_status": "diagnostic_evidence_only_insufficient_article_level_compatibility",
            "safe_to_generate": False,
            "ready_for_benchmark": False,
            "ready_for_customer_view": False,
            "blocking_reason": "no explicit article-level C+ base-to-grate compatibility matrix",
            "recommended_next_action": "find explicit C+ article matrix / catalog table",
            "production_status_note": "diagnostic/evidence-only; proposal-only; no Products, BOM, assembly generation, benchmark, customer-view, or scoring change",
        })
    return pd.DataFrame(rows)


def _base_dataframes():
    products = pd.DataFrame([
        {"manufacturer": "aco", "product_id": "aco-showerdrain-cplus-standard-h92", "system_role": "drain_unit", "flow_rate_lps": 0.91, "water_seal_mm": 50, "outlet_dn": "DN50", "height_adj_min_mm": 80, "height_adj_max_mm": 128},
        {"manufacturer": "aco", "product_id": "aco-showerdrain-cplus-low-h69", "system_role": "drain_unit", "flow_rate_lps": 0.62, "water_seal_mm": 25, "outlet_dn": "DN40", "height_adj_min_mm": 57, "height_adj_max_mm": 128},
    ])
    data = {
        "Products": products,
        "Comparison": products[["manufacturer", "product_id", "system_role"]].copy(),
        "Scoring_Field_Coverage": products[["manufacturer", "product_id"]].copy(),
        "Components": pd.DataFrame([{"manufacturer": "aco", "product_id": "aco-comp-1", "system_role": "component"}]),
        "Candidates_All": pd.DataFrame([{"product_id": "c1"}, {"product_id": "c2"}]),
        "BOM_Options": pd.DataFrame([{
            "manufacturer": "aco", "product_id": "aco-showerdrain-cplus-standard-h92", "component_id": "aco-comp-1", "option_type": "compatible_grate",
            "option_meta": "compatibility_confidence=implicit_family_level; explicit_article_matrix=false; source_limitation=; no explicit article-to-article matrix found"
        }]),
        "Mplus_Compound_Mappings": _mplus_compound_mappings_dataframe(),
        "Eplus_Proposal_Mappings": _eplus_proposal_mappings_dataframe(),
        "Cplus_Compatible_Grate_Evidence": _cplus_evidence_dataframe(),
        "Conditional_Technical_Values": _conditional_technical_values_dataframe(),
        "Article_Variants": pd.DataFrame([
            {
                "manufacturer": "aco",
                "base_product_id": "aco-assembled-easyflow-1",
                "article_number": "2500.55.00",
                "variant_type": "candidate_body_variant",
                "product_family": "easyflow",
                "source_url": "https://example.test/easyflow/body-1",
                "water_seal_mm": 50,
                "outlet_dn": "DN50",
                "flow_rate_lps": 1.5,
                "height_adj_min_mm": 15,
                "height_adj_max_mm": 96,
                "cutout_mm": "150 x 150 mm",
                "side_inlet": "false",
                "row_text": "2500.55.00 WS50 DN50",
                "attribution_status": "candidate_variant",
                "why_not_promoted": "multiple_candidate_articles",
            },
            {
                "manufacturer": "aco",
                "base_product_id": "aco-assembled-easyflow-1",
                "article_number": "2500.05.00",
                "variant_type": "candidate_body_variant",
                "product_family": "easyflow",
                "source_url": "https://example.test/easyflow/body-2",
                "water_seal_mm": 50,
                "outlet_dn": "DN50",
                "flow_rate_lps": 1.0,
                "height_adj_min_mm": 7,
                "height_adj_max_mm": 75,
                "cutout_mm": "160 x 160 mm",
                "side_inlet": "true",
                "row_text": "2500.05.00 WS50 DN50",
                "attribution_status": "candidate_variant",
                "why_not_promoted": "multiple_candidate_articles",
            },
            {
                "manufacturer": "aco",
                "base_product_id": "aco-assembled-easyflow-1",
                "article_number": "2500.00.00",
                "variant_type": "candidate_body_variant",
                "product_family": "easyflow",
                "source_url": "https://example.test/easyflow/body-3",
                "water_seal_mm": 50,
                "outlet_dn": "DN50",
                "flow_rate_lps": 1.5,
                "height_adj_min_mm": 15,
                "height_adj_max_mm": 96,
                "cutout_mm": "150 x 150 mm",
                "side_inlet": "",
                "row_text": "2500.00.00 WS50 DN50",
                "attribution_status": "candidate_variant",
                "why_not_promoted": "multiple_candidate_articles",
            },
        ]),
        "Final_Assemblies": pd.DataFrame([
            {
                "manufacturer": "aco",
                "product_id": "aco-assembled-easyflow-1",
                "assembled_family": "easyflow",
                "water_seal_mm": 50,
                "outlet_dn": "DN50",
                "flow_rate_lps": None,
                "height_adj_min_mm": None,
                "height_adj_max_mm": None,
                "is_complete_technical_data": False,
                "missing_technical_fields": "flow_rate_lps,height_adj_min_mm,height_adj_max_mm",
                "data_quality_status": "partial",
                "source_status_note": "WS/DN inherited from base row; flow/height ambiguous at current article/variant granularity",
            },
            {
                "manufacturer": "aco",
                "product_id": "aco-assembled-easyflow-2",
                "assembled_family": "easyflow",
                "water_seal_mm": 50,
                "outlet_dn": "DN50",
                "flow_rate_lps": None,
                "height_adj_min_mm": None,
                "height_adj_max_mm": None,
                "is_complete_technical_data": False,
                "missing_technical_fields": "flow_rate_lps,height_adj_min_mm,height_adj_max_mm",
                "data_quality_status": "partial",
                "source_status_note": "WS/DN inherited from base row; flow/height ambiguous at current article/variant granularity",
            },
        ]),
    }
    final = data["Final_Assemblies"]
    data["Final_Set_Details"] = pd.DataFrame([
        {
            "set_id": row["product_id"],
            "assembled_product_id": row["product_id"],
            "assembled_family": row["assembled_family"],
            "manufacturer": row["manufacturer"],
            "product_name": "",
            "base_product_id": "",
            "component_id": "",
            "component_role": "",
            "component_family": "",
            "flow_rate_lps": row["flow_rate_lps"],
            "water_seal_mm": row["water_seal_mm"],
            "outlet_dn": row["outlet_dn"],
            "height_adj_min_mm": row["height_adj_min_mm"],
            "height_adj_max_mm": row["height_adj_max_mm"],
            "is_complete_technical_data": row["is_complete_technical_data"],
            "missing_technical_fields": row["missing_technical_fields"],
            "data_quality_status": row["data_quality_status"],
            "source_status_note": row["source_status_note"],
            "ready_for_benchmark": False,
            "ready_for_customer_view": False,
            "blocked_reason": "flow/height ambiguous at current article/variant granularity",
            "article_variant_status": "multiple_candidate_articles",
            "article_variant_note": "matching Article_Variants include multiple WS50/DN50 candidates; no article variant is selected by default",
            "product_url": "",
            "source_url": "",
            "sources": "",
        }
        for _, row in final.iterrows()
    ])
    return data


def _mplus_final_assembly_dataframe(ready_value):
    rows = []
    for _, mapping in _mplus_compound_mappings_dataframe().iterrows():
        assembled_id = mapping["set_id"].replace("diagnostic-mplus-", "aco-assembled-showerdrain-mplus-")
        rows.append({
            "manufacturer": "aco",
            "product_id": assembled_id,
            "assembled_family": "showerdrain_mplus",
            "product_family": "showerdrain_mplus",
            "family": "showerdrain_mplus",
            "product_name": "ACO ShowerDrain M+ assembled set",
            "system_role": "assembled_system",
            "assembled_from_bom": True,
            "channel_body_id": mapping["channel_body_id"],
            "drain_body_id": mapping["drain_body_id"],
            "grate_id": mapping["grate_id"],
            "source_url_channel_body": mapping["source_url_channel_body"],
            "source_url_drain_body": mapping["source_url_drain_body"],
            "source_url_grate": mapping["source_url_grate"],
            "water_seal_mm": mapping["water_seal_mm"],
            "outlet_dn": mapping["outlet_dn"],
            "height_adj_min_mm": mapping["height_adj_min_mm"],
            "height_adj_max_mm": mapping["height_adj_max_mm"],
            "flow_rate_lps": "",
            "flow_rate_status": "conditional",
            "is_complete_technical_data": False,
            "missing_technical_fields": "flow_rate_lps",
            "data_quality_status": "conditional_parameter_available_production_blocked",
            "ready_for_benchmark": ready_value,
            "ready_for_customer_view": ready_value,
            "blocked_reason": "blocked_pending_conditional_parameter_scoring",
            "source_status_note": "conditional flow values available in Conditional_Technical_Values; scenario scoring not implemented",
        })
    return pd.DataFrame(rows)


def _mplus_final_set_details_dataframe(final_assemblies: pd.DataFrame, ready_value=False):
    return pd.DataFrame([
        {
            "set_id": row["product_id"],
            "assembled_product_id": row["product_id"],
            "assembled_family": "showerdrain_mplus",
            "manufacturer": "aco",
            "product_name": row["product_name"],
            "base_product_id": row["channel_body_id"],
            "component_id": row["drain_body_id"],
            "component_role": "",
            "component_family": "",
            "flow_rate_lps": "",
            "water_seal_mm": row["water_seal_mm"],
            "outlet_dn": row["outlet_dn"],
            "height_adj_min_mm": row["height_adj_min_mm"],
            "height_adj_max_mm": row["height_adj_max_mm"],
            "is_complete_technical_data": False,
            "missing_technical_fields": "flow_rate_lps",
            "data_quality_status": "conditional_parameter_available_production_blocked",
            "source_status_note": row["source_status_note"],
            "ready_for_benchmark": ready_value,
            "ready_for_customer_view": ready_value,
            "blocked_reason": "blocked_pending_conditional_parameter_scoring",
            "article_variant_status": "conditional_parameter_available",
            "article_variant_note": row["source_status_note"],
            "product_url": "",
            "source_url": "",
            "sources": "",
        }
        for _, row in final_assemblies.iterrows()
    ])


def _mplus_only_dataframes(ready_value=0.0):
    final_assemblies = _mplus_final_assembly_dataframe(ready_value)
    products = final_assemblies.drop(columns=["assembled_family"]).copy()
    comparison = products.copy()
    mplus_mappings = _mplus_compound_mappings_dataframe()
    mplus_mappings["set_id"] = final_assemblies["product_id"].tolist()
    conditional_values = _conditional_technical_values_dataframe()
    set_id_map = dict(zip(_mplus_compound_mappings_dataframe()["set_id"], final_assemblies["product_id"]))
    conditional_values["set_id"] = conditional_values["set_id"].map(set_id_map)
    return {
        "Products": products,
        "Comparison": comparison,
        "Scoring_Field_Coverage": products[["manufacturer", "product_id"]].copy(),
        "Candidates_All": pd.DataFrame(),
        "Components": pd.DataFrame(),
        "BOM_Options": pd.DataFrame(),
        "Mplus_Compound_Mappings": mplus_mappings,
        "Eplus_Proposal_Mappings": _eplus_proposal_mappings_dataframe(),
        "Cplus_Compatible_Grate_Evidence": pd.DataFrame(columns=_cplus_evidence_dataframe().columns),
        "Conditional_Technical_Values": conditional_values,
        "Article_Variants": pd.DataFrame(columns=[
            "manufacturer",
            "base_product_id",
            "article_number",
            "variant_type",
            "product_family",
            "source_url",
            "water_seal_mm",
            "outlet_dn",
            "flow_rate_lps",
            "height_adj_min_mm",
            "height_adj_max_mm",
            "cutout_mm",
            "side_inlet",
            "row_text",
            "attribution_status",
            "why_not_promoted",
        ]),
        "Final_Assemblies": final_assemblies,
        "Final_Set_Details": _mplus_final_set_details_dataframe(final_assemblies, False),
    }


def _patch_mplus_only_expectations(mod):
    mod.EXPECTED_SHEET_COUNTS = {
        "Products": 4,
        "Comparison": 4,
        "Scoring_Field_Coverage": 4,
        "Candidates_All": 0,
        "Components": 0,
        "BOM_Options": 0,
        "Final_Assemblies": 4,
        "Final_Set_Details": 4,
        "Mplus_Compound_Mappings": 4,
        "Eplus_Proposal_Mappings": 3,
        "Conditional_Technical_Values": 8,
        "Article_Variants": 0,
    }
    mod.COMPONENTS_MIN_ROWS = 0
    mod.EXPECTED_BOM_OPTION_TYPE_COUNTS = {"optional_accessory": 0, "compatible_grate": 0}
    mod.EXPECTED_ASSEMBLED_PREFIX_COUNTS = {
        "aco-assembled-showerdrain-splus": 0,
        "aco-assembled-showerdrain-c": 0,
        "aco-assembled-showerdrain-mplus": 4,
        "aco-assembled-showerdrain-eplus": 0,
        "aco-assembled-showerdrain-b": 0,
        "aco-assembled-showerdrain-cplus": 0,
    }
    mod.EXPECTED_FINAL_ASSEMBLIES_FAMILY_COUNTS = {"showerdrain_mplus": 4}
    mod.EXPECTED_FINAL_ASSEMBLIES_STATUS_COUNTS = {
        "complete": 0,
        "partial": 0,
        "conditional_parameter_available_production_blocked": 4,
        "missing": 0,
    }
    mod.EXPECTED_FINAL_SET_DETAILS_ROW_COUNT = 4
    mod.EXPECTED_FINAL_SET_DETAILS_FAMILY_COUNTS = {"showerdrain_mplus": 4}
    mod.EXPECTED_FINAL_SET_DETAILS_READY_COUNTS = {True: 0, False: 4}
    mod.CPLUS_EXPECTED = {}
    mod.EASYFLOW_WS50_DN50_EXPECTED_ARTICLES = set()

def _result_for(results, name):
    return next(r for r in results if r.name == name)


def _write_xlsx(path: Path, sheets: dict):
    sheets = dict(sheets)
    comparison = sheets.get("Comparison", pd.DataFrame())
    conditional = sheets.get("Conditional_Technical_Values", pd.DataFrame())
    sheets.setdefault("Scoring_Scenarios", scoring_scenarios_dataframe())
    sheets.setdefault(
        "Comparison_flow_head_10mm",
        build_scenario_comparison(comparison, conditional, "flow_head_10mm"),
    )
    sheets.setdefault(
        "Comparison_flow_head_20mm",
        build_scenario_comparison(comparison, conditional, "flow_head_20mm"),
    )
    with pd.ExcelWriter(path, engine="openpyxl") as writer:
        for name, df in sheets.items():
            df.to_excel(writer, sheet_name=name, index=False)


def _patch_small_expectations(mod, counts):
    mod.EXPECTED_SHEET_COUNTS = counts
    mod.EXPECTED_BOM_OPTION_TYPE_COUNTS = {"optional_accessory": 0, "compatible_grate": 1}
    mod.EXPECTED_ASSEMBLED_PREFIX_COUNTS = {k: 0 for k in mod.EXPECTED_ASSEMBLED_PREFIX_COUNTS}
    mod.EXPECTED_FINAL_ASSEMBLIES_FAMILY_COUNTS = {
        "easyflow": 2,
        "easyflowplus": 0,
        "showerdrain_c": 0,
        "showerdrain_splus": 0,
    }
    mod.EXPECTED_FINAL_ASSEMBLIES_STATUS_COUNTS = {
        "complete": 0,
        "partial": 2,
        "missing": 0,
    }
    mod.EXPECTED_FINAL_SET_DETAILS_ROW_COUNT = counts.get("Final_Set_Details", counts.get("Final_Assemblies", 2))
    mod.EXPECTED_FINAL_SET_DETAILS_FAMILY_COUNTS = mod.EXPECTED_FINAL_ASSEMBLIES_FAMILY_COUNTS.copy()
    mod.EXPECTED_FINAL_SET_DETAILS_READY_COUNTS = {True: 0, False: 2}


def test_default_baseline_counts_updated():
    mod = _load_validator_module()
    assert mod.EXPECTED_SHEET_COUNTS["Candidates_All"] == 118
    assert mod.EXPECTED_SHEET_COUNTS["Components"] == 100
    assert mod.EXPECTED_SHEET_COUNTS["Products"] == 80
    assert mod.EXPECTED_SHEET_COUNTS["Comparison"] == 80
    assert mod.EXPECTED_SHEET_COUNTS["Scoring_Field_Coverage"] == 80
    assert mod.EXPECTED_SHEET_COUNTS["BOM_Options"] == 251
    assert mod.EXPECTED_SHEET_COUNTS["Final_Assemblies"] == 62
    assert mod.EXPECTED_SHEET_COUNTS["Final_Set_Details"] == 62
    assert mod.EXPECTED_SHEET_COUNTS["Cplus_Compatible_Grate_Evidence"] == 30
    assert mod.EXPECTED_SHEET_COUNTS["Mplus_Compound_Mappings"] == 4
    assert mod.EXPECTED_SHEET_COUNTS["Eplus_Proposal_Mappings"] == 3
    assert mod.EXPECTED_SHEET_COUNTS["Conditional_Technical_Values"] == 8
    assert mod.EXPECTED_SHEET_COUNTS["Article_Variants"] == 76
    assert mod.EXPECTED_FINAL_SET_DETAILS_ROW_COUNT == 62
    assert mod.EXPECTED_ASSEMBLED_PREFIX_COUNTS["aco-assembled-showerdrain-cplus"] == 30
    assert mod.EXPECTED_FINAL_ASSEMBLIES_FAMILY_COUNTS["showerdrain_cplus"] == 30


def test_pass_workbook_exits_0(tmp_path):
    mod = _load_validator_module()
    _patch_small_expectations(mod, {"Products": 2, "Comparison": 2, "Scoring_Field_Coverage": 2, "Candidates_All": 2, "Components": 1, "BOM_Options": 1, "Final_Assemblies": 2})
    path = tmp_path / "ok.xlsx"
    _write_xlsx(path, _base_dataframes())
    passed, _ = mod.validate_xlsx(str(path))
    assert passed is True


def test_missing_required_sheet_fails(tmp_path):
    mod = _load_validator_module()
    data = _base_dataframes()
    data.pop("Comparison")
    path = tmp_path / "missing.xlsx"
    _write_xlsx(path, data)
    passed, results = mod.validate_xlsx(str(path))
    assert not passed
    assert any(r.name == "required_sheets" and not r.passed for r in results)


def test_wrong_products_count_fails(tmp_path):
    mod = _load_validator_module()
    path = tmp_path / "wrong_count.xlsx"
    _write_xlsx(path, _base_dataframes())
    passed, results = mod.validate_xlsx(str(path))
    assert not passed
    assert any(r.name == "row_count:Products" and not r.passed for r in results)


def test_bom_self_reference_fails(tmp_path):
    mod = _load_validator_module()
    _patch_small_expectations(mod, {"Products": 2, "Comparison": 2, "Scoring_Field_Coverage": 2, "Candidates_All": 2, "Components": 1, "BOM_Options": 1, "Final_Assemblies": 2})
    data = _base_dataframes()
    data["BOM_Options"].loc[0, "component_id"] = "aco-showerdrain-cplus-standard-h92"
    path = tmp_path / "selfref.xlsx"
    _write_xlsx(path, data)
    passed, results = mod.validate_xlsx(str(path))
    assert not passed
    assert any(r.name == "bom_self_reference" and not r.passed for r in results)


def test_missing_cplus_row_fails(tmp_path):
    mod = _load_validator_module()
    _patch_small_expectations(mod, {"Products": 1, "Comparison": 1, "Scoring_Field_Coverage": 1, "Candidates_All": 2, "Components": 1, "BOM_Options": 1, "Final_Assemblies": 2})
    data = _base_dataframes()
    data["Products"] = data["Products"].iloc[:1].copy()
    data["Comparison"] = data["Comparison"].iloc[:1].copy()
    data["Scoring_Field_Coverage"] = data["Scoring_Field_Coverage"].iloc[:1].copy()
    path = tmp_path / "missing_cplus.xlsx"
    _write_xlsx(path, data)
    passed, results = mod.validate_xlsx(str(path))
    assert not passed
    assert any(r.name == "cplus_presence:Products" and not r.passed for r in results)


def test_compatible_grate_metadata_all_required_snippets_pass(tmp_path):
    mod = _load_validator_module()
    _patch_small_expectations(mod, {"Products": 2, "Comparison": 2, "Scoring_Field_Coverage": 2, "Candidates_All": 2, "Components": 1, "BOM_Options": 1, "Final_Assemblies": 2})
    data = _base_dataframes()
    path = tmp_path / "meta_ok.xlsx"
    _write_xlsx(path, data)
    passed, results = mod.validate_xlsx(str(path))
    assert passed
    assert _result_for(results, "compatible_grate_metadata").passed


def test_compatible_grate_metadata_family_specific_source_limitation_passes(tmp_path):
    mod = _load_validator_module()
    _patch_small_expectations(mod, {"Products": 2, "Comparison": 2, "Scoring_Field_Coverage": 2, "Candidates_All": 2, "Components": 1, "BOM_Options": 1, "Final_Assemblies": 2})
    data = _base_dataframes()
    data["BOM_Options"].loc[0, "option_meta"] = (
        "compatibility_confidence=implicit_family_level; explicit_article_matrix=false; "
        "source_limitation=E+ family compatibility inferred from dimensional grouping; "
        "no explicit article-to-article matrix found"
    )
    path = tmp_path / "meta_family_ok.xlsx"
    _write_xlsx(path, data)
    passed, results = mod.validate_xlsx(str(path))
    assert passed
    assert _result_for(results, "compatible_grate_metadata").passed


def test_compatible_grate_metadata_missing_compatibility_confidence_fails(tmp_path):
    mod = _load_validator_module()
    _patch_small_expectations(mod, {"Products": 2, "Comparison": 2, "Scoring_Field_Coverage": 2, "Candidates_All": 2, "Components": 1, "BOM_Options": 1, "Final_Assemblies": 2})
    data = _base_dataframes()
    data["BOM_Options"].loc[0, "option_meta"] = (
        "explicit_article_matrix=false; source_limitation=family level only; "
        "no explicit article-to-article matrix found"
    )
    path = tmp_path / "meta_missing_conf.xlsx"
    _write_xlsx(path, data)
    passed, results = mod.validate_xlsx(str(path))
    assert not passed
    assert not _result_for(results, "compatible_grate_metadata").passed


def test_compatible_grate_metadata_missing_explicit_article_matrix_fails(tmp_path):
    mod = _load_validator_module()
    _patch_small_expectations(mod, {"Products": 2, "Comparison": 2, "Scoring_Field_Coverage": 2, "Candidates_All": 2, "Components": 1, "BOM_Options": 1, "Final_Assemblies": 2})
    data = _base_dataframes()
    data["BOM_Options"].loc[0, "option_meta"] = (
        "compatibility_confidence=implicit_family_level; source_limitation=family level only; "
        "no explicit article-to-article matrix found"
    )
    path = tmp_path / "meta_missing_explicit_matrix.xlsx"
    _write_xlsx(path, data)
    passed, results = mod.validate_xlsx(str(path))
    assert not passed
    assert not _result_for(results, "compatible_grate_metadata").passed


def test_compatible_grate_metadata_missing_source_limitation_fails(tmp_path):
    mod = _load_validator_module()
    _patch_small_expectations(mod, {"Products": 2, "Comparison": 2, "Scoring_Field_Coverage": 2, "Candidates_All": 2, "Components": 1, "BOM_Options": 1, "Final_Assemblies": 2})
    data = _base_dataframes()
    data["BOM_Options"].loc[0, "option_meta"] = (
        "compatibility_confidence=implicit_family_level; explicit_article_matrix=false; "
        "no explicit article-to-article matrix found"
    )
    path = tmp_path / "meta_missing_source_limitation.xlsx"
    _write_xlsx(path, data)
    passed, results = mod.validate_xlsx(str(path))
    assert not passed
    assert not _result_for(results, "compatible_grate_metadata").passed


def test_compatible_grate_metadata_missing_no_explicit_matrix_sentence_fails(tmp_path):
    mod = _load_validator_module()
    _patch_small_expectations(mod, {"Products": 2, "Comparison": 2, "Scoring_Field_Coverage": 2, "Candidates_All": 2, "Components": 1, "BOM_Options": 1, "Final_Assemblies": 2})
    data = _base_dataframes()
    data["BOM_Options"].loc[0, "option_meta"] = (
        "compatibility_confidence=implicit_family_level; explicit_article_matrix=false; "
        "source_limitation=family level only"
    )
    path = tmp_path / "meta_missing_sentence.xlsx"
    _write_xlsx(path, data)
    passed, results = mod.validate_xlsx(str(path))
    assert not passed
    assert not _result_for(results, "compatible_grate_metadata").passed


def test_final_assemblies_baseline_expectations():
    mod = _load_validator_module()
    assert mod.EXPECTED_SHEET_COUNTS["Final_Assemblies"] == 62
    assert mod.EXPECTED_SHEET_COUNTS["Final_Set_Details"] == 62
    assert mod.EXPECTED_SHEET_COUNTS["Mplus_Compound_Mappings"] == 4
    assert mod.EXPECTED_SHEET_COUNTS["Eplus_Proposal_Mappings"] == 3
    assert mod.EXPECTED_SHEET_COUNTS["Conditional_Technical_Values"] == 8
    assert mod.EXPECTED_SHEET_COUNTS["Article_Variants"] == 76
    assert mod.EXPECTED_FINAL_SET_DETAILS_ROW_COUNT == 62
    assert mod.EXPECTED_FINAL_ASSEMBLIES_FAMILY_COUNTS == {
        "easyflow": 2,
        "easyflowplus": 6,
        "showerdrain_c": 4,
        "showerdrain_splus": 16,
        "showerdrain_mplus": 4,
        "showerdrain_cplus": 30,
    }
    assert mod.EXPECTED_FINAL_ASSEMBLIES_STATUS_COUNTS == {
        "complete": 26,
        "explicit_source_ready_production_assembly": 30,
        "partial": 2,
        "conditional_parameter_available_production_blocked": 4,
        "missing": 0,
    }


def test_missing_final_assemblies_sheet_fails(tmp_path):
    mod = _load_validator_module()
    data = _base_dataframes()
    data.pop("Final_Assemblies")
    path = tmp_path / "missing_final_assemblies.xlsx"
    _write_xlsx(path, data)
    passed, results = mod.validate_xlsx(str(path))
    assert not passed
    assert not _result_for(results, "required_sheets").passed
    assert not _result_for(results, "final_assemblies_sheet_exists").passed


def test_final_assemblies_rejects_non_assembled_product_rows(tmp_path):
    mod = _load_validator_module()
    _patch_small_expectations(mod, {"Products": 2, "Comparison": 2, "Scoring_Field_Coverage": 2, "Candidates_All": 2, "Components": 1, "BOM_Options": 1, "Final_Assemblies": 2})
    data = _base_dataframes()
    data["Final_Assemblies"].loc[0, "product_id"] = "aco-showerdrain-cplus-standard-h92"
    path = tmp_path / "final_non_assembled.xlsx"
    _write_xlsx(path, data)
    passed, results = mod.validate_xlsx(str(path))
    assert not passed
    assert not _result_for(results, "final_assemblies_product_id_prefix").passed
    assert not _result_for(results, "final_assemblies_no_non_assembled_rows").passed


def test_final_assemblies_requires_assembled_family_column(tmp_path):
    mod = _load_validator_module()
    _patch_small_expectations(mod, {"Products": 2, "Comparison": 2, "Scoring_Field_Coverage": 2, "Candidates_All": 2, "Components": 1, "BOM_Options": 1, "Final_Assemblies": 2})
    data = _base_dataframes()
    data["Final_Assemblies"] = data["Final_Assemblies"].drop(columns=["assembled_family"])
    path = tmp_path / "final_missing_family.xlsx"
    _write_xlsx(path, data)
    passed, results = mod.validate_xlsx(str(path))
    assert not passed
    assert not _result_for(results, "final_assemblies_assembled_family_column").passed


def test_final_assemblies_family_count_mismatch_fails(tmp_path):
    mod = _load_validator_module()
    _patch_small_expectations(mod, {"Products": 2, "Comparison": 2, "Scoring_Field_Coverage": 2, "Candidates_All": 2, "Components": 1, "BOM_Options": 1, "Final_Assemblies": 2})
    data = _base_dataframes()
    data["Final_Assemblies"].loc[1, "assembled_family"] = "easyflowplus"
    path = tmp_path / "final_family_mismatch.xlsx"
    _write_xlsx(path, data)
    passed, results = mod.validate_xlsx(str(path))
    assert not passed
    assert not _result_for(results, "final_assemblies_family_count:easyflow").passed
    assert not _result_for(results, "final_assemblies_family_count:easyflowplus").passed


def test_final_assemblies_easyflow_values_must_match(tmp_path):
    mod = _load_validator_module()
    _patch_small_expectations(mod, {"Products": 2, "Comparison": 2, "Scoring_Field_Coverage": 2, "Candidates_All": 2, "Components": 1, "BOM_Options": 1, "Final_Assemblies": 2})
    data = _base_dataframes()
    data["Final_Assemblies"].loc[0, "water_seal_mm"] = 25
    data["Final_Assemblies"].loc[1, "outlet_dn"] = "DN40"
    path = tmp_path / "final_easyflow_bad_values.xlsx"
    _write_xlsx(path, data)
    passed, results = mod.validate_xlsx(str(path))
    assert not passed
    assert not _result_for(results, "final_assemblies_easyflow_value:water_seal_mm").passed
    assert not _result_for(results, "final_assemblies_easyflow_value:outlet_dn").passed


def test_final_assemblies_easyflow_ambiguous_fields_must_stay_empty(tmp_path):
    mod = _load_validator_module()
    _patch_small_expectations(mod, {"Products": 2, "Comparison": 2, "Scoring_Field_Coverage": 2, "Candidates_All": 2, "Components": 1, "BOM_Options": 1, "Final_Assemblies": 2})
    data = _base_dataframes()
    data["Final_Assemblies"].loc[0, "flow_rate_lps"] = 0.8
    data["Final_Assemblies"].loc[0, "height_adj_min_mm"] = 80
    data["Final_Assemblies"].loc[0, "height_adj_max_mm"] = 128
    path = tmp_path / "final_easyflow_filled_ambiguous.xlsx"
    _write_xlsx(path, data)
    passed, results = mod.validate_xlsx(str(path))
    assert not passed
    assert not _result_for(results, "final_assemblies_easyflow_empty:flow_rate_lps").passed
    assert not _result_for(results, "final_assemblies_easyflow_empty:height_adj_min_mm").passed
    assert not _result_for(results, "final_assemblies_easyflow_empty:height_adj_max_mm").passed


def test_final_assemblies_requires_completeness_columns(tmp_path):
    mod = _load_validator_module()
    _patch_small_expectations(mod, {"Products": 2, "Comparison": 2, "Scoring_Field_Coverage": 2, "Candidates_All": 2, "Components": 1, "BOM_Options": 1, "Final_Assemblies": 2})
    data = _base_dataframes()
    data["Final_Assemblies"] = data["Final_Assemblies"].drop(columns=["data_quality_status"])
    path = tmp_path / "final_missing_completeness_column.xlsx"
    _write_xlsx(path, data)
    passed, results = mod.validate_xlsx(str(path))
    assert not passed
    assert not _result_for(results, "final_assemblies_completeness_columns").passed


def test_final_assemblies_easyflow_completeness_status_must_match(tmp_path):
    mod = _load_validator_module()
    _patch_small_expectations(mod, {"Products": 2, "Comparison": 2, "Scoring_Field_Coverage": 2, "Candidates_All": 2, "Components": 1, "BOM_Options": 1, "Final_Assemblies": 2})
    data = _base_dataframes()
    data["Final_Assemblies"].loc[0, "data_quality_status"] = "complete"
    data["Final_Assemblies"].loc[0, "is_complete_technical_data"] = True
    data["Final_Assemblies"].loc[0, "missing_technical_fields"] = "height_adj_min_mm,flow_rate_lps,height_adj_max_mm"
    data["Final_Assemblies"].loc[0, "source_status_note"] = "partial technical data"
    path = tmp_path / "final_bad_easyflow_completeness.xlsx"
    _write_xlsx(path, data)
    passed, results = mod.validate_xlsx(str(path))
    assert not passed
    assert not _result_for(results, "final_assemblies_status_count:complete").passed
    assert not _result_for(results, "final_assemblies_status_count:partial").passed
    assert not _result_for(results, "final_assemblies_easyflow_status_partial").passed
    assert not _result_for(results, "final_assemblies_easyflow_is_complete_false").passed
    assert not _result_for(results, "final_assemblies_easyflow_missing_fields").passed
    assert not _result_for(results, "final_assemblies_easyflow_source_note").passed


def test_mplus_compound_mappings_validation_rejects_product_flow_write(tmp_path):
    mod = _load_validator_module()
    counts = {
        "Products": 2,
        "Comparison": 2,
        "Scoring_Field_Coverage": 2,
        "Candidates_All": 2,
        "Components": 1,
        "BOM_Options": 1,
        "Final_Assemblies": 2,
        "Final_Set_Details": 2,
        "Mplus_Compound_Mappings": 4,
        "Eplus_Proposal_Mappings": 3,
        "Conditional_Technical_Values": 8,
        "Article_Variants": 3,
    }
    _patch_small_expectations(mod, counts)
    mod.EXPECTED_SHEET_COUNTS = counts
    data = _base_dataframes()
    data["Mplus_Compound_Mappings"].loc[0, "flow_rate_lps"] = "0.4"
    path = tmp_path / "mplus_bad_flow.xlsx"
    _write_xlsx(path, data)

    passed, results = mod.validate_xlsx(str(path))

    assert not passed
    assert not _result_for(results, "mplus_compound_mappings_empty:flow_rate_lps").passed


def test_mplus_compound_mappings_validation_passes_expected_diagnostic_rows(tmp_path):
    mod = _load_validator_module()
    counts = {
        "Products": 2,
        "Comparison": 2,
        "Scoring_Field_Coverage": 2,
        "Candidates_All": 2,
        "Components": 1,
        "BOM_Options": 1,
        "Final_Assemblies": 2,
        "Final_Set_Details": 2,
        "Mplus_Compound_Mappings": 4,
        "Eplus_Proposal_Mappings": 3,
        "Conditional_Technical_Values": 8,
        "Article_Variants": 3,
    }
    _patch_small_expectations(mod, counts)
    mod.EXPECTED_SHEET_COUNTS = counts
    path = tmp_path / "mplus_ok.xlsx"
    _write_xlsx(path, _base_dataframes())

    passed, results = mod.validate_xlsx(str(path))

    assert passed
    assert _result_for(results, "row_count:Mplus_Compound_Mappings").passed
    assert _result_for(results, "mplus_compound_mappings_expected_articles").passed
    assert _result_for(results, "mplus_compound_mappings_empty:selected_default_flow_rate_lps").passed
    assert _result_for(results, "conditional_technical_values_mplus_row_count").passed
    assert _result_for(results, "conditional_technical_values_two_flow_rows_per_mplus_set").passed


def test_bool_series_eq_accepts_pandas_numeric_false_and_rejects_numeric_true():
    mod = _load_validator_module()
    false_values = [False, np.bool_(False), "False", "false", "FALSE", "0", 0, 0.0, np.int64(0), np.float64(0.0)]
    true_values = [True, np.bool_(True), "True", "true", "TRUE", "1", 1, 1.0, np.int64(1), np.float64(1.0)]
    missing_values = [None, "", np.nan]
    df = pd.DataFrame({"value": false_values + true_values + missing_values})

    is_false = mod._bool_series_eq(df, "value", False)
    is_true = mod._bool_series_eq(df, "value", True)

    assert is_false.iloc[:len(false_values)].all()
    assert not is_false.iloc[len(false_values):].any()
    assert is_true.iloc[len(false_values):len(false_values) + len(true_values)].all()
    assert not is_true.iloc[:len(false_values)].any()
    assert not is_true.iloc[-len(missing_values):].any()
    assert not is_false.iloc[-len(missing_values):].any()


def test_mplus_final_assemblies_numeric_zero_readiness_passes_validator(tmp_path):
    mod = _load_validator_module()
    _patch_mplus_only_expectations(mod)
    path = tmp_path / "mplus_numeric_zero_ready.xlsx"
    _write_xlsx(path, _mplus_only_dataframes(ready_value=0.0))

    passed, results = mod.validate_xlsx(str(path))

    assert passed
    assert _result_for(results, "final_assemblies_mplus_ready_for_benchmark_false").passed
    assert _result_for(results, "final_assemblies_mplus_ready_for_customer_view_false").passed


def test_mplus_final_assemblies_numeric_one_readiness_fails_validator(tmp_path):
    mod = _load_validator_module()
    _patch_mplus_only_expectations(mod)
    path = tmp_path / "mplus_numeric_one_ready.xlsx"
    _write_xlsx(path, _mplus_only_dataframes(ready_value=1.0))

    passed, results = mod.validate_xlsx(str(path))

    assert not passed
    assert not _result_for(results, "final_assemblies_mplus_ready_for_benchmark_false").passed
    assert not _result_for(results, "final_assemblies_mplus_ready_for_customer_view_false").passed


def test_scenario_mplus_flow_validation_rejects_wrong_explicit_value(tmp_path):
    mod = _load_validator_module()
    _patch_mplus_only_expectations(mod)
    data = _mplus_only_dataframes(ready_value=0.0)
    data["Comparison_flow_head_10mm"] = build_scenario_comparison(
        data["Comparison"], data["Conditional_Technical_Values"], "flow_head_10mm"
    )
    data["Comparison_flow_head_10mm"].loc[
        data["Comparison_flow_head_10mm"]["product_family"].eq("showerdrain_mplus"),
        "flow_rate_lps",
    ] = 0.46
    path = tmp_path / "wrong_scenario_flow.xlsx"
    _write_xlsx(path, data)

    passed, results = mod.validate_xlsx(str(path))

    assert passed is False
    assert not _result_for(results, "scenario_mplus_flow:Comparison_flow_head_10mm").passed


def test_assembled_family_count_does_not_mix_showerdrain_c_and_cplus():
    mod = _load_validator_module()
    products = pd.DataFrame({
        "product_id": (
            [f"aco-assembled-showerdrain-c-base-{index}__grate-{index}" for index in range(4)]
            + [
                f"aco-assembled-showerdrain-cplus-base-{index}__grate-{index}"
                for index in range(30)
            ]
        )
    })

    families = mod._assembled_family_series(products)

    assert int(families.eq("showerdrain_c").sum()) == 4
    assert int(families.eq("showerdrain_cplus").sum()) == 30
    assert int(families.eq("showerdrain_c").sum()) != 34
