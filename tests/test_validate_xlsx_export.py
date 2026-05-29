import importlib.util
from pathlib import Path

import pandas as pd


def _load_validator_module():
    spec = importlib.util.spec_from_file_location("validate_xlsx_export", "tools/validate_xlsx_export.py")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def _base_dataframes():
    products = pd.DataFrame([
        {"manufacturer": "aco", "product_id": "aco-showerdrain-cplus-standard-h92", "system_role": "drain_unit", "flow_rate_lps": 0.91, "water_seal_mm": 50, "outlet_dn": "DN50", "height_adj_min_mm": 80, "height_adj_max_mm": 128},
        {"manufacturer": "aco", "product_id": "aco-showerdrain-cplus-low-h69", "system_role": "drain_unit", "flow_rate_lps": 0.62, "water_seal_mm": 25, "outlet_dn": "DN40", "height_adj_min_mm": 57, "height_adj_max_mm": 128},
    ])
    return {
        "Products": products,
        "Comparison": products[["manufacturer", "product_id", "system_role"]].copy(),
        "Scoring_Field_Coverage": products[["manufacturer", "product_id"]].copy(),
        "Components": pd.DataFrame([{"manufacturer": "aco", "product_id": "aco-comp-1", "system_role": "component"}]),
        "Candidates_All": pd.DataFrame([{"product_id": "c1"}, {"product_id": "c2"}]),
        "BOM_Options": pd.DataFrame([{
            "manufacturer": "aco", "product_id": "aco-showerdrain-cplus-standard-h92", "component_id": "aco-comp-1", "option_type": "compatible_grate",
            "option_meta": "compatibility_confidence=implicit_family_level; explicit_article_matrix=false; source_limitation=; no explicit article-to-article matrix found"
        }]),
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

def _result_for(results, name):
    return next(r for r in results if r.name == name)


def _write_xlsx(path: Path, sheets: dict):
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


def test_default_baseline_counts_updated():
    mod = _load_validator_module()
    assert mod.EXPECTED_SHEET_COUNTS["Candidates_All"] == 118
    assert mod.EXPECTED_SHEET_COUNTS["Components"] == 100
    assert mod.EXPECTED_SHEET_COUNTS["Final_Assemblies"] == 28


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
    assert mod.EXPECTED_SHEET_COUNTS["Final_Assemblies"] == 28
    assert mod.EXPECTED_FINAL_ASSEMBLIES_FAMILY_COUNTS == {
        "easyflow": 2,
        "easyflowplus": 6,
        "showerdrain_c": 4,
        "showerdrain_splus": 16,
    }
    assert mod.EXPECTED_FINAL_ASSEMBLIES_STATUS_COUNTS == {
        "complete": 26,
        "partial": 2,
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
