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
    }


def _write_xlsx(path: Path, sheets: dict):
    with pd.ExcelWriter(path, engine="openpyxl") as writer:
        for name, df in sheets.items():
            df.to_excel(writer, sheet_name=name, index=False)


def _patch_small_expectations(mod, counts):
    mod.EXPECTED_SHEET_COUNTS = counts
    mod.EXPECTED_BOM_OPTION_TYPE_COUNTS = {"optional_accessory": 0, "compatible_grate": 1}
    mod.EXPECTED_ASSEMBLED_PREFIX_COUNTS = {k: 0 for k in mod.EXPECTED_ASSEMBLED_PREFIX_COUNTS}


def test_pass_workbook_exits_0(tmp_path):
    mod = _load_validator_module()
    _patch_small_expectations(mod, {"Products": 2, "Comparison": 2, "Scoring_Field_Coverage": 2, "Candidates_All": 2, "Components": 1, "BOM_Options": 1})
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
    _patch_small_expectations(mod, {"Products": 2, "Comparison": 2, "Scoring_Field_Coverage": 2, "Candidates_All": 2, "Components": 1, "BOM_Options": 1})
    data = _base_dataframes()
    data["BOM_Options"].loc[0, "component_id"] = "aco-showerdrain-cplus-standard-h92"
    path = tmp_path / "selfref.xlsx"
    _write_xlsx(path, data)
    passed, results = mod.validate_xlsx(str(path))
    assert not passed
    assert any(r.name == "bom_self_reference" and not r.passed for r in results)


def test_missing_cplus_row_fails(tmp_path):
    mod = _load_validator_module()
    _patch_small_expectations(mod, {"Products": 1, "Comparison": 1, "Scoring_Field_Coverage": 1, "Candidates_All": 2, "Components": 1, "BOM_Options": 1})
    data = _base_dataframes()
    data["Products"] = data["Products"].iloc[:1].copy()
    data["Comparison"] = data["Comparison"].iloc[:1].copy()
    data["Scoring_Field_Coverage"] = data["Scoring_Field_Coverage"].iloc[:1].copy()
    path = tmp_path / "missing_cplus.xlsx"
    _write_xlsx(path, data)
    passed, results = mod.validate_xlsx(str(path))
    assert not passed
    assert any(r.name == "cplus_presence:Products" and not r.passed for r in results)
