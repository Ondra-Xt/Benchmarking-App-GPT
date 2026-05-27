import os
from pathlib import Path

import pandas as pd

import tools.report_aco_xlsx_quality as report_mod


def _write_xlsx(path: Path, sheets: dict):
    with pd.ExcelWriter(path, engine="openpyxl") as writer:
        for name, df in sheets.items():
            df.to_excel(writer, sheet_name=name, index=False)


def _valid_sheets():
    products = pd.DataFrame([
        {"manufacturer": "aco", "product_id": "aco-showerdrain-splus-1", "family": "ShowerDrain S+", "name": "ACO ShowerDrain S+", "system_role": "drain_unit", "assembled_from_bom": True},
        {"manufacturer": "aco", "product_id": "aco-showerdrain-c-1", "family": "ShowerDrain C", "name": "ACO ShowerDrain C", "system_role": "drain_unit", "assembled_from_bom": True},
        {"manufacturer": "aco", "product_id": "aco-easyflow-1", "family": "Easyflow", "name": "ACO Easyflow", "system_role": "drain_unit", "assembled_from_bom": True},
        {"manufacturer": "aco", "product_id": "aco-easyflow-plus-1", "family": "Easyflow+", "name": "ACO Easyflow+", "system_role": "drain_unit", "assembled_from_bom": True},
    ])
    comparison = products[["manufacturer", "product_id", "family"]].copy()
    components = pd.DataFrame([
        {"manufacturer": "aco", "product_id": "aco-comp-grate-1", "component_id": "aco-comp-grate-1", "family": "ShowerDrain S+", "component_role": "grate", "option_type": "compatible_grate"},
        {"manufacturer": "aco", "product_id": "aco-showerdrain-c-1", "component_id": "aco-comp-accessory-1", "family": "ShowerDrain C", "component_role": "accessory", "option_type": "optional_accessory", "system_role": "drain_unit"},
    ])
    bom = pd.DataFrame([
        {"manufacturer": "aco", "product_id": "aco-showerdrain-splus-1", "component_id": "aco-comp-grate-1", "option_type": "compatible_grate"},
        {"manufacturer": "aco", "product_id": "aco-showerdrain-c-1", "component_id": "aco-comp-accessory-1", "option_type": "optional_accessory"},
        {"manufacturer": "aco", "product_id": "aco-self", "component_id": "aco-self", "option_type": "compatible_grate"},
    ])
    coverage = pd.DataFrame([
        {"manufacturer": "aco", "product_id": "aco-showerdrain-splus-1", "family": "ShowerDrain S+", "score_flow": 1.2, "score_noise": None},
        {"manufacturer": "aco", "product_id": "aco-showerdrain-c-1", "family": "ShowerDrain C", "score_flow": None, "score_noise": None},
    ])
    return {
        "Products": products,
        "Comparison": comparison,
        "Components": components,
        "BOM_Options": bom,
        "Scoring_Field_Coverage": coverage,
    }


def test_generates_report_from_valid_xlsx_path(tmp_path, capsys):
    xlsx = tmp_path / "benchmark_output.xlsx"
    _write_xlsx(xlsx, _valid_sheets())
    rc = report_mod.main(["--xlsx", str(xlsx)])
    out = capsys.readouterr().out
    assert rc == 0
    assert "1. Selected workbook" in out
    assert "8. Overall report status" in out


def test_fails_cleanly_when_xlsx_missing(capsys):
    rc = report_mod.main(["--xlsx", "not_found.xlsx"])
    out = capsys.readouterr().out
    assert rc != 0
    assert "ERROR: XLSX file does not exist" in out


def test_fails_when_required_sheets_missing(tmp_path, capsys):
    xlsx = tmp_path / "benchmark_output.xlsx"
    _write_xlsx(xlsx, {"Products": pd.DataFrame([{"manufacturer": "aco", "product_id": "a"}])})
    rc = report_mod.main(["--xlsx", str(xlsx)])
    out = capsys.readouterr().out
    assert rc != 0
    assert "Required sheets missing" in out


def test_fails_when_no_aco_rows(tmp_path, capsys):
    non_aco = {}
    for name, df in _valid_sheets().items():
        mutated = df.copy()
        if "manufacturer" in mutated.columns:
            mutated["manufacturer"] = "other"
        for col in ["product_id", "component_id", "name", "family"]:
            if col in mutated.columns:
                mutated[col] = mutated[col].astype(str).str.replace("aco", "other", case=False, regex=False)
        non_aco[name] = mutated
    xlsx = tmp_path / "benchmark_output.xlsx"
    _write_xlsx(xlsx, non_aco)
    rc = report_mod.main(["--xlsx", str(xlsx)])
    out = capsys.readouterr().out
    assert rc != 0
    assert "No ACO rows found" in out


def test_reports_assembled_family_counts(tmp_path, capsys):
    xlsx = tmp_path / "benchmark_output.xlsx"
    _write_xlsx(xlsx, _valid_sheets())
    rc = report_mod.main(["--xlsx", str(xlsx)])
    out = capsys.readouterr().out
    assert rc == 0
    assert "ShowerDrain S+: 1" in out
    assert "ShowerDrain C: 1" in out
    assert "Easyflow: 1" in out
    assert "Easyflow+: 1" in out


def test_reports_scoring_field_missing_frequency(tmp_path, capsys):
    xlsx = tmp_path / "benchmark_output.xlsx"
    _write_xlsx(xlsx, _valid_sheets())
    rc = report_mod.main(["--xlsx", str(xlsx)])
    out = capsys.readouterr().out
    assert rc == 0
    assert "field=score_noise present=0 missing=2 missing_pct=100.0%" in out


def test_dir_selects_latest_benchmark_output(tmp_path, capsys):
    older = tmp_path / "benchmark_output_old.xlsx"
    newer = tmp_path / "benchmark_output_new.xlsx"
    _write_xlsx(older, _valid_sheets())
    _write_xlsx(newer, _valid_sheets())
    older.touch()
    newer.touch()
    older_m = older.stat().st_mtime + 1
    newer_m = older.stat().st_mtime + 10
    os.utime(older, (older_m, older_m))
    os.utime(newer, (newer_m, newer_m))

    rc = report_mod.main(["--dir", str(tmp_path)])
    out = capsys.readouterr().out
    assert rc == 0
    assert f"Selected XLSX: {newer}" in out
