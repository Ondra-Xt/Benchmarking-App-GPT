import os
from pathlib import Path

import pandas as pd

import tools.validate_latest_xlsx_export as latest_mod
import tools.validate_xlsx_export as validator_mod


def _write_xlsx(path: Path, sheets: dict):
    with pd.ExcelWriter(path, engine="openpyxl") as writer:
        for name, df in sheets.items():
            df.to_excel(writer, sheet_name=name, index=False)


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


def _patch_small_expectations(monkeypatch):
    monkeypatch.setattr(validator_mod, "EXPECTED_SHEET_COUNTS", {"Products": 2, "Comparison": 2, "Scoring_Field_Coverage": 2, "Candidates_All": 2, "Components": 1, "BOM_Options": 1})
    monkeypatch.setattr(validator_mod, "EXPECTED_BOM_OPTION_TYPE_COUNTS", {"optional_accessory": 0, "compatible_grate": 1})
    monkeypatch.setattr(validator_mod, "EXPECTED_ASSEMBLED_PREFIX_COUNTS", {k: 0 for k in validator_mod.EXPECTED_ASSEMBLED_PREFIX_COUNTS})


def test_finds_newest_benchmark_output_and_validates(tmp_path, monkeypatch, capsys):
    _patch_small_expectations(monkeypatch)
    older = tmp_path / "benchmark_output_old.xlsx"
    newer = tmp_path / "benchmark_output_new.xlsx"
    _write_xlsx(older, _base_dataframes())
    _write_xlsx(newer, _base_dataframes())
    older.touch()
    newer.touch()
    newer_mtime = older.stat().st_mtime + 5
    older_mtime = older.stat().st_mtime + 1
    os.utime(older, (older_mtime, older_mtime))
    os.utime(newer, (newer_mtime, newer_mtime))

    rc = latest_mod.main(["--dir", str(tmp_path)])
    out = capsys.readouterr().out
    assert rc == 0
    assert f"Selected XLSX: {newer}" in out
    assert "OVERALL: PASS" in out


def test_fails_when_no_xlsx_files(tmp_path, capsys):
    rc = latest_mod.main(["--dir", str(tmp_path)])
    assert rc != 0
    assert "No .xlsx files found" in capsys.readouterr().out


def test_fails_when_no_benchmark_like_xlsx_without_fallback(tmp_path, monkeypatch, capsys):
    _patch_small_expectations(monkeypatch)
    _write_xlsx(tmp_path / "random_export.xlsx", _base_dataframes())
    rc = latest_mod.main(["--dir", str(tmp_path)])
    assert rc != 0
    assert "No benchmark-like .xlsx files found" in capsys.readouterr().out


def test_include_any_xlsx_uses_newest_generic_xlsx(tmp_path, monkeypatch, capsys):
    _patch_small_expectations(monkeypatch)
    older = tmp_path / "a.xlsx"
    newer = tmp_path / "z.xlsx"
    _write_xlsx(older, _base_dataframes())
    _write_xlsx(newer, _base_dataframes())
    older.touch()
    newer.touch()
    newer_mtime = older.stat().st_mtime + 5
    older_mtime = older.stat().st_mtime + 1
    os.utime(older, (older_mtime, older_mtime))
    os.utime(newer, (newer_mtime, newer_mtime))

    rc = latest_mod.main(["--dir", str(tmp_path), "--include-any-xlsx"])
    out = capsys.readouterr().out
    assert rc == 0
    assert f"Selected XLSX: {newer}" in out


def test_propagates_validation_failure_exit_code(tmp_path, capsys):
    _write_xlsx(tmp_path / "benchmark_output_invalid.xlsx", {"Products": pd.DataFrame([{"product_id": "p1"}])})
    rc = latest_mod.main(["--dir", str(tmp_path)])
    assert rc != 0
    assert "OVERALL: FAIL" in capsys.readouterr().out
