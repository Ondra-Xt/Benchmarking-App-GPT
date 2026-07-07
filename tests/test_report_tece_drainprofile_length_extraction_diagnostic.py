from __future__ import annotations

import hashlib
import json
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path

import pytest

from tools import report_tece_drainprofile_length_extraction_diagnostic as diag


@dataclass(frozen=True)
class Row:
    article_number: str
    tece_family_candidate: str
    tece_article_role_candidate: str
    evidence_text: str = ""
    source_file: str = "fixture.txt"
    product_family: str = ""
    product_name: str = ""


@dataclass(frozen=True)
class Report:
    rows: list[Row]


@pytest.fixture()
def source_pack(tmp_path: Path) -> Path:
    path = tmp_path / "source_pack"
    path.mkdir()
    (path / "input.txt").write_text("immutable source", encoding="utf-8")
    return path


@pytest.fixture()
def fake_inventory(monkeypatch: pytest.MonkeyPatch) -> list[Row]:
    rows: list[Row] = []
    rows.extend(Row(f"673{i:03d}", "TECEdrainprofile", "drain_body", f"TECEdrainprofile Ablauf Nennlänge: {700 + i * 10} mm DN 50 Sperrwasserhöhe 50 mm RG 1 LE 1 Best.-Nr. 673{i:03d}", f"body{i}.txt") for i in range(1, 13))
    rows.extend(Row(f"670{i:03d}", "TECEdrainprofile", "profile_cover", f"TECEdrainprofile Profilabdeckung Tabelle Nennlänge {700 + i * 10} mm Best.-Nr. 670{i:03d}", f"cover{i}.txt") for i in range(1, 31))
    rows += [
        Row("679999", "TECEdrainprofile", "accessory", "Zubehör Nennlänge 700 mm"),
        Row("679998", "TECEdrainprofile", "complete_set", "Komplettset Nennlänge 800 mm"),
        Row("679997", "TECEdrainprofile", "unknown", "unknown 900 mm"),
        Row("650000", "TECEdrainline", "drain_body", "TECEdrainline Nennlänge 800 mm"),
        Row("3601050", "TECEdrainpoint S", "drain_body", "TECEdrainpoint S 1000 mm"),
        Row("680000", "TECEdrainway", "drain_body", "TECEdrainway 1200 mm"),
    ]
    while len(rows) < 436:
        rows.append(Row(f"9{len(rows):05d}", "Other", "unknown", "other"))
    monkeypatch.setattr(diag, "load_source_pack", lambda _path: Report(rows))
    return rows


def build(tmp_path: Path, source_pack: Path) -> tuple[list[dict], dict]:
    return diag.build_report(source_pack, out=tmp_path / "lengths.csv")


def test_valid_report_produces_exactly_42_diagnostic_rows(tmp_path: Path, source_pack: Path, fake_inventory: list[Row]) -> None:
    rows, report = build(tmp_path, source_pack)
    assert report["valid"] is True
    assert report["input_inventory_row_count"] == 436
    assert report["family_inventory_row_count"] == 45
    assert report["drain_body_count"] == 12
    assert report["profile_cover_count"] == 30
    assert report["target_row_count"] == 42
    assert len(rows) == 42


def test_only_tecedrainprofile_is_included(tmp_path: Path, source_pack: Path, fake_inventory: list[Row]) -> None:
    rows, _ = build(tmp_path, source_pack)
    assert {r["family"] for r in rows} == {"TECEdrainprofile"}
    assert {"650000", "3601050", "680000"}.isdisjoint({r["article_number"] for r in rows})


def test_only_drain_body_and_profile_cover_roles_are_included(tmp_path: Path, source_pack: Path, fake_inventory: list[Row]) -> None:
    rows, report = build(tmp_path, source_pack)
    assert set(report["role_counts"]) == {"drain_body", "profile_cover"}
    assert {r["article_role"] for r in rows} == {"drain_body", "profile_cover"}


@pytest.mark.parametrize("article", ["679999", "679998", "679997"])
def test_accessory_complete_set_and_unknown_rows_are_excluded(article: str, tmp_path: Path, source_pack: Path, fake_inventory: list[Row]) -> None:
    rows, _ = build(tmp_path, source_pack)
    assert article not in {r["article_number"] for r in rows}


def test_diagnostic_ids_are_deterministic_and_sequential(tmp_path: Path, source_pack: Path, fake_inventory: list[Row]) -> None:
    rows, _ = build(tmp_path, source_pack)
    assert [r["length_diagnostic_id"] for r in rows][:3] == ["TECE-DP-LEN-000001", "TECE-DP-LEN-000002", "TECE-DP-LEN-000003"]
    assert rows[-1]["length_diagnostic_id"] == "TECE-DP-LEN-000042"


def test_single_nennlaenge_pattern_extracts_selected_length() -> None:
    values, explicit, _ = diag.extract_length_candidates("Nennlänge: 700 mm Best.-Nr. 670700", "670700")
    assert values == [700]
    row = diag._diagnostic_row(Row("670700", "TECEdrainprofile", "profile_cover", "Nennlänge 700 mm"), 1)
    assert row["selected_diagnostic_length_mm"] == "700"
    assert row["extraction_confidence"] == "high"
    assert explicit is True


def test_multiple_length_candidates_do_not_select_single_length() -> None:
    row = diag._diagnostic_row(Row("670700", "TECEdrainprofile", "profile_cover", "700 mm 800 mm 900 mm 1000 mm 1200 mm"), 1)
    assert row["extraction_status"] == "multiple_length_candidates_found"
    assert row["selected_diagnostic_length_mm"] == ""
    assert row["candidate_length_values_mm"] == "700;800;900;1000;1200"


def test_no_length_candidate_remains_unresolved() -> None:
    row = diag._diagnostic_row(Row("670700", "TECEdrainprofile", "profile_cover", "DN 50 RG 1 LE 1"), 1)
    assert row["extraction_status"] == "no_length_candidate_found"
    assert row["selected_diagnostic_length_mm"] == ""


def test_article_numbers_are_not_misread_as_lengths() -> None:
    assert diag.extract_length_candidates("Best.-Nr. 670700 Artikelnummer 673001", "670700")[0] == []


def test_dn_water_seal_rg_package_unit_numbers_are_not_misread_as_nominal_lengths() -> None:
    text = "DN 50 Sperrwasserhöhe 700 mm water seal 800 mm RG 900 LE 1 1 St."
    assert diag.extract_length_candidates(text, "")[0] == []


@pytest.mark.parametrize("field", ["production_safe", "ready_for_benchmark", "ready_for_customer_view"])
def test_leakage_fails(field: str) -> None:
    row = diag._diagnostic_row(Row("670700", "TECEdrainprofile", "profile_cover", "Nennlänge 700 mm"), 1)
    row[field] = "true"
    gates = diag.validate_rows([row])
    assert gates["production_leakage_rows"] or gates["readiness_leakage_rows"]


def test_diagnostic_only_note_is_present(tmp_path: Path, source_pack: Path, fake_inventory: list[Row]) -> None:
    _, report = build(tmp_path, source_pack)
    assert report["diagnostic_only_note"] == diag.DIAGNOSTIC_ONLY_NOTE


def _hash_tree(path: Path) -> dict[str, str]:
    return {str(p.relative_to(path)): hashlib.sha256(p.read_bytes()).hexdigest() for p in sorted(path.rglob("*")) if p.is_file()}


def test_source_pack_immutability(tmp_path: Path, source_pack: Path, fake_inventory: list[Row]) -> None:
    before = _hash_tree(source_pack)
    build(tmp_path, source_pack)
    assert _hash_tree(source_pack) == before


def test_tecedrainline_reviewed_overlay_artifacts_are_not_changed(tmp_path: Path, source_pack: Path, fake_inventory: list[Row]) -> None:
    overlay_files = [p for p in Path(".").rglob("*tece*overlay*") if p.is_file() and ".git" not in p.parts]
    before = {str(p): hashlib.sha256(p.read_bytes()).hexdigest() for p in overlay_files}
    build(tmp_path, source_pack)
    assert {str(p): hashlib.sha256(p.read_bytes()).hexdigest() for p in overlay_files} == before


def test_cli_writes_csv_and_json(tmp_path: Path, source_pack: Path, fake_inventory: list[Row]) -> None:
    csv_path = tmp_path / "out.csv"; json_path = tmp_path / "out.json"
    rc = diag.main(["--source-pack", str(source_pack), "--out", str(csv_path), "--json-out", str(json_path), "--json"])
    assert rc == 0
    assert json.loads(json_path.read_text(encoding="utf-8"))["valid"] is True
    assert csv_path.read_text(encoding="utf-8-sig").startswith("length_diagnostic_id,")


def test_aco_canonical_baseline_remains_stable() -> None:
    pytest.importorskip("openpyxl")
    out = Path("/tmp/benchmark_aco_canonical_drainprofile_length_test.xlsx")
    export = subprocess.run([sys.executable, "tools/export_canonical_aco_benchmark_xlsx.py", "--out", str(out)], text=True, capture_output=True)
    if export.returncode != 0:
        pytest.skip("ACO canonical export baseline is unavailable in this checkout: " + (export.stderr or export.stdout)[:500])
    validation = subprocess.run([sys.executable, "tools/validate_xlsx_export.py", str(out)], check=True, text=True, capture_output=True)
    result = subprocess.run([sys.executable, "tools/report_aco_final_baseline.py", "--xlsx", str(out)], check=True, text=True, capture_output=True)
    assert "OVERALL: PASS" in validation.stdout
    assert "OVERALL: ACO_BASELINE_STABLE" in result.stdout
