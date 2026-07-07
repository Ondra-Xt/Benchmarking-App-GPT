from __future__ import annotations

import hashlib
import json
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path

import pytest

from tools import report_tece_drainprofile_compatibility_pair_audit as audit


@dataclass(frozen=True)
class Row:
    article_number: str
    tece_family_candidate: str
    tece_article_role_candidate: str
    nominal_length_mm: object = ""
    source_file: str = "fixture.txt"
    evidence_text: str = "fixture evidence"
    product_family: str = ""


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
    rows = [
        Row("673001", "TECEdrainprofile", "drain_body", 800, "body800.txt", "body 800 evidence"),
        Row("673002", "TECEdrainprofile", "drain_body", "", "body_missing.txt", "body missing evidence"),
        Row("670800", "TECEdrainprofile", "profile_cover", 800, "cover800.txt", "cover 800 evidence"),
        Row("670900", "TECEdrainprofile", "profile_cover", 900, "cover900.txt", "cover 900 evidence"),
        Row("670000", "TECEdrainprofile", "profile_cover", "", "cover_missing.txt", "cover missing evidence"),
        Row("679999", "TECEdrainprofile", "accessory", 800),
        Row("679998", "TECEdrainprofile", "complete_set", 800),
        Row("679997", "TECEdrainprofile", "unknown", 800),
        Row("650000", "TECEdrainline", "drain_body", 800),
        Row("3601050", "TECEdrainpoint S", "drain_body", 800),
        Row("680000", "TECEdrainway", "drain_body", 800),
    ]
    monkeypatch.setattr(audit, "load_source_pack", lambda _path: Report(rows))
    return rows


def build(tmp_path: Path, source_pack: Path) -> tuple[list[dict], dict]:
    return audit.build_audit(source_pack, out=tmp_path / "audit.csv")


def test_valid_tecedrainprofile_audit_produces_diagnostic_only_rows(tmp_path: Path, source_pack: Path, fake_inventory: list[Row]) -> None:
    rows, report = build(tmp_path, source_pack)
    assert report["valid"] is True
    assert rows
    assert {row["diagnostic_only"] for row in rows} == {"true"}
    assert {row["production_safe"] for row in rows} == {"false"}
    assert {row["production_promotion_blocked"] for row in rows} == {"true"}
    assert {row["ready_for_benchmark"] for row in rows} == {"false"}
    assert {row["ready_for_customer_view"] for row in rows} == {"false"}
    assert rows[0]["audit_pair_id"] == "TECE-DP-AUDIT-000001"


def test_only_tecedrainprofile_rows_are_included(tmp_path: Path, source_pack: Path, fake_inventory: list[Row]) -> None:
    rows, report = build(tmp_path, source_pack)
    assert {row["family"] for row in rows} == {"TECEdrainprofile"}
    articles = {row["drain_body_article"] for row in rows} | {row["profile_cover_article"] for row in rows}
    assert "650000" not in articles
    assert "3601050" not in articles
    assert "680000" not in articles
    assert report["family_inventory_row_count"] == 8


def test_pair_type_and_roles_are_family_specific(tmp_path: Path, source_pack: Path, fake_inventory: list[Row]) -> None:
    rows, _report = build(tmp_path, source_pack)
    assert {row["pair_type"] for row in rows} == {"drain_body_to_profile_cover"}
    assert "drain_body_to_cover_or_grate" not in {row["pair_type"] for row in rows}
    assert {row["drain_body_role"] for row in rows} == {"drain_body"}
    assert {row["profile_cover_role"] for row in rows} == {"profile_cover"}


@pytest.mark.parametrize("article", ["679999", "679998", "679997"])
def test_accessories_complete_sets_and_unknowns_are_excluded(article: str, tmp_path: Path, source_pack: Path, fake_inventory: list[Row]) -> None:
    rows, report = build(tmp_path, source_pack)
    articles = {row["drain_body_article"] for row in rows} | {row["profile_cover_article"] for row in rows}
    assert article not in articles
    assert report["accessory_count"] == 1
    assert report["complete_set_count"] == 1
    assert report["unknown_count"] == 1


def test_exact_length_matches_and_missing_lengths_are_statused(tmp_path: Path, source_pack: Path, fake_inventory: list[Row]) -> None:
    rows, report = build(tmp_path, source_pack)
    exact = [row for row in rows if row["pairing_status"] == "diagnostic_exact_length_match"]
    unresolved = [row for row in rows if row["pairing_status"] == "unresolved_length_missing"]
    assert exact and all(row["nominal_length_mm"] == "800" for row in exact)
    assert unresolved
    assert report["candidate_pair_counts_by_status"]["diagnostic_exact_length_match"] == len(exact)
    assert report["candidate_pair_counts_by_status"]["unresolved_length_missing"] == len(unresolved)


def test_duplicate_pair_keys_fail() -> None:
    rows = [{"family": "TECEdrainprofile", "pair_type": "drain_body_to_profile_cover", "drain_body_article": "673001", "profile_cover_article": "670800", "nominal_length_mm": "800", "drain_body_role": "drain_body", "profile_cover_role": "profile_cover", "production_safe": "false", "ready_for_benchmark": "false", "ready_for_customer_view": "false"}] * 2
    gates = audit.validate_audit_rows(rows)
    assert gates["duplicate_pair_keys"]


@pytest.mark.parametrize("field", ["production_safe", "ready_for_benchmark", "ready_for_customer_view"])
def test_leakage_gates_fail(field: str) -> None:
    row = {"family": "TECEdrainprofile", "pair_type": "drain_body_to_profile_cover", "drain_body_article": "673001", "profile_cover_article": "670800", "nominal_length_mm": "800", "drain_body_role": "drain_body", "profile_cover_role": "profile_cover", "production_safe": "false", "ready_for_benchmark": "false", "ready_for_customer_view": "false"}
    row[field] = "true"
    gates = audit.validate_audit_rows([row])
    assert gates["production_leakage_rows"] or gates["readiness_leakage_rows"]


def test_diagnostic_only_note_is_present(tmp_path: Path, source_pack: Path, fake_inventory: list[Row]) -> None:
    _rows, report = build(tmp_path, source_pack)
    assert report["diagnostic_only_note"] == audit.DIAGNOSTIC_ONLY_NOTE
    assert "Production promotion remains blocked" in report["diagnostic_only_note"]


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
    after = {str(p): hashlib.sha256(p.read_bytes()).hexdigest() for p in overlay_files}
    assert after == before


def test_cli_writes_csv_and_json(tmp_path: Path, source_pack: Path, fake_inventory: list[Row]) -> None:
    csv_path = tmp_path / "out.csv"
    json_path = tmp_path / "out.json"
    rc = audit.main(["--source-pack", str(source_pack), "--out", str(csv_path), "--json-out", str(json_path), "--json"])
    assert rc == 0
    data = json.loads(json_path.read_text(encoding="utf-8"))
    assert data["valid"] is True
    assert csv_path.read_text(encoding="utf-8-sig").startswith("audit_pair_id,")


def test_aco_canonical_baseline_remains_stable() -> None:
    pytest.importorskip("openpyxl")
    out = Path("/tmp/benchmark_aco_canonical_drainprofile_audit_test.xlsx")
    export = subprocess.run([sys.executable, "tools/export_canonical_aco_benchmark_xlsx.py", "--out", str(out)], text=True, capture_output=True)
    if export.returncode != 0:
        pytest.skip("ACO canonical export baseline is unavailable in this checkout: " + (export.stderr or export.stdout)[:500])
    validation = subprocess.run([sys.executable, "tools/validate_xlsx_export.py", str(out)], check=True, text=True, capture_output=True)
    result = subprocess.run([sys.executable, "tools/report_aco_final_baseline.py", "--xlsx", str(out)], check=True, text=True, capture_output=True)
    assert "OVERALL: PASS" in validation.stdout
    assert "OVERALL: ACO_BASELINE_STABLE" in result.stdout
