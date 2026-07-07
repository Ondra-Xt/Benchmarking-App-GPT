from __future__ import annotations

import csv
import hashlib
import json
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path

import pytest

from tools import export_tece_drainprofile_drain_body_length_review_csv as review


@dataclass(frozen=True)
class Row:
    article_number: str
    tece_family_candidate: str
    tece_article_role_candidate: str
    evidence_text: str = ""
    source_file: str = "fixture.txt"
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
    rows: list[Row] = []
    rows.extend(Row(f"673{i:03d}", "TECEdrainprofile", "drain_body", f"TECEdrainprofile Ablauf DN 50 RG 1 LE 1 Best.-Nr. 673{i:03d}", f"body{i}.txt") for i in range(1, 13))
    rows.extend(Row(f"670{i:03d}", "TECEdrainprofile", "profile_cover", f"TECEdrainprofile Profilabdeckung Nennlänge {700+i*10} mm", f"cover{i}.txt") for i in range(1, 31))
    rows += [
        Row("679999", "TECEdrainprofile", "accessory", "Zubehör"),
        Row("679998", "TECEdrainprofile", "complete_set", "Komplettset"),
        Row("679997", "TECEdrainprofile", "unknown", "unknown"),
        Row("679996", "TECEdrainprofile", "cover_or_grate", "grate"),
        Row("650000", "TECEdrainline", "drain_body", "TECEdrainline"),
        Row("3601050", "TECEdrainpoint S", "drain_body", "TECEdrainpoint S"),
        Row("680000", "TECEdrainway", "drain_body", "TECEdrainway"),
        Row("ACO001", "ACO", "drain_body", "ACO"),
    ]
    while len(rows) < 436:
        rows.append(Row(f"9{len(rows):05d}", "Other", "unknown", "other"))
    monkeypatch.setattr(review, "load_source_pack", lambda _path: Report(rows))
    return rows


def build(tmp_path: Path, source_pack: Path):
    return review.build_report(source_pack, out=tmp_path / "review.csv")


def test_valid_export_produces_exactly_12_review_rows(tmp_path: Path, source_pack: Path, fake_inventory: list[Row]) -> None:
    rows, report = build(tmp_path, source_pack)
    assert report["valid"] is True
    assert report["input_inventory_row_count"] == 436
    assert report["drain_body_count"] == 12
    assert report["exported_review_row_count"] == 12
    assert len(rows) == 12


def test_only_tecedrainprofile_rows_are_included(tmp_path: Path, source_pack: Path, fake_inventory: list[Row]) -> None:
    rows, _ = build(tmp_path, source_pack)
    assert {r["family"] for r in rows} == {"TECEdrainprofile"}
    assert {"650000", "3601050", "680000", "ACO001"}.isdisjoint({r["article_number"] for r in rows})


def test_only_drain_body_rows_are_included(tmp_path: Path, source_pack: Path, fake_inventory: list[Row]) -> None:
    rows, _ = build(tmp_path, source_pack)
    assert {r["article_role"] for r in rows} == {"drain_body"}


@pytest.mark.parametrize("article", ["670001", "679999", "679998", "679997", "679996"])
def test_non_drain_body_roles_are_excluded(article: str, tmp_path: Path, source_pack: Path, fake_inventory: list[Row]) -> None:
    rows, _ = build(tmp_path, source_pack)
    assert article not in {r["article_number"] for r in rows}


def test_review_ids_are_deterministic_and_sequential(tmp_path: Path, source_pack: Path, fake_inventory: list[Row]) -> None:
    rows, _ = build(tmp_path, source_pack)
    assert rows[0]["review_id"] == "TECE-DP-DB-LEN-REVIEW-000001"
    assert rows[-1]["review_id"] == "TECE-DP-DB-LEN-REVIEW-000012"


def test_manual_review_fields_are_blank(tmp_path: Path, source_pack: Path, fake_inventory: list[Row]) -> None:
    rows, _ = build(tmp_path, source_pack)
    for row in rows:
        assert row["reviewed_nominal_length_mm"] == row["reviewer_decision"] == row["reviewer_notes"] == row["safe_to_apply_automatically"] == ""


def test_diagnostic_safety_fields_are_correct(tmp_path: Path, source_pack: Path, fake_inventory: list[Row]) -> None:
    rows, report = build(tmp_path, source_pack)
    assert report["production_safe_candidate_count"] == 0
    for row in rows:
        assert row["diagnostic_only"] == "true"
        assert row["production_safe"] == "false"
        assert row["production_promotion_blocked"] == "true"
        assert row["ready_for_benchmark"] == "false"
        assert row["ready_for_customer_view"] == "false"


def test_duplicate_review_ids_fail() -> None:
    row = review._review_row(Row("673001", "TECEdrainprofile", "drain_body"), 1)
    assert review.validate_rows([row, dict(row)])["duplicate_review_ids"]


def test_duplicate_article_numbers_fail() -> None:
    row1 = review._review_row(Row("673001", "TECEdrainprofile", "drain_body"), 1)
    row2 = review._review_row(Row("673001", "TECEdrainprofile", "drain_body"), 2)
    assert review.validate_rows([row1, row2])["duplicate_article_numbers"]


@pytest.mark.parametrize("field,value", [("reviewed_nominal_length_mm", "900"), ("reviewer_decision", "approve"), ("safe_to_apply_automatically", "true")])
def test_prefilled_review_fields_fail(field: str, value: str) -> None:
    row = review._review_row(Row("673001", "TECEdrainprofile", "drain_body"), 1)
    row[field] = value
    assert review.validate_rows([row])["prefilled_review_field_rows"]


@pytest.mark.parametrize("field,bucket", [("production_safe", "production_leakage_rows"), ("ready_for_benchmark", "readiness_leakage_rows"), ("ready_for_customer_view", "readiness_leakage_rows")])
def test_leakage_fails(field: str, bucket: str) -> None:
    row = review._review_row(Row("673001", "TECEdrainprofile", "drain_body"), 1)
    row[field] = "true"
    assert review.validate_rows([row])[bucket]


def test_diagnostic_only_note_is_present(tmp_path: Path, source_pack: Path, fake_inventory: list[Row]) -> None:
    _, report = build(tmp_path, source_pack)
    assert report["diagnostic_only_note"] == review.DIAGNOSTIC_ONLY_NOTE


def _write_diagnostic(tmp_path: Path, articles: list[str]) -> tuple[Path, Path]:
    csv_path = tmp_path / "diag.csv"
    with csv_path.open("w", encoding="utf-8-sig", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=["family", "article_number", "article_role", "extraction_status", "candidate_length_values_mm", "selected_diagnostic_length_mm"])
        writer.writeheader()
        for article in articles:
            writer.writerow({"family": "TECEdrainprofile", "article_number": article, "article_role": "drain_body", "extraction_status": "no_length_candidate_found"})
    report_path = tmp_path / "diag.json"
    report_path.write_text(json.dumps({"family": "TECEdrainprofile", "extraction_status_counts": {"no_length_candidate_found": 12}}), encoding="utf-8")
    return report_path, csv_path


def test_diagnostic_crosscheck_passes(tmp_path: Path, source_pack: Path, fake_inventory: list[Row]) -> None:
    report_path, csv_path = _write_diagnostic(tmp_path, [f"673{i:03d}" for i in range(1, 13)])
    _, report = review.build_report(source_pack, length_diagnostic_report=report_path, length_diagnostic_csv=csv_path, out=tmp_path / "out.csv")
    assert report["valid"] is True
    assert report["diagnostic_crosscheck_enabled"] is True
    assert report["diagnostic_crosscheck_status"] == "pass"
    assert report["diagnostic_crosscheck_errors"] == []


def test_diagnostic_crosscheck_fails_if_rows_do_not_match(tmp_path: Path, source_pack: Path, fake_inventory: list[Row]) -> None:
    report_path, csv_path = _write_diagnostic(tmp_path, [f"673{i:03d}" for i in range(1, 12)] + ["673999"])
    _, report = review.build_report(source_pack, length_diagnostic_report=report_path, length_diagnostic_csv=csv_path, out=tmp_path / "out.csv")
    assert report["valid"] is False
    assert report["diagnostic_crosscheck_status"] == "fail"


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


def test_aco_canonical_baseline_remains_stable() -> None:
    pytest.importorskip("openpyxl")
    out = Path("/tmp/benchmark_aco_canonical_drainprofile_review_test.xlsx")
    export = subprocess.run([sys.executable, "tools/export_canonical_aco_benchmark_xlsx.py", "--out", str(out)], text=True, capture_output=True)
    if export.returncode != 0:
        pytest.skip("ACO canonical export baseline is unavailable in this checkout: " + (export.stderr or export.stdout)[:500])
    validation = subprocess.run([sys.executable, "tools/validate_xlsx_export.py", str(out)], check=True, text=True, capture_output=True)
    result = subprocess.run([sys.executable, "tools/report_aco_final_baseline.py", "--xlsx", str(out)], check=True, text=True, capture_output=True)
    assert "OVERALL: PASS" in validation.stdout
    assert "OVERALL: ACO_BASELINE_STABLE" in result.stdout


def test_cli_writes_csv_and_json(tmp_path: Path, source_pack: Path, fake_inventory: list[Row]) -> None:
    csv_path = tmp_path / "out.csv"; json_path = tmp_path / "out.json"
    rc = review.main(["--source-pack", str(source_pack), "--out", str(csv_path), "--json-out", str(json_path), "--json"])
    assert rc == 0
    assert json.loads(json_path.read_text(encoding="utf-8"))["valid"] is True
    assert csv_path.read_text(encoding="utf-8-sig").startswith("review_id,")
