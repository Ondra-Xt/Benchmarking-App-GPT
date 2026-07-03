from __future__ import annotations

import csv
import hashlib
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from tools import report_aco_final_baseline as aco_mod
from tools.export_tece_high_priority_pair_shortlist import CSV_COLUMNS
from tools.report_tece_shortlist_review_impact import report_shortlist_review_impact
from tools.validate_tece_high_priority_pair_shortlist_review_csv import validate_shortlist_review_csv


def _row(**kw: object) -> dict[str, object]:
    row: dict[str, object] = {
        "shortlist_id": "TECE-HP-0001",
        "family": "TECEdrainline",
        "pair_type": "drain_body_to_cover_or_grate",
        "nominal_length_mm": "700",
        "drain_body_article": "600001",
        "candidate_article": "700001",
        "candidate_role": "cover_or_grate",
        "drain_body_source_file": "body.pdf",
        "candidate_source_file": "candidate.pdf",
        "drain_body_evidence_text_snippet": "body evidence",
        "candidate_evidence_text_snippet": "candidate evidence",
        "diagnostic_review_priority": "high_review_priority",
        "status": "diagnostic_exact_length_match",
        "diagnostic_only": "true",
        "production_safe": "false",
        "production_promotion_blocked": "true",
        "ready_for_benchmark": "false",
        "ready_for_customer_view": "false",
        "reviewer_pair_decision": "compatible",
        "reviewer_notes": "Reviewed matching catalog evidence.",
        "safe_to_apply_automatically": "true",
    }
    row.update(kw)
    return row


def _write_csv(path: Path, rows: list[dict[str, object]]) -> None:
    with path.open("w", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=CSV_COLUMNS, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def _write_validation(path: Path, review_csv: Path, *, valid: bool = True) -> None:
    report = validate_shortlist_review_csv(review_csv) if valid else {"valid": False, "errors": ["fixture invalid"]}
    path.write_text(json.dumps(report), encoding="utf-8")


def test_valid_strict_review_impact_fixture(tmp_path: Path) -> None:
    rows: list[dict[str, object]] = []
    for i in range(34):
        rows.append(_row(shortlist_id=f"TECE-HP-C-{i:04d}", drain_body_article=f"60{i:04d}", candidate_article=f"70{i:04d}", nominal_length_mm=str(700 + i % 4 * 100)))
    for i in range(22):
        rows.append(_row(shortlist_id=f"TECE-HP-A-{i:04d}", drain_body_article=f"61{i:04d}", candidate_article=f"71{i:04d}", reviewer_pair_decision="ambiguous", safe_to_apply_automatically="false", reviewer_notes="Color variant evidence not explicit.", candidate_evidence_text_snippet="black special variant", nominal_length_mm=str(700 + i % 3 * 100)))
    for i in range(24):
        rows.append(_row(shortlist_id=f"TECE-HP-I-{i:04d}", drain_body_article=f"62{i:04d}", candidate_article=f"72{i:04d}", reviewer_pair_decision="incompatible", safe_to_apply_automatically="false", reviewer_notes="Reviewed mismatch.", nominal_length_mm=str(800 + i % 2 * 100)))
    review_csv = tmp_path / "review.csv"
    validation_json = tmp_path / "validation.json"
    _write_csv(review_csv, rows)
    _write_validation(validation_json, review_csv)

    report = report_shortlist_review_impact(review_csv, validation_json)

    assert report["valid"] is True
    assert report["total_review_rows"] == 80
    assert report["compatible_safe_pair_count"] == 34
    assert report["ambiguous_count"] == 22
    assert report["incompatible_count"] == 24
    assert report["safe_to_apply_true_count"] == 34
    assert report["production_safe_candidate_count"] == 0
    assert report["production_promotion_blocked"] is True
    assert report["ready_for_benchmark"] is False
    assert report["ready_for_customer_view"] is False


def test_invalid_validation_report_fails(tmp_path: Path) -> None:
    review_csv = tmp_path / "review.csv"
    validation_json = tmp_path / "validation.json"
    _write_csv(review_csv, [_row()])
    _write_validation(validation_json, review_csv, valid=False)
    report = report_shortlist_review_impact(review_csv, validation_json)
    assert report["valid"] is False
    assert any("validation report is invalid" in error for error in report["errors"])


def test_safe_compatible_rows_counted_correctly(tmp_path: Path) -> None:
    review_csv = tmp_path / "review.csv"
    validation_json = tmp_path / "validation.json"
    _write_csv(review_csv, [_row(), _row(shortlist_id="TECE-HP-0002", drain_body_article="600002", candidate_article="700002", reviewer_pair_decision="compatible", safe_to_apply_automatically="false")])
    _write_validation(validation_json, review_csv)
    report = report_shortlist_review_impact(review_csv, validation_json)
    assert report["valid"] is True
    assert report["safe_to_apply_true_count"] == 1
    assert report["compatible_safe_pair_count"] == 1
    assert report["not_safe_count"] == 1


def test_ambiguous_incompatible_safe_true_fails(tmp_path: Path) -> None:
    review_csv = tmp_path / "review.csv"
    validation_json = tmp_path / "validation.json"
    _write_csv(review_csv, [_row(reviewer_pair_decision="ambiguous", safe_to_apply_automatically="true", candidate_evidence_text_snippet="black variant")])
    validation_json.write_text(json.dumps({"valid": True}), encoding="utf-8")
    report = report_shortlist_review_impact(review_csv, validation_json)
    assert report["valid"] is False
    assert any("only allowed for compatible" in error for error in report["errors"])
    assert any("color/special variant" in error for error in report["errors"])


def test_production_readiness_leakage_fails(tmp_path: Path) -> None:
    review_csv = tmp_path / "review.csv"
    validation_json = tmp_path / "validation.json"
    _write_csv(review_csv, [_row(production_safe="true", ready_for_benchmark="true", ready_for_customer_view="true")])
    validation_json.write_text(json.dumps({"valid": True}), encoding="utf-8")
    report = report_shortlist_review_impact(review_csv, validation_json)
    assert report["valid"] is False
    assert any("production_safe=true" in error for error in report["errors"])
    assert any("ready_for_benchmark=true" in error for error in report["errors"])
    assert any("ready_for_customer_view=true" in error for error in report["errors"])


def test_blocked_articles_fail(tmp_path: Path) -> None:
    review_csv = tmp_path / "review.csv"
    validation_json = tmp_path / "validation.json"
    _write_csv(review_csv, [_row(candidate_article="650700")])
    validation_json.write_text(json.dumps({"valid": True}), encoding="utf-8")
    report = report_shortlist_review_impact(review_csv, validation_json)
    assert report["valid"] is False
    assert any("blocked article appears" in error for error in report["errors"])


def test_source_pack_immutability(tmp_path: Path) -> None:
    source_pack = Path("tests/fixtures/tece/source_pack")
    before = {p: hashlib.sha256(p.read_bytes()).hexdigest() for p in source_pack.iterdir() if p.is_file()}
    review_csv = tmp_path / "review.csv"
    validation_json = tmp_path / "validation.json"
    _write_csv(review_csv, [_row()])
    _write_validation(validation_json, review_csv)
    report_shortlist_review_impact(review_csv, validation_json)
    after = {p: hashlib.sha256(p.read_bytes()).hexdigest() for p in source_pack.iterdir() if p.is_file()}
    assert after == before


def test_aco_canonical_baseline_remains_pass_and_stable() -> None:
    report = aco_mod.AuditReport((aco_mod.AuditCheck("baseline_guard", True, "unchanged"),), {}, {}, {})
    text = aco_mod.format_report(Path("canonical.xlsx"), report)
    assert "- PASS baseline_guard" in text
    assert "OVERALL: ACO_BASELINE_STABLE" in text
