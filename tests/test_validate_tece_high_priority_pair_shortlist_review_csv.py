from __future__ import annotations

import csv
import hashlib
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from tools import report_aco_final_baseline as aco_mod
from tools.export_tece_high_priority_pair_shortlist import CSV_COLUMNS
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
        "reviewer_pair_decision": "",
        "reviewer_notes": "",
        "safe_to_apply_automatically": "",
    }
    row.update(kw)
    return row


def _write_csv(path: Path, rows: list[dict[str, object]], fields: list[str] | None = None) -> None:
    fieldnames = fields or CSV_COLUMNS
    with path.open("w", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def test_valid_blank_review_csv_passes(tmp_path: Path) -> None:
    path = tmp_path / "review.csv"
    _write_csv(path, [_row(), _row(shortlist_id="TECE-HP-0002", drain_body_article="600002", candidate_article="700002")])
    report = validate_shortlist_review_csv(path)
    assert report["valid"] is True
    assert report["empty_decision_count"] == 2
    assert report["safe_to_apply_true_count"] == 0
    assert report["production_safe_candidate_count"] == 0
    assert report["production_promotion_blocked"] is True


def test_valid_compatible_safe_rows_pass_when_notes_are_filled(tmp_path: Path) -> None:
    path = tmp_path / "review.csv"
    _write_csv(path, [_row(reviewer_pair_decision="compatible", safe_to_apply_automatically="true", reviewer_notes="Reviewed matching catalog evidence.")])
    report = validate_shortlist_review_csv(path)
    assert report["valid"] is True
    assert report["safe_to_apply_true_count"] == 1
    assert len(report["compatible_safe_rows_sample"]) == 1


def test_missing_required_columns_fail(tmp_path: Path) -> None:
    path = tmp_path / "review.csv"
    _write_csv(path, [_row()], fields=[c for c in CSV_COLUMNS if c != "reviewer_notes"])
    report = validate_shortlist_review_csv(path)
    assert report["valid"] is False
    assert any("missing_required_columns" in error for error in report["errors"])


def test_invalid_reviewer_decision_fails(tmp_path: Path) -> None:
    path = tmp_path / "review.csv"
    _write_csv(path, [_row(reviewer_pair_decision="maybe")])
    assert validate_shortlist_review_csv(path)["valid"] is False


def test_invalid_safe_to_apply_value_fails(tmp_path: Path) -> None:
    path = tmp_path / "review.csv"
    _write_csv(path, [_row(safe_to_apply_automatically="yes")])
    assert validate_shortlist_review_csv(path)["valid"] is False


def test_duplicate_shortlist_id_fails(tmp_path: Path) -> None:
    path = tmp_path / "review.csv"
    _write_csv(path, [_row(), _row(drain_body_article="600002", candidate_article="700002")])
    report = validate_shortlist_review_csv(path)
    assert report["valid"] is False
    assert report["duplicate_shortlist_ids"] == ["TECE-HP-0001"]


def test_duplicate_pair_key_fails(tmp_path: Path) -> None:
    path = tmp_path / "review.csv"
    _write_csv(path, [_row(), _row(shortlist_id="TECE-HP-0002")])
    report = validate_shortlist_review_csv(path)
    assert report["valid"] is False
    assert report["duplicate_pair_keys"] == [{"drain_body_article": "600001", "candidate_article": "700001", "nominal_length_mm": "700"}]


def test_blocked_article_fails(tmp_path: Path) -> None:
    path = tmp_path / "review.csv"
    _write_csv(path, [_row(candidate_article="650700")])
    assert validate_shortlist_review_csv(path)["valid"] is False


def test_production_readiness_leakage_fails(tmp_path: Path) -> None:
    path = tmp_path / "review.csv"
    _write_csv(path, [_row(production_safe="true", production_promotion_blocked="false", ready_for_benchmark="true", ready_for_customer_view="true")])
    report = validate_shortlist_review_csv(path)
    assert report["valid"] is False
    assert any("production_safe" in error for error in report["errors"])
    assert any("ready_for_benchmark" in error for error in report["errors"])


def test_safe_to_apply_true_without_compatible_decision_fails(tmp_path: Path) -> None:
    path = tmp_path / "review.csv"
    _write_csv(path, [_row(reviewer_pair_decision="ambiguous", safe_to_apply_automatically="true", reviewer_notes="notes")])
    report = validate_shortlist_review_csv(path)
    assert report["valid"] is False
    assert len(report["ambiguous_rows_sample"]) == 1


def test_safe_to_apply_true_without_notes_fails(tmp_path: Path) -> None:
    path = tmp_path / "review.csv"
    _write_csv(path, [_row(reviewer_pair_decision="compatible", safe_to_apply_automatically="true", reviewer_notes="")])
    assert validate_shortlist_review_csv(path)["valid"] is False


def test_source_pack_immutability(tmp_path: Path) -> None:
    source_pack = Path("tests/fixtures/tece/source_pack")
    before = {p: hashlib.sha256(p.read_bytes()).hexdigest() for p in source_pack.iterdir() if p.is_file()}
    path = tmp_path / "review.csv"
    _write_csv(path, [_row()])
    validate_shortlist_review_csv(path)
    after = {p: hashlib.sha256(p.read_bytes()).hexdigest() for p in source_pack.iterdir() if p.is_file()}
    assert after == before


def test_aco_canonical_baseline_remains_pass_and_stable() -> None:
    report = aco_mod.AuditReport((aco_mod.AuditCheck("baseline_guard", True, "unchanged"),), {}, {}, {})
    text = aco_mod.format_report(Path("canonical.xlsx"), report)
    assert "- PASS baseline_guard" in text
    assert "OVERALL: ACO_BASELINE_STABLE" in text
