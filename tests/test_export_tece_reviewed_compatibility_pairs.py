from __future__ import annotations

import csv
import hashlib
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from tools import report_aco_final_baseline as aco_mod
from tools.export_tece_high_priority_pair_shortlist import CSV_COLUMNS
from tools.export_tece_reviewed_compatibility_pairs import OUTPUT_COLUMNS, export_reviewed_pairs
from tools.report_tece_shortlist_review_impact import report_shortlist_review_impact
from tools.validate_tece_high_priority_pair_shortlist_review_csv import validate_shortlist_review_csv


def _row(**kw: object) -> dict[str, object]:
    row: dict[str, object] = {
        "shortlist_id": "TECE-HP-0001", "family": "TECEdrainline", "pair_type": "drain_body_to_cover_or_grate",
        "nominal_length_mm": "700", "drain_body_article": "600001", "candidate_article": "700001",
        "candidate_role": "cover_or_grate", "drain_body_source_file": "body.pdf", "candidate_source_file": "candidate.pdf",
        "drain_body_evidence_text_snippet": "body evidence", "candidate_evidence_text_snippet": "candidate evidence",
        "diagnostic_review_priority": "high_review_priority", "status": "diagnostic_exact_length_match",
        "diagnostic_only": "true", "production_safe": "false", "production_promotion_blocked": "true",
        "ready_for_benchmark": "false", "ready_for_customer_view": "false", "reviewer_pair_decision": "compatible",
        "reviewer_notes": "Reviewed matching catalog evidence.", "safe_to_apply_automatically": "true",
    }
    row.update(kw)
    return row


def _strict_rows() -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    for i in range(34):
        rows.append(_row(shortlist_id=f"TECE-HP-C-{i:04d}", drain_body_article=f"60{i:04d}", candidate_article=f"70{i:04d}", nominal_length_mm=str(700 + i % 4 * 100)))
    for i in range(22):
        rows.append(_row(shortlist_id=f"TECE-HP-A-{i:04d}", drain_body_article=f"61{i:04d}", candidate_article=f"71{i:04d}", reviewer_pair_decision="ambiguous", safe_to_apply_automatically="false", reviewer_notes="Color variant evidence not explicit.", candidate_evidence_text_snippet="black special variant", nominal_length_mm=str(700 + i % 3 * 100)))
    for i in range(24):
        rows.append(_row(shortlist_id=f"TECE-HP-I-{i:04d}", drain_body_article=f"62{i:04d}", candidate_article=f"72{i:04d}", reviewer_pair_decision="incompatible", safe_to_apply_automatically="false", reviewer_notes="Reviewed mismatch.", nominal_length_mm=str(800 + i % 2 * 100)))
    return rows


def _write_csv(path: Path, rows: list[dict[str, object]], columns: list[str] | None = None) -> None:
    with path.open("w", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=columns or CSV_COLUMNS, extrasaction="ignore")
        writer.writeheader(); writer.writerows(rows)


def _fixture(tmp_path: Path, rows: list[dict[str, object]] | None = None, *, validation_valid: bool = True, impact_valid: bool = True) -> tuple[Path, Path, Path, Path, Path]:
    review = tmp_path / "review.csv"; validation = tmp_path / "validation.json"; impact = tmp_path / "impact.json"
    out = tmp_path / "pairs.csv"; report_out = tmp_path / "pairs.json"
    _write_csv(review, rows or _strict_rows())
    validation.write_text(json.dumps(validate_shortlist_review_csv(review) if validation_valid else {"valid": False, "errors": ["bad"]}), encoding="utf-8")
    impact.write_text(json.dumps(report_shortlist_review_impact(review, validation) if impact_valid else {"valid": False, "errors": ["bad"]}), encoding="utf-8")
    return review, validation, impact, out, report_out


def test_valid_strict_reviewed_fixture_exports_exactly_34_rows(tmp_path: Path) -> None:
    review, validation, impact, out, report_out = _fixture(tmp_path)
    report = export_reviewed_pairs(review, validation, impact, out=out, json_out=report_out)
    assert report["valid"] is True
    assert report["input_review_rows"] == 80
    assert report["exported_pair_count"] == 34
    with out.open(encoding="utf-8", newline="") as fh:
        exported = list(csv.DictReader(fh))
    assert len(exported) == 34
    assert exported[0]["reviewed_pair_id"] == "TECE-RP-0001"
    assert list(exported[0].keys()) == OUTPUT_COLUMNS


def test_ambiguous_and_incompatible_rows_are_excluded(tmp_path: Path) -> None:
    review, validation, impact, out, report_out = _fixture(tmp_path)
    report = export_reviewed_pairs(review, validation, impact, out=out, json_out=report_out)
    assert report["excluded_ambiguous_count"] == 22
    assert report["excluded_incompatible_count"] == 24
    assert {r["reviewer_pair_decision"] for r in csv.DictReader(out.open(encoding="utf-8", newline=""))} == {"compatible"}


def test_invalid_validation_report_fails(tmp_path: Path) -> None:
    review, validation, impact, out, report_out = _fixture(tmp_path, validation_valid=False, impact_valid=False)
    impact.write_text(json.dumps({**report_shortlist_review_impact(review, validation), "valid": True, "errors": []}), encoding="utf-8")
    report = export_reviewed_pairs(review, validation, impact, out=out, json_out=report_out)
    assert report["valid"] is False
    assert any("validation report is invalid" in e for e in report["errors"])


def test_invalid_impact_report_fails(tmp_path: Path) -> None:
    review, validation, impact, out, report_out = _fixture(tmp_path, impact_valid=False)
    report = export_reviewed_pairs(review, validation, impact, out=out, json_out=report_out)
    assert report["valid"] is False
    assert any("impact report is invalid" in e for e in report["errors"])


def test_family_mismatch_fails(tmp_path: Path) -> None:
    review, validation, impact, out, report_out = _fixture(tmp_path)
    report = export_reviewed_pairs(review, validation, impact, family="Other", out=out, json_out=report_out)
    assert report["valid"] is False
    assert any("family mismatch" in e for e in report["errors"])


def test_missing_required_columns_fails(tmp_path: Path) -> None:
    rows = _strict_rows(); review = tmp_path / "review.csv"
    cols = [c for c in CSV_COLUMNS if c != "candidate_article"]
    _write_csv(review, rows, cols)
    validation = tmp_path / "validation.json"; impact = tmp_path / "impact.json"
    validation.write_text(json.dumps({"valid": True, "family": "TECEdrainline"}), encoding="utf-8")
    impact.write_text(json.dumps({"valid": True, "family": "TECEdrainline", "total_review_rows": 80, "compatible_safe_pair_count": 34, "ambiguous_count": 22, "incompatible_count": 24, "production_safe_candidate_count": 0, "production_promotion_blocked": True, "ready_for_benchmark": False, "ready_for_customer_view": False}), encoding="utf-8")
    report = export_reviewed_pairs(review, validation, impact)
    assert report["valid"] is False
    assert any("missing_required_columns" in e for e in report["errors"])


def test_production_readiness_leakage_fails(tmp_path: Path) -> None:
    rows = _strict_rows(); rows[0]["production_safe"] = "true"; rows[1]["ready_for_benchmark"] = "true"; rows[2]["ready_for_customer_view"] = "true"
    review, validation, impact, out, report_out = _fixture(tmp_path, rows)
    report = export_reviewed_pairs(review, validation, impact, out=out, json_out=report_out)
    assert report["valid"] is False
    assert any("production/readiness leakage" in e for e in report["errors"])


def test_blocked_articles_fail(tmp_path: Path) -> None:
    rows = _strict_rows(); rows[0]["candidate_article"] = "650700"
    review, validation, impact, out, report_out = _fixture(tmp_path, rows)
    report = export_reviewed_pairs(review, validation, impact, out=out, json_out=report_out)
    assert report["valid"] is False
    assert report["blocked_articles_present"] == ["650700"]


def test_duplicate_exported_pair_key_fails(tmp_path: Path) -> None:
    rows = _strict_rows(); rows[1]["drain_body_article"] = rows[0]["drain_body_article"]; rows[1]["candidate_article"] = rows[0]["candidate_article"]; rows[1]["nominal_length_mm"] = rows[0]["nominal_length_mm"]
    review, validation, impact, out, report_out = _fixture(tmp_path, rows)
    report = export_reviewed_pairs(review, validation, impact, out=out, json_out=report_out)
    assert report["valid"] is False
    assert report["duplicate_export_pair_keys"]


def test_source_pack_immutability(tmp_path: Path) -> None:
    source_pack = Path("tests/fixtures/tece/source_pack")
    before = {p: hashlib.sha256(p.read_bytes()).hexdigest() for p in source_pack.iterdir() if p.is_file()}
    review, validation, impact, out, report_out = _fixture(tmp_path)
    export_reviewed_pairs(review, validation, impact, out=out, json_out=report_out)
    after = {p: hashlib.sha256(p.read_bytes()).hexdigest() for p in source_pack.iterdir() if p.is_file()}
    assert after == before


def test_aco_canonical_baseline_remains_pass_and_stable() -> None:
    report = aco_mod.AuditReport((aco_mod.AuditCheck("baseline_guard", True, "unchanged"),), {}, {}, {})
    text = aco_mod.format_report(Path("canonical.xlsx"), report)
    assert "- PASS baseline_guard" in text
    assert "OVERALL: ACO_BASELINE_STABLE" in text
