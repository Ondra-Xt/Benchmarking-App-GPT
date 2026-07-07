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
from tools.report_tece_reviewed_compatibility_pairs_qa import report_reviewed_compatibility_pairs_qa
from tools.report_tece_shortlist_review_impact import report_shortlist_review_impact
from tools.validate_tece_high_priority_pair_shortlist_review_csv import validate_shortlist_review_csv


def _row(**kw: object) -> dict[str, object]:
    row: dict[str, object] = {
        "shortlist_id": "TECE-HP-0001", "family": "TECEdrainline", "pair_type": "drain_body_to_cover_or_grate",
        "nominal_length_mm": "700", "drain_body_article": "600700", "candidate_article": "600710",
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
    body_counts = {"600700": 8, "600701": 8, "600702": 8, "600703": 8, "600705": 1, "600707": 1}
    cand_counts = {"600710": 4, "600711": 4, "600751": 4, "600770": 4, "600772": 4, "600782": 4, "600783": 4, "600785": 6}
    body_left = body_counts.copy()
    cand_left = cand_counts.copy()
    pairs: list[tuple[str, str]] = []
    for body in body_counts:
        for cand in sorted(cand_counts, key=lambda value: -cand_left[value]):
            if body_left[body] and cand_left[cand]:
                pairs.append((body, cand))
                body_left[body] -= 1
                cand_left[cand] -= 1
            if body_left[body] == 0:
                break
    rows: list[dict[str, object]] = []
    for i, (body, cand) in enumerate(pairs, start=1):
        rows.append(_row(shortlist_id=f"TECE-HP-C-{i:04d}", drain_body_article=body, candidate_article=cand))
    for i in range(22):
        rows.append(_row(shortlist_id=f"TECE-HP-A-{i:04d}", drain_body_article=f"61{i:04d}", candidate_article=f"71{i:04d}", reviewer_pair_decision="ambiguous", safe_to_apply_automatically="false", reviewer_notes="Color variant evidence not explicit."))
    for i in range(24):
        rows.append(_row(shortlist_id=f"TECE-HP-I-{i:04d}", drain_body_article=f"62{i:04d}", candidate_article=f"72{i:04d}", reviewer_pair_decision="incompatible", safe_to_apply_automatically="false", reviewer_notes="Reviewed mismatch."))
    return rows


def _write_csv(path: Path, rows: list[dict[str, object]], columns: list[str]) -> None:
    with path.open("w", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=columns, extrasaction="ignore")
        writer.writeheader(); writer.writerows(rows)


def _fixture(tmp_path: Path, rows: list[dict[str, object]] | None = None) -> tuple[Path, Path, Path, Path, Path, Path]:
    review = tmp_path / "review.csv"; validation = tmp_path / "validation.json"; impact = tmp_path / "impact.json"
    pairs = tmp_path / "pairs.csv"; pairs_report = tmp_path / "pairs_report.json"; qa = tmp_path / "qa.json"
    _write_csv(review, rows or _strict_rows(), CSV_COLUMNS)
    validation.write_text(json.dumps(validate_shortlist_review_csv(review)), encoding="utf-8")
    impact.write_text(json.dumps(report_shortlist_review_impact(review, validation)), encoding="utf-8")
    export_reviewed_pairs(review, validation, impact, out=pairs, json_out=pairs_report)
    return review, validation, impact, pairs, pairs_report, qa


def _qa(paths: tuple[Path, Path, Path, Path, Path, Path]) -> dict[str, object]:
    review, validation, impact, pairs, pairs_report, qa = paths
    return report_reviewed_compatibility_pairs_qa(pairs, pairs_report, review, validation, impact, out=qa)


def _rewrite_pairs(path: Path, mutator) -> None:
    rows = list(csv.DictReader(path.open(encoding="utf-8", newline="")))
    mutator(rows)
    _write_csv(path, rows, OUTPUT_COLUMNS)


def test_valid_reviewed_pairs_qa_fixture_passes(tmp_path: Path) -> None:
    report = _qa(_fixture(tmp_path))
    assert report["valid"] is True
    assert report["reviewed_pairs_row_count"] == 34
    assert report["compatible_safe_source_rows_count"] == 34
    assert report["length_distribution"] == {"700": 34}


def test_exported_csv_row_count_mismatch_fails(tmp_path: Path) -> None:
    paths = _fixture(tmp_path); _rewrite_pairs(paths[3], lambda rows: rows.pop())
    report = _qa(paths)
    assert report["valid"] is False
    assert any("row count" in e for e in report["errors"])


def test_missing_source_review_pair_fails(tmp_path: Path) -> None:
    paths = _fixture(tmp_path); _rewrite_pairs(paths[3], lambda rows: rows[0].update(candidate_article="699999"))
    report = _qa(paths)
    assert report["valid"] is False
    assert report["exported_pairs_missing_from_review"]


def test_unexpected_exported_ambiguous_row_fails(tmp_path: Path) -> None:
    paths = _fixture(tmp_path); _rewrite_pairs(paths[3], lambda rows: rows[0].update(drain_body_article="610000", candidate_article="710000"))
    report = _qa(paths)
    assert report["valid"] is False
    assert report["unexpected_exported_pairs"]


def test_unexpected_exported_incompatible_row_fails(tmp_path: Path) -> None:
    paths = _fixture(tmp_path); _rewrite_pairs(paths[3], lambda rows: rows[0].update(drain_body_article="620000", candidate_article="720000"))
    report = _qa(paths)
    assert report["valid"] is False
    assert report["unexpected_exported_pairs"]


def test_invalid_reviewed_pair_id_sequence_fails(tmp_path: Path) -> None:
    paths = _fixture(tmp_path); _rewrite_pairs(paths[3], lambda rows: rows[3].update(reviewed_pair_id="TECE-RP-9999"))
    report = _qa(paths)
    assert report["valid"] is False
    assert report["invalid_reviewed_pair_ids"]


def test_duplicate_exported_pair_key_fails(tmp_path: Path) -> None:
    paths = _fixture(tmp_path); _rewrite_pairs(paths[3], lambda rows: rows[1].update(drain_body_article=rows[0]["drain_body_article"], candidate_article=rows[0]["candidate_article"], nominal_length_mm=rows[0]["nominal_length_mm"]))
    report = _qa(paths)
    assert report["valid"] is False
    assert report["duplicate_export_pair_keys"]


def test_blocked_article_fails(tmp_path: Path) -> None:
    paths = _fixture(tmp_path); _rewrite_pairs(paths[3], lambda rows: rows[0].update(candidate_article="650700"))
    report = _qa(paths)
    assert report["valid"] is False
    assert report["blocked_articles_present"] == ["650700"]


def test_production_readiness_leakage_fails(tmp_path: Path) -> None:
    paths = _fixture(tmp_path); _rewrite_pairs(paths[3], lambda rows: (rows[0].update(production_safe="true"), rows[1].update(ready_for_benchmark="true")))
    report = _qa(paths)
    assert report["valid"] is False
    assert report["production_leakage_rows"]
    assert report["readiness_leakage_rows"]


def test_invalid_upstream_validation_report_fails(tmp_path: Path) -> None:
    paths = _fixture(tmp_path); paths[1].write_text(json.dumps({"valid": False, "errors": ["bad"]}), encoding="utf-8")
    report = _qa(paths)
    assert report["valid"] is False
    assert any("validation report is invalid" in e for e in report["errors"])


def test_invalid_upstream_impact_report_fails(tmp_path: Path) -> None:
    paths = _fixture(tmp_path); paths[2].write_text(json.dumps({"valid": False, "errors": ["bad"]}), encoding="utf-8")
    report = _qa(paths)
    assert report["valid"] is False
    assert any("impact report is invalid" in e for e in report["errors"])


def test_invalid_reviewed_pairs_report_fails(tmp_path: Path) -> None:
    paths = _fixture(tmp_path); paths[4].write_text(json.dumps({"valid": False, "errors": ["bad"]}), encoding="utf-8")
    report = _qa(paths)
    assert report["valid"] is False
    assert any("reviewed pairs report is invalid" in e for e in report["errors"])


def test_family_mismatch_fails(tmp_path: Path) -> None:
    review, validation, impact, pairs, pairs_report, qa = _fixture(tmp_path)
    report = report_reviewed_compatibility_pairs_qa(pairs, pairs_report, review, validation, impact, family="Other", out=qa)
    assert report["valid"] is False
    assert any("family mismatch" in e for e in report["errors"])


def test_source_pack_immutability(tmp_path: Path) -> None:
    source_pack = Path("tests/fixtures/tece/source_pack")
    before = {p: hashlib.sha256(p.read_bytes()).hexdigest() for p in source_pack.iterdir() if p.is_file()}
    _qa(_fixture(tmp_path))
    after = {p: hashlib.sha256(p.read_bytes()).hexdigest() for p in source_pack.iterdir() if p.is_file()}
    assert after == before


def test_aco_canonical_baseline_remains_pass_and_stable() -> None:
    report = aco_mod.AuditReport((aco_mod.AuditCheck("baseline_guard", True, "unchanged"),), {}, {}, {})
    text = aco_mod.format_report(Path("canonical.xlsx"), report)
    assert "- PASS baseline_guard" in text
    assert "OVERALL: ACO_BASELINE_STABLE" in text
