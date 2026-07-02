from __future__ import annotations

import csv
import hashlib
import json
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from tools import report_aco_final_baseline as aco_mod
from tools import report_tece_compatibility_pair_audit_ranking as mod
from tools import report_tece_overlay_compatibility_diagnostic as diag_mod
from tests.test_report_aco_final_baseline import _canonical_sheets
from tests.test_report_tece_reviewed_role_overlay_qa import _write_fixture


def _hash(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _set_fixture_lengths(csv_path: Path) -> None:
    with csv_path.open(encoding="utf-8", newline="") as fh:
        rows = list(csv.DictReader(fh))
        fieldnames = list(rows[0])
    for col in ["nominal_length_mm", "evidence_text_snippet"]:
        if col not in fieldnames:
            fieldnames.append(col)
    for row in rows:
        row["nominal_length_mm"] = ""
        row["evidence_text_snippet"] = ""
    body_done = cover_done = accessory_done = False
    for row in rows:
        if row.get("product_family") != "TECEdrainline":
            continue
        role = row.get("applied_article_role")
        if role == "drain_body" and not body_done:
            row["nominal_length_mm"] = "900"
            row["evidence_text_snippet"] = "body evidence 900 mm"
            body_done = True
        elif role == "cover_or_grate" and not cover_done:
            row["nominal_length_mm"] = "900"
            row["evidence_text_snippet"] = "cover evidence 900 mm"
            cover_done = True
        elif role == "accessory" and not accessory_done:
            row["nominal_length_mm"] = "900"
            row["evidence_text_snippet"] = "accessory evidence 900 mm"
            accessory_done = True
    with csv_path.open("w", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def _compat_path(tmp_path: Path, csv_path: Path, qa_path: Path, updates: dict[str, object] | None = None) -> Path:
    report = diag_mod.build_overlay_compatibility_diagnostic(csv_path, qa_path)
    if updates:
        report.update(updates)
    path = tmp_path / "compat.json"
    path.write_text(json.dumps(report), encoding="utf-8")
    return path


def test_valid_real_style_pair_audit_fixture(tmp_path: Path) -> None:
    csv_path, qa_path = _write_fixture(tmp_path)
    _set_fixture_lengths(csv_path)
    compat_path = _compat_path(tmp_path, csv_path, qa_path)

    report = mod.build_pair_audit_ranking(csv_path, qa_path, compat_path)

    assert report["valid"] is True
    assert report["blocked_unknown_articles"] == ["650700", "650800", "651500"]
    assert report["recomputed_pair_counts_by_type"] == report["input_candidate_pair_counts_by_type"]
    assert report["recomputed_pair_counts_by_status"] == report["input_candidate_pair_counts_by_status"]
    assert report["production_safe_candidate_count"] == 0
    assert report["production_promotion_blocked"] is True
    assert report["ready_for_benchmark"] is False
    assert report["ready_for_customer_view"] is False


def test_invalid_qa_report_fails(tmp_path: Path) -> None:
    csv_path, qa_path = _write_fixture(tmp_path, report_updates={"applied_count": 132})
    compat_path = tmp_path / "compat.json"
    compat_path.write_text(json.dumps({"valid": True}), encoding="utf-8")

    report = mod.build_pair_audit_ranking(csv_path, qa_path, compat_path)

    assert report["valid"] is False
    assert any("QA report is invalid" in error for error in report["errors"])


def test_invalid_compatibility_report_fails(tmp_path: Path) -> None:
    csv_path, qa_path = _write_fixture(tmp_path)
    compat_path = tmp_path / "compat.json"
    compat_path.write_text(json.dumps({"valid": False, "errors": ["bad"]}), encoding="utf-8")

    report = mod.build_pair_audit_ranking(csv_path, qa_path, compat_path)

    assert report["valid"] is False
    assert any("compatibility report is invalid" in error for error in report["errors"])


def test_recomputed_counts_mismatch_fails(tmp_path: Path) -> None:
    csv_path, qa_path = _write_fixture(tmp_path)
    compat_path = _compat_path(tmp_path, csv_path, qa_path, {"candidate_pair_counts_by_type": {"drain_body_to_cover_or_grate": 1}})

    report = mod.build_pair_audit_ranking(csv_path, qa_path, compat_path)

    assert report["valid"] is False
    assert any("counts by type differ" in error for error in report["errors"])


def test_blocked_unknown_articles_preserved(tmp_path: Path) -> None:
    csv_path, qa_path = _write_fixture(tmp_path)
    compat_path = _compat_path(tmp_path, csv_path, qa_path)
    report = mod.build_pair_audit_ranking(csv_path, qa_path, compat_path)
    assert report["blocked_unknown_articles"] == ["650700", "650800", "651500"]
    assert report["blocked_review_count"] > 0


def test_no_production_safe_pairs(tmp_path: Path) -> None:
    csv_path, qa_path = _write_fixture(tmp_path)
    _set_fixture_lengths(csv_path)
    compat_path = _compat_path(tmp_path, csv_path, qa_path)
    report = mod.build_pair_audit_ranking(csv_path, qa_path, compat_path)
    pairs = report["top_high_priority_pairs_sample"] + report["blocked_review_sample"]
    assert pairs
    assert all(pair["production_safe"] is False for pair in pairs)
    assert all(pair["ready_for_benchmark"] is False for pair in pairs)
    assert all(pair["ready_for_customer_view"] is False for pair in pairs)


def test_exact_length_cover_pairs_high_and_accessory_pairs_medium(tmp_path: Path) -> None:
    csv_path, qa_path = _write_fixture(tmp_path)
    _set_fixture_lengths(csv_path)
    compat_path = _compat_path(tmp_path, csv_path, qa_path)
    report = mod.build_pair_audit_ranking(csv_path, qa_path, compat_path)
    assert report["high_review_priority_count"] > 0
    assert report["medium_review_priority_count"] > 0
    assert report["top_high_priority_pairs_sample"][0]["pair_type"] == "drain_body_to_cover_or_grate"


def test_unresolved_length_missing_bucketed_and_complete_sets_sampled_not_paired(tmp_path: Path) -> None:
    csv_path, qa_path = _write_fixture(tmp_path)
    compat_path = _compat_path(tmp_path, csv_path, qa_path)
    report = mod.build_pair_audit_ranking(csv_path, qa_path, compat_path)
    assert report["unresolved_length_missing_count"] > 0
    assert report["complete_set_rows_sample"]
    assert all(pair["pair_type"] != "complete_set" for pair in report["unresolved_length_missing_sample"])


def test_optional_csv_output_works_with_required_columns(tmp_path: Path) -> None:
    csv_path, qa_path = _write_fixture(tmp_path)
    compat_path = _compat_path(tmp_path, csv_path, qa_path)
    csv_out = tmp_path / "audit.csv"
    out = tmp_path / "audit.json"

    exit_code = mod.main(["--preview-csv", str(csv_path), "--qa-report", str(qa_path), "--compatibility-report", str(compat_path), "--out", str(out), "--csv-out", str(csv_out), "--json"])

    assert exit_code == 0
    with csv_out.open(encoding="utf-8", newline="") as fh:
        reader = csv.DictReader(fh)
        assert reader.fieldnames == mod.CSV_COLUMNS
        assert next(reader)["diagnostic_only"] == "True"


def test_source_pack_immutability(tmp_path: Path) -> None:
    csv_path, qa_path = _write_fixture(tmp_path)
    compat_path = _compat_path(tmp_path, csv_path, qa_path)
    before = {_path: _hash(_path) for _path in (csv_path, qa_path, compat_path)}
    mod.main(["--preview-csv", str(csv_path), "--qa-report", str(qa_path), "--compatibility-report", str(compat_path), "--out", str(tmp_path / "audit.json"), "--json"])
    assert {_path: _hash(_path) for _path in (csv_path, qa_path, compat_path)} == before


def test_aco_canonical_baseline_remains_pass_stable() -> None:
    report = aco_mod.audit_frames(_canonical_sheets())
    assert all(check.passed for check in report.checks)
    assert report.overall == aco_mod.STABLE
    assert "OVERALL: ACO_BASELINE_STABLE" in aco_mod.format_report(Path("canonical.xlsx"), report)
