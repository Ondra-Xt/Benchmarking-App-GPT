from __future__ import annotations

import csv
import hashlib
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from tools import report_aco_final_baseline as aco_mod
from tools import report_tece_overlay_compatibility_diagnostic as mod
from tests.test_report_aco_final_baseline import _canonical_sheets
from tests.test_report_tece_reviewed_role_overlay_qa import _write_fixture


def _hash(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _set_lengths(csv_path: Path) -> None:
    with csv_path.open(encoding="utf-8", newline="") as fh:
        rows = list(csv.DictReader(fh))
        fieldnames = list(rows[0])
    if "nominal_length_mm" not in fieldnames:
        fieldnames.append("nominal_length_mm")
    seen_body = False
    seen_cover = False
    seen_accessory_missing = False
    for row in rows:
        role = row.get("applied_article_role")
        if row.get("product_family") != "TECEdrainline":
            row["nominal_length_mm"] = ""
            continue
        row["nominal_length_mm"] = ""
        if role == "drain_body" and not seen_body:
            row["nominal_length_mm"] = "900"
            seen_body = True
        elif role == "cover_or_grate" and not seen_cover:
            row["nominal_length_mm"] = "900"
            seen_cover = True
        elif role == "accessory" and not seen_accessory_missing:
            row["nominal_length_mm"] = ""
            seen_accessory_missing = True
    with csv_path.open("w", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def test_valid_real_style_overlay_diagnostic_fixture(tmp_path: Path) -> None:
    csv_path, qa_path = _write_fixture(tmp_path)
    _set_lengths(csv_path)

    report = mod.build_overlay_compatibility_diagnostic(csv_path, qa_path)

    assert report["valid"] is True
    assert report["total_preview_rows"] == 436
    assert report["overlay_role_counts"] == {
        "accessory": 40,
        "complete_set": 4,
        "cover_or_grate": 123,
        "drain_body": 56,
        "unknown": 3,
    }
    assert report["blocked_unknown_articles"] == ["650700", "650800", "651500"]
    assert report["production_safe_candidate_count"] == 0
    assert report["production_promotion_blocked"] is True
    assert report["ready_for_benchmark"] is False
    assert report["ready_for_customer_view"] is False


def test_invalid_qa_report_makes_diagnostic_invalid(tmp_path: Path) -> None:
    csv_path, qa_path = _write_fixture(tmp_path, report_updates={"applied_count": 132})

    report = mod.build_overlay_compatibility_diagnostic(csv_path, qa_path)

    assert report["valid"] is False
    assert any("QA report is invalid" in error for error in report["errors"])


def test_blocked_unknown_rows_remain_unresolved(tmp_path: Path) -> None:
    csv_path, qa_path = _write_fixture(tmp_path)

    report = mod.build_overlay_compatibility_diagnostic(csv_path, qa_path)

    assert report["blocked_unknown_articles"] == ["650700", "650800", "651500"]
    assert report["unresolved_unknown_count"] == 3
    assert report["candidate_pair_counts_by_type"]["drain_body_to_unknown"] > 0
    assert all(pair["status"].startswith("unresolved") for pair in report["unresolved_candidates_sample"])


def test_no_production_safe_candidates_are_emitted(tmp_path: Path) -> None:
    csv_path, qa_path = _write_fixture(tmp_path)
    _set_lengths(csv_path)

    report = mod.build_overlay_compatibility_diagnostic(csv_path, qa_path)

    assert report["production_safe_candidate_count"] == 0
    assert all(pair["production_safe"] is False for pair in report["candidate_pairs_sample"])


def test_no_source_pack_mutation(tmp_path: Path) -> None:
    csv_path, qa_path = _write_fixture(tmp_path)
    before_csv = _hash(csv_path)
    before_qa = _hash(qa_path)

    exit_code = mod.main(["--preview-csv", str(csv_path), "--qa-report", str(qa_path), "--out", str(tmp_path / "diagnostic.json")])

    assert exit_code == 0
    assert _hash(csv_path) == before_csv
    assert _hash(qa_path) == before_qa


def test_aco_baseline_remains_pass_and_stable() -> None:
    report = aco_mod.audit_frames(_canonical_sheets())

    assert all(check.passed for check in report.checks)
    assert report.overall == aco_mod.STABLE
    assert "OVERALL: ACO_BASELINE_STABLE" in aco_mod.format_report(Path("canonical.xlsx"), report)


def test_length_missing_candidates_are_marked_unresolved_length_missing(tmp_path: Path) -> None:
    csv_path, qa_path = _write_fixture(tmp_path)

    report = mod.build_overlay_compatibility_diagnostic(csv_path, qa_path)

    assert report["candidate_pair_counts_by_status"]["unresolved_length_missing"] > 0


def test_exact_length_match_candidates_are_diagnostic_only_not_production_safe(tmp_path: Path) -> None:
    csv_path, qa_path = _write_fixture(tmp_path)
    _set_lengths(csv_path)

    report = mod.build_overlay_compatibility_diagnostic(csv_path, qa_path)

    exact = [pair for pair in report["candidate_pairs_sample"] if pair["status"] == "diagnostic_exact_length_match"]
    assert exact
    assert all(pair["diagnostic_only"] is True for pair in exact)
    assert all(pair["production_safe"] is False for pair in exact)
