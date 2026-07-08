from __future__ import annotations

import csv
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from tools import report_aco_final_baseline as aco_mod
from tools.validate_tece_drainprofile_drain_body_length_role_review_csv import (
    DIAGNOSTIC_ONLY_NOTE,
    EXPECTED_ARTICLES,
    EXPECTED_REJECT_REASON_TAG_MAPPING,
    validate_tece_drainprofile_drain_body_length_role_review_csv,
)

FIELDS = [
    "review_id",
    "family",
    "article_number",
    "original_article_role",
    "reviewer_decision",
    "reviewed_nominal_length_mm",
    "safe_to_apply_automatically",
    "reviewer_notes",
    "production_safe",
    "production_promotion_blocked",
    "ready_for_benchmark",
    "ready_for_customer_view",
    "diagnostic_only",
]
REJECT_FIELDS = [
    "article_number",
    "reviewer_decision",
    "reviewed_nominal_length_mm",
    "safe_to_apply_automatically",
    "reject_reason_tag",
]


def _decision(article: str) -> str:
    return "drain_body_no_length_found" if article in {"673001", "673002", "673003"} else "not_drain_body"


def _review_rows() -> list[dict[str, str]]:
    return [
        {
            "review_id": f"TECE-DP-{index:04d}",
            "family": "TECEdrainprofile",
            "article_number": article,
            "original_article_role": "drain_body",
            "reviewer_decision": _decision(article),
            "reviewed_nominal_length_mm": "",
            "safe_to_apply_automatically": "false",
            "reviewer_notes": f"Reviewed article {article}; diagnostic-only manual role decision.",
            "production_safe": "false",
            "production_promotion_blocked": "true",
            "ready_for_benchmark": "false",
            "ready_for_customer_view": "false",
            "diagnostic_only": "true",
        }
        for index, article in enumerate(EXPECTED_ARTICLES, start=1)
    ]


def _reject_rows() -> list[dict[str, str]]:
    return [
        {
            "article_number": article,
            "reviewer_decision": _decision(article),
            "reviewed_nominal_length_mm": "",
            "safe_to_apply_automatically": "false",
            "reject_reason_tag": EXPECTED_REJECT_REASON_TAG_MAPPING[article],
        }
        for article in EXPECTED_ARTICLES
    ]


def _write(path: Path, rows: list[dict[str, str]], fields: list[str]) -> None:
    with path.open("w", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def _report(tmp_path: Path, rows: list[dict[str, str]] | None = None, reject_rows: list[dict[str, str]] | None = None):
    review = tmp_path / "review.csv"
    _write(review, rows if rows is not None else _review_rows(), FIELDS)
    reject = None
    if reject_rows is not None:
        reject = tmp_path / "reject.csv"
        _write(reject, reject_rows, REJECT_FIELDS)
    return validate_tece_drainprofile_drain_body_length_role_review_csv(review, reject_reason_csv=reject)


def test_valid_reviewed_csv_passes(tmp_path: Path) -> None:
    report = _report(tmp_path)
    assert report["valid"] is True
    assert report["decision_counts"] == {"drain_body_no_length_found": 3, "not_drain_body": 9}


def test_valid_reviewed_csv_and_reject_reason_csv_passes(tmp_path: Path) -> None:
    report = _report(tmp_path, reject_rows=_reject_rows())
    assert report["valid"] is True
    assert report["reject_reason_crosscheck_status"] == "pass"
    assert report["reject_reason_tag_counts"] == {"accessory": 1, "no_nominal_length": 3, "profile_cover": 1, "spare_part": 4, "water_trap": 3}


def test_exact_12_row_requirement(tmp_path: Path) -> None:
    assert _report(tmp_path, rows=_review_rows()[:-1])["valid"] is False


def test_exact_article_set_requirement(tmp_path: Path) -> None:
    rows = _review_rows(); rows[0]["article_number"] = "999999"
    assert _report(tmp_path, rows=rows)["valid"] is False


def test_duplicate_article_number_fails(tmp_path: Path) -> None:
    rows = _review_rows(); rows[1]["article_number"] = rows[0]["article_number"]
    report = _report(tmp_path, rows=rows)
    assert report["valid"] is False
    assert report["duplicate_article_numbers"] == ["673001"]


def test_invalid_family_fails(tmp_path: Path) -> None:
    rows = _review_rows(); rows[0]["family"] = "TECEdrainline"
    assert _report(tmp_path, rows=rows)["invalid_family_rows"]


def test_invalid_original_article_role_fails(tmp_path: Path) -> None:
    rows = _review_rows(); rows[0]["original_article_role"] = "accessory"
    assert _report(tmp_path, rows=rows)["invalid_role_rows"]


def test_invalid_reviewer_decision_fails(tmp_path: Path) -> None:
    rows = _review_rows(); rows[0]["reviewer_decision"] = "maybe"
    assert _report(tmp_path, rows=rows)["invalid_decision_rows"]


def test_unexpected_decision_count_fails(tmp_path: Path) -> None:
    rows = _review_rows(); rows[3]["reviewer_decision"] = "drain_body_no_length_found"
    assert _report(tmp_path, rows=rows)["valid"] is False


def test_filled_reviewed_nominal_length_mm_fails(tmp_path: Path) -> None:
    rows = _review_rows(); rows[0]["reviewed_nominal_length_mm"] = "900"
    assert _report(tmp_path, rows=rows)["reviewed_nominal_length_filled_count"] == 1


def test_safe_to_apply_automatically_true_fails(tmp_path: Path) -> None:
    rows = _review_rows(); rows[0]["safe_to_apply_automatically"] = "true"
    assert _report(tmp_path, rows=rows)["safe_to_apply_true_count"] == 1


def test_missing_reviewer_notes_fails(tmp_path: Path) -> None:
    rows = _review_rows(); rows[0]["reviewer_notes"] = ""
    assert _report(tmp_path, rows=rows)["reviewer_notes_missing_count"] == 1


def test_production_safe_leakage_fails(tmp_path: Path) -> None:
    rows = _review_rows(); rows[0]["production_safe"] = "true"
    assert _report(tmp_path, rows=rows)["production_leakage_rows"]


def test_production_promotion_blocked_false_fails(tmp_path: Path) -> None:
    rows = _review_rows(); rows[0]["production_promotion_blocked"] = "false"
    assert _report(tmp_path, rows=rows)["production_promotion_blocked"] is False


def test_ready_for_benchmark_leakage_fails(tmp_path: Path) -> None:
    rows = _review_rows(); rows[0]["ready_for_benchmark"] = "true"
    assert _report(tmp_path, rows=rows)["readiness_leakage_rows"]


def test_ready_for_customer_view_leakage_fails(tmp_path: Path) -> None:
    rows = _review_rows(); rows[0]["ready_for_customer_view"] = "true"
    assert _report(tmp_path, rows=rows)["readiness_leakage_rows"]


def test_diagnostic_only_false_fails(tmp_path: Path) -> None:
    rows = _review_rows(); rows[0]["diagnostic_only"] = "false"
    assert _report(tmp_path, rows=rows)["diagnostic_only_leakage_rows"]


def test_reject_reason_csv_missing_article_fails(tmp_path: Path) -> None:
    assert _report(tmp_path, reject_rows=_reject_rows()[:-1])["valid"] is False


def test_reject_reason_csv_duplicate_article_fails(tmp_path: Path) -> None:
    rows = _reject_rows(); rows[1]["article_number"] = rows[0]["article_number"]
    assert _report(tmp_path, reject_rows=rows)["valid"] is False


def test_reject_reason_invalid_tag_fails(tmp_path: Path) -> None:
    rows = _reject_rows(); rows[0]["reject_reason_tag"] = "bad"
    assert _report(tmp_path, reject_rows=rows)["reject_reason_tag_mapping_errors"]


def test_reject_reason_wrong_tag_count_fails(tmp_path: Path) -> None:
    rows = _reject_rows(); rows[0]["reject_reason_tag"] = "spare_part"
    assert _report(tmp_path, reject_rows=rows)["valid"] is False


def test_reject_reason_wrong_article_tag_mapping_fails(tmp_path: Path) -> None:
    rows = _reject_rows(); rows[-1]["reject_reason_tag"] = "accessory"
    assert _report(tmp_path, reject_rows=rows)["reject_reason_tag_mapping_errors"]


def test_diagnostic_only_note_is_present(tmp_path: Path) -> None:
    assert _report(tmp_path)["diagnostic_only_note"] == DIAGNOSTIC_ONLY_NOTE


def test_aco_canonical_baseline_remains_pass_and_stable() -> None:
    report = aco_mod.AuditReport((aco_mod.AuditCheck("baseline_guard", True, "unchanged"),), {}, {}, {})
    text = aco_mod.format_report(Path("canonical.xlsx"), report)
    assert "- PASS baseline_guard" in text
    assert "OVERALL: ACO_BASELINE_STABLE" in text
