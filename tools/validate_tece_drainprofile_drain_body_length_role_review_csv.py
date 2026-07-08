from __future__ import annotations

import argparse
import csv
import sys
from collections import Counter
from pathlib import Path
from typing import Any

if __package__ in {None, ""}:
    repo_root = Path(__file__).resolve().parents[1]
    if str(repo_root) not in sys.path:
        sys.path.insert(0, str(repo_root))

from tools.report_tece_unknown_role_contexts import _clean
from tools.tece_report_output import write_json_output, write_text_output

EXPECTED_FAMILY = "TECEdrainprofile"
EXPECTED_REVIEW_ROWS = 12
EXPECTED_ARTICLES = [
    "673001",
    "673002",
    "673003",
    "675004",
    "675005",
    "675006",
    "675008",
    "675009",
    "675016",
    "675017",
    "675018",
    "675025",
]
ALLOWED_DECISIONS = {
    "confirmed_drain_body_length",
    "drain_body_no_length_found",
    "drain_body_length_ambiguous",
    "not_drain_body",
    "product_role_ambiguous",
}
EXPECTED_DECISION_COUNTS = {
    "confirmed_drain_body_length": 0,
    "drain_body_no_length_found": 3,
    "not_drain_body": 9,
    "drain_body_length_ambiguous": 0,
    "product_role_ambiguous": 0,
}
ALLOWED_REJECT_REASON_TAGS = {"no_nominal_length", "spare_part", "water_trap", "accessory", "profile_cover"}
EXPECTED_REJECT_REASON_TAG_COUNTS = {
    "no_nominal_length": 3,
    "spare_part": 4,
    "water_trap": 3,
    "accessory": 1,
    "profile_cover": 1,
}
EXPECTED_REJECT_REASON_TAG_MAPPING = {
    "673001": "no_nominal_length",
    "673002": "no_nominal_length",
    "673003": "no_nominal_length",
    "675004": "spare_part",
    "675005": "spare_part",
    "675006": "water_trap",
    "675008": "water_trap",
    "675009": "accessory",
    "675016": "spare_part",
    "675017": "spare_part",
    "675018": "water_trap",
    "675025": "profile_cover",
}
DIAGNOSTIC_ONLY_NOTE = (
    "This validator is read-only and diagnostic-only. It validates TECEdrainprofile drain body length and role manual "
    "review results and does not mutate source-pack files, TECE logic, ACO export, Products, Comparison, BOM_Options, "
    "Final_Assemblies, or Final_Set_Details. Production promotion remains blocked."
)


def _norm(value: Any) -> str:
    return _clean(value)


def _norm_bool(value: Any) -> str:
    return _clean(value).lower()


def _article(row: dict[str, Any]) -> str:
    for key in ("article_number", "article", "article_no", "article_id"):
        value = _norm(row.get(key))
        if value:
            return value
    return ""


def _nonzero(counter: Counter[str] | dict[str, int]) -> dict[str, int]:
    return {key: value for key, value in sorted(dict(counter).items()) if value}


def _duplicates(values: list[str]) -> list[str]:
    counts = Counter(values)
    return sorted(value for value, count in counts.items() if value and count > 1)


def _load_csv(path: Path) -> list[dict[str, Any]]:
    with path.open(encoding="utf-8-sig", newline="") as fh:
        return list(csv.DictReader(fh))


def validate_tece_drainprofile_drain_body_length_role_review_csv(
    review_csv: str | Path,
    reject_reason_csv: str | Path | None = None,
    family: str = EXPECTED_FAMILY,
) -> dict[str, Any]:
    """Validate the manually reviewed TECEdrainprofile drain-body length/role CSV without applying it."""
    review_path = Path(review_csv)
    reject_path = Path(reject_reason_csv) if reject_reason_csv else None
    rows = _load_csv(review_path)
    errors: list[str] = []
    warnings: list[str] = []

    article_numbers = [_article(row) for row in rows]
    duplicate_article_numbers = _duplicates(article_numbers)
    duplicate_review_ids = _duplicates([_norm(row.get("review_id")) for row in rows])
    decision_counts: Counter[str] = Counter(_norm(row.get("reviewer_decision")) for row in rows if _norm(row.get("reviewer_decision")))

    invalid_family_rows: list[dict[str, Any]] = []
    invalid_role_rows: list[dict[str, Any]] = []
    invalid_decision_rows: list[dict[str, Any]] = []
    invalid_safe_to_apply_rows: list[dict[str, Any]] = []
    invalid_reviewed_length_rows: list[dict[str, Any]] = []
    missing_reviewer_notes_rows: list[dict[str, Any]] = []
    production_leakage_rows: list[dict[str, Any]] = []
    readiness_leakage_rows: list[dict[str, Any]] = []
    diagnostic_only_leakage_rows: list[dict[str, Any]] = []

    for row_number, row in enumerate(rows, start=2):
        article = _article(row)
        sample = {"row_number": row_number, "article_number": article}
        if _norm(row.get("family")) != family or family != EXPECTED_FAMILY:
            invalid_family_rows.append(sample | {"family": _norm(row.get("family"))})
        if _norm(row.get("original_article_role") or row.get("article_role")) != "drain_body":
            invalid_role_rows.append(sample | {"original_article_role": _norm(row.get("original_article_role") or row.get("article_role"))})
        decision = _norm(row.get("reviewer_decision"))
        if decision not in ALLOWED_DECISIONS:
            invalid_decision_rows.append(sample | {"reviewer_decision": decision})
        if _norm(row.get("reviewed_nominal_length_mm")):
            invalid_reviewed_length_rows.append(sample | {"reviewed_nominal_length_mm": _norm(row.get("reviewed_nominal_length_mm"))})
        if _norm_bool(row.get("safe_to_apply_automatically")) == "true":
            invalid_safe_to_apply_rows.append(sample)
        if not _norm(row.get("reviewer_notes")):
            missing_reviewer_notes_rows.append(sample)
        if _norm_bool(row.get("production_safe")) == "true":
            production_leakage_rows.append(sample | {"production_safe": _norm_bool(row.get("production_safe"))})
        if _norm_bool(row.get("production_promotion_blocked")) != "true":
            production_leakage_rows.append(sample | {"production_promotion_blocked": _norm_bool(row.get("production_promotion_blocked"))})
        if _norm_bool(row.get("ready_for_benchmark")) == "true":
            readiness_leakage_rows.append(sample | {"ready_for_benchmark": _norm_bool(row.get("ready_for_benchmark"))})
        if _norm_bool(row.get("ready_for_customer_view")) == "true":
            readiness_leakage_rows.append(sample | {"ready_for_customer_view": _norm_bool(row.get("ready_for_customer_view"))})
        if _norm_bool(row.get("diagnostic_only")) != "true":
            diagnostic_only_leakage_rows.append(sample | {"diagnostic_only": _norm_bool(row.get("diagnostic_only"))})

    if len(rows) != EXPECTED_REVIEW_ROWS:
        errors.append(f"total_review_rows must be {EXPECTED_REVIEW_ROWS}, got {len(rows)}")
    if sorted(article_numbers) != EXPECTED_ARTICLES:
        errors.append("article_numbers do not match the expected TECEdrainprofile pilot article set")
    if duplicate_article_numbers:
        errors.append(f"duplicate article numbers: {', '.join(duplicate_article_numbers)}")
    if invalid_family_rows:
        errors.append("family must be TECEdrainprofile for every row")
    if invalid_role_rows:
        errors.append("original article_role must be drain_body for every row")
    if invalid_decision_rows:
        errors.append("reviewer_decision contains invalid values")
    if {key: decision_counts.get(key, 0) for key in EXPECTED_DECISION_COUNTS} != EXPECTED_DECISION_COUNTS:
        errors.append("reviewer_decision counts do not match the reviewed pilot expectations")
    if invalid_reviewed_length_rows:
        errors.append("reviewed_nominal_length_mm must be empty for every row")
    if invalid_safe_to_apply_rows:
        errors.append("safe_to_apply_automatically must be false for every row")
    if missing_reviewer_notes_rows:
        errors.append("reviewer_notes must be non-empty for every row")
    if production_leakage_rows:
        errors.append("production safety/promotion leakage detected")
    if readiness_leakage_rows:
        errors.append("benchmark/customer readiness leakage detected")
    if diagnostic_only_leakage_rows:
        errors.append("diagnostic_only must be true for every row")

    reject_counts: Counter[str] = Counter()
    reject_mapping_errors: list[dict[str, Any]] = []
    reject_status = "not_run"
    if reject_path:
        reject_status = "pass"
        reject_rows = _load_csv(reject_path)
        reject_articles = [_article(row) for row in reject_rows]
        reject_dupes = _duplicates(reject_articles)
        review_by_article = {article: row for article, row in zip(article_numbers, rows)}
        if sorted(reject_articles) != sorted(article_numbers):
            errors.append("reject reason CSV article numbers do not match review CSV article numbers")
            reject_status = "fail"
        if reject_dupes:
            errors.append(f"reject reason CSV duplicate article numbers: {', '.join(reject_dupes)}")
            reject_status = "fail"
        for row_number, row in enumerate(reject_rows, start=2):
            article = _article(row)
            tag = _norm(row.get("reject_reason_tag"))
            reject_counts[tag] += 1
            expected_tag = EXPECTED_REJECT_REASON_TAG_MAPPING.get(article)
            if tag not in ALLOWED_REJECT_REASON_TAGS:
                reject_mapping_errors.append({"row_number": row_number, "article_number": article, "reject_reason_tag": tag, "reason": "invalid_tag"})
            if expected_tag != tag:
                reject_mapping_errors.append({"row_number": row_number, "article_number": article, "expected": expected_tag, "actual": tag})
            review_row = review_by_article.get(article)
            if review_row and _norm(review_row.get("reviewer_decision")) != _norm(row.get("reviewer_decision")):
                reject_mapping_errors.append({"row_number": row_number, "article_number": article, "reason": "reviewer_decision_mismatch"})
            if _norm_bool(row.get("safe_to_apply_automatically")) == "true":
                reject_mapping_errors.append({"row_number": row_number, "article_number": article, "reason": "safe_to_apply_true"})
            if _norm(row.get("reviewed_nominal_length_mm")):
                reject_mapping_errors.append({"row_number": row_number, "article_number": article, "reason": "reviewed_length_filled"})
        if {key: reject_counts.get(key, 0) for key in EXPECTED_REJECT_REASON_TAG_COUNTS} != EXPECTED_REJECT_REASON_TAG_COUNTS:
            errors.append("reject_reason_tag counts do not match expected values")
            reject_status = "fail"
        if reject_mapping_errors:
            errors.append("reject reason CSV mapping/cross-check errors detected")
            reject_status = "fail"

    report = {
        "valid": False,
        "errors": errors,
        "warnings": warnings,
        "review_csv_path": str(review_path),
        "reject_reason_csv_path": str(reject_path) if reject_path else None,
        "family": family,
        "total_review_rows": len(rows),
        "expected_review_rows": EXPECTED_REVIEW_ROWS,
        "article_numbers": sorted(article_numbers),
        "decision_counts": _nonzero(decision_counts),
        "safe_to_apply_true_count": len(invalid_safe_to_apply_rows),
        "reviewed_nominal_length_filled_count": len(invalid_reviewed_length_rows),
        "reviewer_notes_missing_count": len(missing_reviewer_notes_rows),
        "reject_reason_crosscheck_enabled": reject_path is not None,
        "reject_reason_crosscheck_status": reject_status,
        "reject_reason_tag_counts": _nonzero(reject_counts),
        "reject_reason_tag_mapping_errors": reject_mapping_errors,
        "duplicate_review_ids": duplicate_review_ids,
        "duplicate_article_numbers": duplicate_article_numbers,
        "invalid_family_rows": invalid_family_rows,
        "invalid_role_rows": invalid_role_rows,
        "invalid_decision_rows": invalid_decision_rows,
        "invalid_safe_to_apply_rows": invalid_safe_to_apply_rows,
        "invalid_reviewed_length_rows": invalid_reviewed_length_rows,
        "missing_reviewer_notes_rows": missing_reviewer_notes_rows,
        "production_leakage_rows": production_leakage_rows,
        "readiness_leakage_rows": readiness_leakage_rows,
        "diagnostic_only_leakage_rows": diagnostic_only_leakage_rows,
        "production_safe_candidate_count": 0,
        "production_promotion_blocked": not production_leakage_rows,
        "ready_for_benchmark": False,
        "ready_for_customer_view": False,
        "diagnostic_only_note": DIAGNOSTIC_ONLY_NOTE,
    }
    report["valid"] = not errors
    return report


def _text(report: dict[str, Any]) -> str:
    lines = [
        f"review_csv_path: {report['review_csv_path']}",
        f"reject_reason_csv_path: {report['reject_reason_csv_path']}",
        f"family: {report['family']}",
        f"valid: {report['valid']}",
        f"total_review_rows: {report['total_review_rows']}",
        f"decision_counts: {report['decision_counts']}",
        f"reject_reason_crosscheck_status: {report['reject_reason_crosscheck_status']}",
        f"reject_reason_tag_counts: {report['reject_reason_tag_counts']}",
        f"diagnostic_only_note: {report['diagnostic_only_note']}",
    ]
    if report["errors"]:
        lines.append("errors:")
        lines.extend(f"- {error}" for error in report["errors"])
    if report["warnings"]:
        lines.append("warnings:")
        lines.extend(f"- {warning}" for warning in report["warnings"])
    return "\n".join(lines)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Validate diagnostic-only TECEdrainprofile drain body length role review CSV.")
    parser.add_argument("--review-csv", required=True)
    parser.add_argument("--reject-reason-csv")
    parser.add_argument("--family", default=EXPECTED_FAMILY)
    parser.add_argument("--json", action="store_true")
    parser.add_argument("--out")
    args = parser.parse_args(argv)
    report = validate_tece_drainprofile_drain_body_length_role_review_csv(
        args.review_csv,
        reject_reason_csv=args.reject_reason_csv,
        family=args.family,
    )
    if args.json:
        write_json_output(report, out=args.out)
    else:
        write_text_output(_text(report), out=args.out)
    return 0 if report["valid"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
