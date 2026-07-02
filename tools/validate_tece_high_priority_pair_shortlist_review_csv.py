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

from tools.export_tece_high_priority_pair_shortlist import BLOCKED_UNKNOWN_ARTICLES, CSV_COLUMNS
from tools.report_tece_unknown_role_contexts import _clean
from tools.tece_report_output import write_json_output, write_text_output

REQUIRED_COLUMNS = CSV_COLUMNS
ALLOWED_DECISIONS = {"compatible", "incompatible", "ambiguous", "keep_for_later", "not_a_pair", ""}
ALLOWED_SAFE_VALUES = {"true", "false", ""}
BLOCKING_DECISIONS = {"ambiguous", "incompatible", "not_a_pair", "keep_for_later"}
DIAGNOSTIC_ONLY_NOTE = (
    "Diagnostic-only validation report for manually reviewed TECE high-priority pair shortlists; this tool reads the "
    "review CSV and optionally writes only this validation report. It does not mutate source-pack files, TECE "
    "extraction/classification logic, production promotion flags, ACO canonical export, Products, Comparison, "
    "BOM_Options, Final_Assemblies, or Final_Set_Details."
)


def _norm_bool(value: Any) -> str:
    return _clean(value).lower()


def _row_sample(row_number: int, row: dict[str, Any], reason: str | None = None) -> dict[str, Any]:
    sample = {
        "row_number": row_number,
        "shortlist_id": _clean(row.get("shortlist_id")),
        "drain_body_article": _clean(row.get("drain_body_article")),
        "candidate_article": _clean(row.get("candidate_article")),
        "nominal_length_mm": _clean(row.get("nominal_length_mm")),
        "reviewer_pair_decision": _clean(row.get("reviewer_pair_decision")),
        "safe_to_apply_automatically": _norm_bool(row.get("safe_to_apply_automatically")),
    }
    if reason:
        sample["reason"] = reason
    return sample


def validate_shortlist_review_csv(review_csv: str | Path, family: str = "TECEdrainline") -> dict[str, Any]:
    """Validate a manually reviewed TECE high-priority shortlist without applying any review decisions."""
    review_path = Path(review_csv)
    errors: list[str] = []
    warnings: list[str] = []

    with review_path.open(encoding="utf-8-sig", newline="") as fh:
        reader = csv.DictReader(fh)
        fieldnames = reader.fieldnames or []
        missing = [column for column in REQUIRED_COLUMNS if column not in fieldnames]
        if missing:
            errors.append(f"missing_required_columns: {', '.join(missing)}")
        rows = list(reader)

    decision_counts: Counter[str] = Counter()
    shortlist_ids: Counter[str] = Counter()
    pair_keys: Counter[tuple[str, str, str]] = Counter()
    duplicate_shortlist_ids: list[str] = []
    duplicate_pair_keys: list[dict[str, str]] = []
    invalid_rows_sample: list[dict[str, Any]] = []
    compatible_safe_rows_sample: list[dict[str, Any]] = []
    ambiguous_rows_sample: list[dict[str, Any]] = []
    safe_to_apply_true_count = 0
    blocked_or_not_safe_count = 0
    empty_decision_count = 0

    def add_error(message: str, row_number: int | None = None, row: dict[str, Any] | None = None) -> None:
        errors.append(message)
        if row_number is not None and row is not None and len(invalid_rows_sample) < 25:
            invalid_rows_sample.append(_row_sample(row_number, row, message))

    for row_number, row in enumerate(rows, start=2):
        decision = _clean(row.get("reviewer_pair_decision")).lower()
        safe = _norm_bool(row.get("safe_to_apply_automatically"))
        notes = _clean(row.get("reviewer_notes"))
        body = _clean(row.get("drain_body_article"))
        candidate = _clean(row.get("candidate_article"))
        length = _clean(row.get("nominal_length_mm"))
        shortlist_id = _clean(row.get("shortlist_id"))

        decision_counts[decision or "(empty)"] += 1
        if not decision:
            empty_decision_count += 1
        if safe == "true":
            safe_to_apply_true_count += 1
        if safe != "true" or decision in BLOCKING_DECISIONS or not decision:
            blocked_or_not_safe_count += 1
        if safe == "true" and decision == "compatible" and len(compatible_safe_rows_sample) < 25:
            compatible_safe_rows_sample.append(_row_sample(row_number, row))
        if decision == "ambiguous" and len(ambiguous_rows_sample) < 25:
            ambiguous_rows_sample.append(_row_sample(row_number, row))

        shortlist_ids[shortlist_id] += 1
        pair_key = (body, candidate, length)
        pair_keys[pair_key] += 1

        expected_values = {
            "family": family,
            "pair_type": "drain_body_to_cover_or_grate",
            "diagnostic_review_priority": "high_review_priority",
            "status": "diagnostic_exact_length_match",
            "diagnostic_only": "true",
            "production_safe": "false",
            "production_promotion_blocked": "true",
            "ready_for_benchmark": "false",
            "ready_for_customer_view": "false",
        }
        for column, expected in expected_values.items():
            actual = _norm_bool(row.get(column)) if expected in {"true", "false"} else _clean(row.get(column))
            if actual != expected:
                add_error(f"row {row_number}: {column} must be {expected!r}, got {actual!r}", row_number, row)
        if not body:
            add_error(f"row {row_number}: drain_body_article must be non-empty", row_number, row)
        if not candidate:
            add_error(f"row {row_number}: candidate_article must be non-empty", row_number, row)
        if not length:
            add_error(f"row {row_number}: nominal_length_mm must be non-empty", row_number, row)
        blocked = sorted({body, candidate} & BLOCKED_UNKNOWN_ARTICLES)
        if blocked:
            add_error(f"row {row_number}: blocked article appears: {', '.join(blocked)}", row_number, row)
        if decision not in ALLOWED_DECISIONS:
            add_error(f"row {row_number}: invalid reviewer_pair_decision {decision!r}", row_number, row)
        if safe not in ALLOWED_SAFE_VALUES:
            add_error(f"row {row_number}: invalid safe_to_apply_automatically {safe!r}", row_number, row)
        if safe == "true" and decision != "compatible":
            add_error(f"row {row_number}: safe_to_apply_automatically=true requires reviewer_pair_decision='compatible'", row_number, row)
        if safe == "true" and not notes:
            add_error(f"row {row_number}: safe_to_apply_automatically=true requires non-empty reviewer_notes", row_number, row)
        if decision in BLOCKING_DECISIONS and safe == "true":
            add_error(f"row {row_number}: reviewer_pair_decision={decision!r} must have safe_to_apply_automatically false or empty", row_number, row)

    for value, count in sorted(shortlist_ids.items()):
        if value and count > 1:
            duplicate_shortlist_ids.append(value)
            errors.append(f"duplicate shortlist_id: {value}")
    for (body, candidate, length), count in sorted(pair_keys.items()):
        if body and candidate and length and count > 1:
            duplicate_pair_keys.append({"drain_body_article": body, "candidate_article": candidate, "nominal_length_mm": length})
            errors.append(f"duplicate pair key: {body} + {candidate} + {length}")

    return {
        "valid": not errors,
        "errors": errors,
        "warnings": warnings,
        "review_csv_path": str(review_path),
        "family": family,
        "total_review_rows": len(rows),
        "decision_counts": dict(sorted(decision_counts.items())),
        "safe_to_apply_true_count": safe_to_apply_true_count,
        "blocked_or_not_safe_count": blocked_or_not_safe_count,
        "empty_decision_count": empty_decision_count,
        "duplicate_shortlist_ids": duplicate_shortlist_ids,
        "duplicate_pair_keys": duplicate_pair_keys,
        "invalid_rows_sample": invalid_rows_sample,
        "compatible_safe_rows_sample": compatible_safe_rows_sample,
        "ambiguous_rows_sample": ambiguous_rows_sample,
        "production_safe_candidate_count": 0,
        "production_promotion_blocked": True,
        "ready_for_benchmark": False,
        "ready_for_customer_view": False,
        "diagnostic_only_note": DIAGNOSTIC_ONLY_NOTE,
    }


def _text(report: dict[str, Any]) -> str:
    lines = [
        f"review_csv_path: {report['review_csv_path']}",
        f"family: {report['family']}",
        f"valid: {report['valid']}",
        f"total_review_rows: {report['total_review_rows']}",
        f"decision_counts: {report['decision_counts']}",
        f"safe_to_apply_true_count: {report['safe_to_apply_true_count']}",
        f"blocked_or_not_safe_count: {report['blocked_or_not_safe_count']}",
        "production_safe_candidate_count: 0",
        "production_promotion_blocked: true",
        "ready_for_benchmark: false",
        "ready_for_customer_view: false",
    ]
    if report["errors"]:
        lines.append("errors:")
        lines.extend(f"- {error}" for error in report["errors"])
    if report["warnings"]:
        lines.append("warnings:")
        lines.extend(f"- {warning}" for warning in report["warnings"])
    return "\n".join(lines)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Validate diagnostic-only TECE high-priority pair shortlist review CSV.")
    parser.add_argument("--review-csv", required=True)
    parser.add_argument("--family", default="TECEdrainline")
    parser.add_argument("--json", action="store_true")
    parser.add_argument("--out")
    args = parser.parse_args(argv)
    report = validate_shortlist_review_csv(args.review_csv, family=args.family)
    if args.json:
        write_json_output(report, out=args.out)
    else:
        write_text_output(_text(report), out=args.out)
    return 0 if report["valid"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
