from __future__ import annotations

import argparse
import csv
import json
import sys
from collections import Counter
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from tools.apply_tece_unknown_role_review_csv import DIAGNOSTIC_ONLY_NOTE
from tools.report_tece_unknown_role_contexts import _clean
from tools.tece_report_output import write_json_output, write_text_output

EXPECTED_TECE_PILOT = {
    "applied_count": 133,
    "skipped_blocked_count": 3,
    "skipped_duplicate_match_count": 0,
    "unknown_reduction": 133,
    "current_inventory_role_counts": {
        "accessory": 21,
        "complete_set": 4,
        "cover_or_grate": 58,
        "drain_body": 7,
        "unknown": 136,
    },
    "preview_role_counts_after_safe_apply": {
        "accessory": 40,
        "complete_set": 4,
        "cover_or_grate": 123,
        "drain_body": 56,
        "unknown": 3,
    },
    "blocked_articles": {"650700", "650800", "651500"},
}

STATUS_APPLIED = "applied"
STATUS_BLOCKED = "skipped_blocked"
STATUS_NOT_REVIEWED = "not_reviewed"


def _read_csv(path: str | Path) -> list[dict[str, Any]]:
    with Path(path).open(encoding="utf-8-sig", newline="") as fh:
        return list(csv.DictReader(fh))


def _read_json(path: str | Path) -> dict[str, Any]:
    with Path(path).open(encoding="utf-8-sig") as fh:
        payload = json.load(fh)
    if not isinstance(payload, dict):
        raise ValueError("preview report JSON root must be an object")
    return payload


def _family(row: dict[str, Any]) -> str:
    return _clean(row.get("tece_family_candidate") or row.get("product_family") or row.get("family"))


def _article(row: dict[str, Any]) -> str:
    return _clean(row.get("article_number") or row.get("product_article_number"))


def _original_role(row: dict[str, Any]) -> str:
    return _clean(row.get("original_article_role") or row.get("tece_article_role_candidate")) or "unknown"


def _applied_role(row: dict[str, Any]) -> str:
    return _clean(row.get("applied_article_role") or row.get("tece_article_role_candidate")) or "unknown"


def _status(row: dict[str, Any]) -> str:
    return _clean(row.get("review_apply_status")) or STATUS_NOT_REVIEWED


def _norm_counts(value: Any) -> dict[str, int]:
    if not isinstance(value, dict):
        return {}
    out: dict[str, int] = {}
    for key, count in value.items():
        try:
            out[_clean(key)] = int(count)
        except (TypeError, ValueError):
            continue
    return dict(sorted(out.items()))


def _report_int(report: dict[str, Any], key: str) -> int:
    try:
        return int(report.get(key, 0))
    except (TypeError, ValueError):
        return 0


def _same_counts(a: dict[str, int], b: dict[str, int]) -> bool:
    return {k: v for k, v in a.items() if v} == {k: v for k, v in b.items() if v}


def build_qa_report(preview_csv: str | Path, preview_report: str | Path, family: str = "TECEdrainline") -> dict[str, Any]:
    errors: list[str] = []
    warnings: list[str] = []
    preview_csv_path = str(Path(preview_csv))
    preview_report_path = str(Path(preview_report))

    try:
        rows = _read_csv(preview_csv)
    except Exception as exc:  # pragma: no cover - defensive CLI error path
        rows = []
        errors.append(f"failed to read preview CSV: {exc}")
    try:
        source_report = _read_json(preview_report)
    except Exception as exc:  # pragma: no cover - defensive CLI error path
        source_report = {}
        errors.append(f"failed to read preview report JSON: {exc}")

    family_rows = [row for row in rows if _family(row) == family]
    status_counts = Counter(_status(row) for row in family_rows)
    current_counts = Counter(_original_role(row) for row in family_rows)
    preview_counts = Counter(_applied_role(row) for row in family_rows)
    applied_role_counts = Counter(_applied_role(row) for row in family_rows if _status(row) == STATUS_APPLIED)
    remaining_unknown_rows = [
        {"article_number": _article(row), "review_apply_status": _status(row), "applied_article_role": _applied_role(row)}
        for row in family_rows
        if _applied_role(row) == "unknown"
    ]
    blocked_rows = [
        {"article_number": _article(row), "review_apply_status": _status(row), "applied_article_role": _applied_role(row)}
        for row in family_rows
        if _status(row) == STATUS_BLOCKED
    ]
    non_reviewed_non_unknown_duplicate_article_rows = [
        {"article_number": _article(row), "review_apply_status": _status(row), "applied_article_role": _applied_role(row)}
        for row in family_rows
        if _status(row) not in {STATUS_APPLIED, STATUS_BLOCKED, STATUS_NOT_REVIEWED}
    ]
    evidence_source_file_counts = Counter(
        _clean(row.get("source_file")) or "(blank)" for row in family_rows if _status(row) == STATUS_APPLIED
    )

    if source_report.get("valid") is not True:
        errors.append("preview report is not valid=true")
    if source_report.get("family") and _clean(source_report.get("family")) != family:
        errors.append(f"preview report family {source_report.get('family')!r} does not match expected family {family!r}")

    report_current = _norm_counts(source_report.get("current_inventory_role_counts"))
    report_preview = _norm_counts(source_report.get("preview_role_counts_after_safe_apply"))
    if report_current and not _same_counts(report_current, dict(current_counts)):
        errors.append("current inventory role counts differ between preview CSV and preview report")
    if report_preview and not _same_counts(report_preview, dict(preview_counts)):
        errors.append("preview role counts differ between preview CSV and preview report")

    derived_unknown_reduction = current_counts.get("unknown", 0) - preview_counts.get("unknown", 0)
    checks = {
        "applied_count": status_counts.get(STATUS_APPLIED, 0),
        "skipped_blocked_count": status_counts.get(STATUS_BLOCKED, 0),
        "skipped_duplicate_match_count": status_counts.get("skipped_duplicate_match", 0),
        "unknown_reduction": derived_unknown_reduction,
    }
    for key, derived in checks.items():
        if key in source_report and _report_int(source_report, key) != derived:
            errors.append(f"{key} differs between preview CSV ({derived}) and preview report ({source_report.get(key)!r})")

    if family == "TECEdrainline":
        for key in ("applied_count", "skipped_blocked_count", "skipped_duplicate_match_count", "unknown_reduction"):
            expected = EXPECTED_TECE_PILOT[key]
            actual = checks[key]
            if actual != expected:
                errors.append(f"TECEdrainline pilot {key} expected {expected}, found {actual}")
            if key in source_report and _report_int(source_report, key) != expected:
                errors.append(f"TECEdrainline pilot report {key} expected {expected}, found {source_report.get(key)!r}")
        for key in ("current_inventory_role_counts", "preview_role_counts_after_safe_apply"):
            expected_counts = EXPECTED_TECE_PILOT[key]
            actual_counts = dict(current_counts if key == "current_inventory_role_counts" else preview_counts)
            if not _same_counts(actual_counts, expected_counts):
                errors.append(f"TECEdrainline pilot {key} expected {expected_counts}, found {dict(sorted(actual_counts.items()))}")
        blocked_articles = {_article(row) for row in family_rows if _status(row) == STATUS_BLOCKED and _applied_role(row) == "unknown"}
        if blocked_articles != EXPECTED_TECE_PILOT["blocked_articles"]:
            errors.append(
                "TECEdrainline pilot blocked rows must be exactly skipped_blocked unknown rows "
                f"{sorted(EXPECTED_TECE_PILOT['blocked_articles'])}, found {sorted(blocked_articles)}"
            )

    output = {
        "valid": not errors,
        "errors": errors,
        "warnings": warnings,
        "preview_csv_path": preview_csv_path,
        "preview_report_path": preview_report_path,
        "family": family,
        "total_preview_rows": len(rows),
        "review_total_rows": _report_int(source_report, "total_review_rows"),
        "safe_to_apply_true_count": _report_int(source_report, "safe_to_apply_true_count"),
        "blocked_count": _report_int(source_report, "blocked_count"),
        "applied_count": status_counts.get(STATUS_APPLIED, 0),
        "skipped_blocked_count": status_counts.get(STATUS_BLOCKED, 0),
        "skipped_duplicate_match_count": status_counts.get("skipped_duplicate_match", 0),
        "unknown_reduction": derived_unknown_reduction,
        "current_inventory_role_counts": dict(sorted(current_counts.items())),
        "preview_role_counts_after_safe_apply": dict(sorted(preview_counts.items())),
        "apply_status_counts": dict(sorted(status_counts.items())),
        "applied_role_counts_from_preview_csv": dict(sorted(applied_role_counts.items())),
        "remaining_unknown_rows": remaining_unknown_rows,
        "blocked_rows": blocked_rows,
        "non_reviewed_non_unknown_duplicate_article_rows": non_reviewed_non_unknown_duplicate_article_rows,
        "evidence_source_file_counts_for_applied_rows": dict(sorted(evidence_source_file_counts.items())),
        "production_promotion_blocked": True,
        "ready_for_benchmark": False,
        "ready_for_customer_view": False,
        "diagnostic_only_note": DIAGNOSTIC_ONLY_NOTE,
    }
    return output


def _text(report: dict[str, Any]) -> str:
    keys = [
        "valid", "family", "total_preview_rows", "review_total_rows", "safe_to_apply_true_count",
        "blocked_count", "applied_count", "skipped_blocked_count", "skipped_duplicate_match_count",
        "unknown_reduction", "current_inventory_role_counts", "preview_role_counts_after_safe_apply",
        "apply_status_counts", "blocked_rows", "production_promotion_blocked", "ready_for_benchmark",
        "ready_for_customer_view", "diagnostic_only_note",
    ]
    lines = [f"{key}: {report[key]}" for key in keys]
    if report["errors"]:
        lines.append("errors:")
        lines.extend(f"- {error}" for error in report["errors"])
    if report["warnings"]:
        lines.append("warnings:")
        lines.extend(f"- {warning}" for warning in report["warnings"])
    return "\n".join(lines)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Validate the diagnostic TECE reviewed-role safe-apply preview overlay QA report.")
    parser.add_argument("--preview-csv", required=True)
    parser.add_argument("--preview-report", required=True)
    parser.add_argument("--family", default="TECEdrainline")
    parser.add_argument("--json", action="store_true")
    parser.add_argument("--out", help="Optional JSON output path.")
    args = parser.parse_args(argv)

    report = build_qa_report(args.preview_csv, args.preview_report, family=args.family)
    if args.out:
        write_json_output(report, out=args.out)
    elif args.json:
        write_json_output(report)
    else:
        write_text_output(_text(report))
    return 0 if report["valid"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
