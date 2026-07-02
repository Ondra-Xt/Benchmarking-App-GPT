from __future__ import annotations

import argparse
import csv
import sys
from collections import Counter, defaultdict
from dataclasses import asdict
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from tools.report_tece_source_inventory import load_source_pack
from tools.report_tece_unknown_role_contexts import _clean
from tools.tece_report_output import write_json_output, write_text_output
from tools.validate_tece_unknown_role_review_csv import (
    BLOCKED_DECISIONS,
    validate_unknown_role_review_csv,
)

APPLY_DECISIONS = {"drain_body", "cover_or_grate", "profile_cover", "accessory", "complete_set"}
PREVIEW_COLUMNS = [
    "original_article_role",
    "applied_article_role",
    "review_decision_source",
    "review_safe_to_apply",
    "review_apply_status",
    "review_apply_note",
]
DIAGNOSTIC_ONLY_NOTE = (
    "Diagnostic-only apply preview; writes only the requested preview/report outputs and does not mutate "
    "source-pack files, TECE extraction/classification logic, production promotion flags, or ACO canonical export."
)


def _norm_bool(value: Any) -> str:
    return _clean(value).lower()


def _role(row: Any) -> str:
    return _clean(row.tece_article_role_candidate) or "unknown"


def _family(row: Any) -> str:
    return _clean(row.tece_family_candidate or row.product_family)


def _read_review_rows(review_csv: str | Path) -> list[dict[str, Any]]:
    with Path(review_csv).open(encoding="utf-8-sig", newline="") as fh:
        return list(csv.DictReader(fh))


def _base_preview_row(row: Any) -> dict[str, Any]:
    payload = asdict(row)
    original_role = _role(row)
    payload.update({
        "original_article_role": original_role,
        "applied_article_role": original_role,
        "review_decision_source": "",
        "review_safe_to_apply": "",
        "review_apply_status": "not_reviewed",
        "review_apply_note": "no matching review row considered for this inventory row",
    })
    return payload


def build_apply_preview(review_csv: str | Path, source_pack: str | Path, family: str = "TECEdrainline") -> tuple[list[dict[str, Any]], dict[str, Any]]:
    """Validate and build a safe diagnostic-only TECE unknown-role apply preview.

    The preview is in-memory until callers explicitly write the returned rows. Source
    pack files and production/canonical exporter state are never modified.
    """
    validation = validate_unknown_role_review_csv(review_csv, source_pack, family=family)
    if not validation["valid"]:
        report = {
            **{k: validation[k] for k in ("valid", "errors", "warnings", "total_review_rows", "safe_to_apply_true_count", "blocked_count")},
            "applied_count": 0,
            "skipped_blocked_count": 0,
            "skipped_non_unknown_count": 0,
            "skipped_unmatched_count": 0,
            "skipped_duplicate_match_count": 0,
            "current_inventory_role_counts": {},
            "preview_role_counts_after_safe_apply": {},
            "unknown_reduction": 0,
            "blocked_decision_counts": {},
            "production_promotion_blocked": True,
            "ready_for_benchmark": False,
            "ready_for_customer_view": False,
            "diagnostic_only_note": DIAGNOSTIC_ONLY_NOTE,
        }
        return [], report

    inventory = load_source_pack(source_pack)
    review_name = Path(review_csv).name
    inventory_rows = [row for row in inventory.rows if Path(row.source_file).name != review_name]
    preview_rows = [_base_preview_row(row) for row in inventory_rows]

    current_counts: Counter[str] = Counter()
    unknown_matches_by_key: dict[tuple[str, str], list[int]] = defaultdict(list)
    non_unknown_matches_by_key: dict[tuple[str, str], list[int]] = defaultdict(list)
    for idx, row in enumerate(inventory_rows):
        if _family(row) != family:
            continue
        role = _role(row)
        current_counts[role] += 1
        key = (_clean(row.article_number), _family(row))
        if role == "unknown":
            unknown_matches_by_key[key].append(idx)
        else:
            non_unknown_matches_by_key[key].append(idx)

    review_rows = _read_review_rows(review_csv)
    applied_count = 0
    skipped_blocked_count = 0
    skipped_non_unknown_count = 0
    skipped_unmatched_count = 0
    skipped_duplicate_match_count = 0
    blocked_decision_counts: Counter[str] = Counter()
    touched_inventory_indices: set[int] = set()

    for review_row in review_rows:
        article = _clean(review_row.get("article_number"))
        row_family = _clean(review_row.get("product_family"))
        decision = _clean(review_row.get("reviewer_role_decision")) or "(empty)"
        safe = _norm_bool(review_row.get("safe_to_apply_automatically"))
        is_blocked = safe != "true" or decision not in APPLY_DECISIONS or decision in BLOCKED_DECISIONS
        key = (article, row_family)
        unknown_matches = unknown_matches_by_key.get(key, [])
        non_unknown_matches = non_unknown_matches_by_key.get(key, [])

        if is_blocked:
            skipped_blocked_count += 1
            blocked_decision_counts[decision] += 1
            for idx in unknown_matches:
                preview_rows[idx].update({
                    "review_decision_source": decision,
                    "review_safe_to_apply": safe,
                    "review_apply_status": "skipped_blocked",
                    "review_apply_note": "review row is not safe for automatic diagnostic preview apply",
                })
            continue
        if row_family != family or (not unknown_matches and not non_unknown_matches):
            skipped_unmatched_count += 1
            continue
        if len(unknown_matches) > 1:
            skipped_duplicate_match_count += 1
            for idx in unknown_matches:
                preview_rows[idx].update({
                    "review_decision_source": decision,
                    "review_safe_to_apply": safe,
                    "review_apply_status": "skipped_duplicate_match",
                    "review_apply_note": "review row matched multiple unknown source inventory rows; safe preview apply requires exactly one unknown match",
                })
            continue
        if not unknown_matches:
            skipped_non_unknown_count += 1
            for idx in non_unknown_matches:
                preview_rows[idx].update({
                    "review_decision_source": decision,
                    "review_safe_to_apply": safe,
                    "review_apply_status": "skipped_non_unknown",
                    "review_apply_note": f"current source inventory role is {preview_rows[idx]['original_article_role']!r}, not 'unknown'",
                })
            continue

        idx = unknown_matches[0]
        if idx in touched_inventory_indices:
            continue

        preview_rows[idx].update({
            "applied_article_role": decision,
            "review_decision_source": decision,
            "review_safe_to_apply": safe,
            "review_apply_status": "applied",
            "review_apply_note": "safe diagnostic preview role applied to output CSV only",
        })
        touched_inventory_indices.add(idx)
        applied_count += 1

    preview_counts: Counter[str] = Counter()
    for row in preview_rows:
        if _clean(row.get("tece_family_candidate") or row.get("product_family")) == family:
            preview_counts[_clean(row.get("applied_article_role")) or "unknown"] += 1

    report = {
        "valid": True,
        "errors": validation["errors"],
        "warnings": validation["warnings"],
        "total_review_rows": validation["total_review_rows"],
        "safe_to_apply_true_count": validation["safe_to_apply_true_count"],
        "blocked_count": validation["blocked_count"],
        "applied_count": applied_count,
        "skipped_blocked_count": skipped_blocked_count,
        "skipped_non_unknown_count": skipped_non_unknown_count,
        "skipped_unmatched_count": skipped_unmatched_count,
        "skipped_duplicate_match_count": skipped_duplicate_match_count,
        "current_inventory_role_counts": dict(sorted((k, v) for k, v in current_counts.items() if v)),
        "preview_role_counts_after_safe_apply": dict(sorted((k, v) for k, v in preview_counts.items() if v)),
        "unknown_reduction": current_counts.get("unknown", 0) - preview_counts.get("unknown", 0),
        "blocked_decision_counts": dict(sorted(blocked_decision_counts.items())),
        "production_promotion_blocked": True,
        "ready_for_benchmark": False,
        "ready_for_customer_view": False,
        "diagnostic_only_note": DIAGNOSTIC_ONLY_NOTE,
    }
    return preview_rows, report


def write_preview_csv(rows: list[dict[str, Any]], out: str | Path) -> None:
    fieldnames: list[str] = []
    for row in rows:
        for key in row:
            if key not in fieldnames:
                fieldnames.append(key)
    Path(out).parent.mkdir(parents=True, exist_ok=True)
    with Path(out).open("w", encoding="utf-8-sig", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def _text(report: dict[str, Any]) -> str:
    lines = [f"{key}: {report[key]}" for key in (
        "valid", "total_review_rows", "safe_to_apply_true_count", "blocked_count",
        "applied_count", "skipped_blocked_count", "skipped_non_unknown_count",
        "skipped_unmatched_count", "skipped_duplicate_match_count", "current_inventory_role_counts",
        "preview_role_counts_after_safe_apply", "unknown_reduction", "blocked_decision_counts",
        "production_promotion_blocked", "ready_for_benchmark", "ready_for_customer_view",
        "diagnostic_only_note",
    )]
    if report["errors"]:
        lines.append("errors:")
        lines.extend(f"- {error}" for error in report["errors"])
    if report["warnings"]:
        lines.append("warnings:")
        lines.extend(f"- {warning}" for warning in report["warnings"])
    return "\n".join(lines)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Build a diagnostic-only safe apply preview for TECE unknown-role review CSV decisions.")
    parser.add_argument("--review-csv", required=True)
    parser.add_argument("--source-pack", required=True)
    parser.add_argument("--family", default="TECEdrainline")
    parser.add_argument("--out", required=True, help="Output path for applied preview CSV.")
    parser.add_argument("--report-out", help="Optional JSON report output path.")
    parser.add_argument("--json", action="store_true", help="Emit report JSON to stdout unless --report-out is provided.")
    args = parser.parse_args(argv)

    rows, report = build_apply_preview(args.review_csv, args.source_pack, family=args.family)
    if report["valid"]:
        write_preview_csv(rows, args.out)
    if args.report_out:
        write_json_output(report, out=args.report_out)
    elif args.json:
        write_json_output(report)
    else:
        write_text_output(_text(report))
    return 0 if report["valid"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
