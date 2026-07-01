from __future__ import annotations

import argparse
import csv
import sys
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from tools.report_tece_source_inventory import load_source_pack
from tools.report_tece_unknown_role_contexts import _clean
from tools.tece_report_output import write_json_output, write_text_output
from tools.validate_tece_unknown_role_review_csv import (
    ALLOWED_DIAGNOSTIC_DECISIONS,
    BLOCKED_DECISIONS,
    REQUIRED_COLUMNS,
    validate_unknown_role_review_csv,
)

EXAMPLE_LIMIT = 5


def _role(row: Any) -> str:
    return _clean(row.tece_article_role_candidate) or "unknown"


def _norm_bool(value: Any) -> str:
    return _clean(value).lower()


def _read_rows(review_csv: str | Path) -> tuple[list[dict[str, Any]], list[str]]:
    with Path(review_csv).open(encoding="utf-8-sig", newline="") as fh:
        reader = csv.DictReader(fh)
        return list(reader), list(reader.fieldnames or [])


def build_impact_report(review_csv: str | Path, source_pack: str | Path, family: str = "TECEdrainline") -> dict[str, Any]:
    """Build a diagnostic-only impact report for TECE unknown-role decisions.

    The report validates and reads the review CSV, then models what role counts would
    look like if only non-blocked safe_to_apply_automatically=true decisions were
    applied. It intentionally does not mutate source-pack files, inventory rows,
    role classification, TECE promotion status, or ACO canonical exports.
    """
    validation = validate_unknown_role_review_csv(review_csv, source_pack, family=family)
    inventory = load_source_pack(source_pack)
    review_rows, fieldnames = _read_rows(review_csv)

    missing = [column for column in REQUIRED_COLUMNS if column not in fieldnames]
    current_role_counts: Counter[str] = Counter()
    unknown_articles: set[str] = set()
    review_name = Path(review_csv).name
    for row in inventory.rows:
        if Path(row.source_file).name == review_name:
            continue
        row_family = _clean(row.tece_family_candidate or row.product_family)
        role = _role(row)
        if row_family != family:
            continue
        current_role_counts[role] += 1
        if role == "unknown":
            unknown_articles.add(row.article_number)

    hypothetical_role_counts = Counter(current_role_counts)
    safe_applied_articles: set[str] = set()
    blocked_decision_counts: Counter[str] = Counter()
    examples_by_decision: dict[str, list[dict[str, Any]]] = defaultdict(list)

    for row in review_rows:
        article = _clean(row.get("article_number"))
        decision = _clean(row.get("reviewer_role_decision")) or "(empty)"
        safe = _norm_bool(row.get("safe_to_apply_automatically"))
        blocked = decision in {"(empty)", *BLOCKED_DECISIONS} or safe != "true"

        if blocked:
            blocked_decision_counts[decision] += 1
        elif decision in ALLOWED_DIAGNOSTIC_DECISIONS and article in unknown_articles and article not in safe_applied_articles:
            hypothetical_role_counts["unknown"] -= 1
            hypothetical_role_counts[decision] += 1
            safe_applied_articles.add(article)

        if len(examples_by_decision[decision]) < EXAMPLE_LIMIT:
            examples_by_decision[decision].append({
                "article_number": article,
                "safe_to_apply_automatically": safe,
                "source_file": _clean(row.get("source_file")),
                "context_group": _clean(row.get("context_group")),
                "evidence_text_snippet": _clean(row.get("evidence_text_snippet")),
            })

    current_unknown = current_role_counts.get("unknown", 0)
    hypothetical_unknown = hypothetical_role_counts.get("unknown", 0)
    report = {
        "review_csv_path": str(Path(review_csv)),
        "source_pack_path": str(source_pack),
        "family": family,
        "valid": validation["valid"] and not missing,
        "errors": validation["errors"],
        "warnings": validation["warnings"],
        "total_review_rows": validation["total_review_rows"],
        "matched_unknown_inventory_rows": validation["matched_unknown_inventory_rows"],
        "decision_counts": validation["decision_counts"],
        "safe_to_apply_true_count": validation["safe_to_apply_true_count"],
        "blocked_count": validation["blocked_count"],
        "current_inventory_role_counts": dict(sorted((k, v) for k, v in current_role_counts.items() if v)),
        "hypothetical_role_counts_after_safe_apply_only": dict(sorted((k, v) for k, v in hypothetical_role_counts.items() if v)),
        "hypothetical_unknown_reduction_safe_apply_only": current_unknown - hypothetical_unknown,
        "blocked_decision_counts": dict(sorted(blocked_decision_counts.items())),
        "examples_by_decision": dict(sorted(examples_by_decision.items())),
        "production_promotion_blocked": True,
        "ready_for_benchmark": False,
        "ready_for_customer_view": False,
        "diagnostic_only_note": "Impact report only; no decisions are applied and no TECE inventory, role classification, promotion state, or ACO canonical export is changed.",
    }
    return report


def _text(report: dict[str, Any]) -> str:
    lines = [
        f"review_csv_path: {report['review_csv_path']}",
        f"source_pack_path: {report['source_pack_path']}",
        f"family: {report['family']}",
        f"valid: {report['valid']}",
        f"total_review_rows: {report['total_review_rows']}",
        f"matched_unknown_inventory_rows: {report['matched_unknown_inventory_rows']}",
        f"decision_counts: {report['decision_counts']}",
        f"safe_to_apply_true_count: {report['safe_to_apply_true_count']}",
        f"blocked_count: {report['blocked_count']}",
        f"current_inventory_role_counts: {report['current_inventory_role_counts']}",
        f"hypothetical_role_counts_after_safe_apply_only: {report['hypothetical_role_counts_after_safe_apply_only']}",
        f"hypothetical_unknown_reduction_safe_apply_only: {report['hypothetical_unknown_reduction_safe_apply_only']}",
        f"blocked_decision_counts: {report['blocked_decision_counts']}",
        f"examples_by_decision: {report['examples_by_decision']}",
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
    parser = argparse.ArgumentParser(description="Report diagnostic-only impact of TECE unknown-role manual review CSV decisions.")
    parser.add_argument("--review-csv", required=True, help="Path to the manually filled TECE unknown-role review CSV.")
    parser.add_argument("--source-pack", required=True, help="Path to current TECE source pack directory or file.")
    parser.add_argument("--family", default="TECEdrainline", help="TECE family to report; default: TECEdrainline.")
    parser.add_argument("--json", action="store_true", help="Emit JSON instead of text.")
    parser.add_argument("--out", help="Optional output path.")
    args = parser.parse_args(argv)

    report = build_impact_report(args.review_csv, args.source_pack, family=args.family)
    if args.json:
        write_json_output(report, out=args.out)
    else:
        write_text_output(_text(report), out=args.out)
    return 0 if report["valid"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
