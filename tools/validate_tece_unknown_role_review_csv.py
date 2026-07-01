from __future__ import annotations

import argparse
import csv
import sys
from collections import Counter
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from tools.report_tece_source_inventory import load_source_pack
from tools.report_tece_unknown_role_contexts import _clean
from tools.tece_report_output import write_json_output, write_text_output

REQUIRED_COLUMNS = [
    "article_number",
    "product_family",
    "article_role",
    "tece_article_role_candidate",
    "extraction_method",
    "extraction_priority",
    "source_file",
    "page_range_label",
    "candidate_terms",
    "context_group",
    "evidence_text_snippet",
    "reviewer_role_decision",
    "reviewer_notes",
    "safe_to_apply_automatically",
]

ALLOWED_DIAGNOSTIC_DECISIONS = {
    "drain_body",
    "cover_or_grate",
    "profile_cover",
    "accessory",
    "complete_set",
    "not_product",
    "ambiguous",
    "keep_unknown",
}

BLOCKED_DECISIONS = {"ambiguous", "keep_unknown"}


def _norm_bool(value: Any) -> str:
    return _clean(value).lower()


def _unknown_inventory_articles(source_pack: str | Path, family: str, review_csv: str | Path | None = None) -> set[str]:
    inventory = load_source_pack(source_pack)
    excluded_name = Path(review_csv).name if review_csv is not None else None
    return {
        row.article_number
        for row in inventory.rows
        if (excluded_name is None or Path(row.source_file).name != excluded_name)
        and _clean(row.tece_family_candidate or row.product_family) == family
        and (_clean(row.tece_article_role_candidate) or "unknown") == "unknown"
    }


def validate_unknown_role_review_csv(review_csv: str | Path, source_pack: str | Path, family: str = "TECEdrainline") -> dict[str, Any]:
    """Validate a manually filled TECE unknown-role review CSV without applying it.

    This diagnostic validator intentionally never mutates source-pack files,
    inventory rows, role classifications, TECE promotion state, or ACO exports.
    """
    review_path = Path(review_csv)
    unknown_articles = _unknown_inventory_articles(source_pack, family, review_csv=review_csv)
    errors: list[str] = []
    warnings: list[str] = []

    with review_path.open(encoding="utf-8-sig", newline="") as fh:
        reader = csv.DictReader(fh)
        fieldnames = reader.fieldnames or []
        missing = [column for column in REQUIRED_COLUMNS if column not in fieldnames]
        if missing:
            errors.append(f"missing_required_columns: {', '.join(missing)}")
        rows = list(reader)

    seen: Counter[tuple[str, str]] = Counter()
    decision_counts: Counter[str] = Counter()
    matched_articles: set[str] = set()
    unmatched_rows: list[dict[str, Any]] = []
    duplicate_review_rows: list[dict[str, Any]] = []
    safe_true_count = 0
    blocked_count = 0

    for index, row in enumerate(rows, start=2):
        article = _clean(row.get("article_number"))
        snippet = _clean(row.get("evidence_text_snippet"))
        row_family = _clean(row.get("product_family"))
        article_role = _clean(row.get("article_role")) or "unknown"
        decision = _clean(row.get("reviewer_role_decision"))
        safe = _norm_bool(row.get("safe_to_apply_automatically"))

        if row_family != family:
            errors.append(f"row {index}: product_family {row_family!r} does not match selected family {family!r}")
        if article_role != "unknown":
            errors.append(f"row {index}: article_role must be unknown, got {article_role!r}")
        if article in unknown_articles:
            matched_articles.add(article)
        else:
            unmatched_rows.append({"row_number": index, "article_number": article})
            warnings.append(f"row {index}: article_number {article!r} not found in current {family} unknown-role inventory")

        key = (article, snippet)
        seen[key] += 1
        if seen[key] == 2:
            duplicate_review_rows.append({"article_number": article, "evidence_text_snippet": snippet})
            errors.append(f"duplicate review row for article_number={article!r} evidence_text_snippet={snippet!r}")

        if decision:
            decision_counts[decision] += 1
            if decision not in ALLOWED_DIAGNOSTIC_DECISIONS:
                errors.append(f"row {index}: invalid reviewer_role_decision {decision!r}")
        else:
            decision_counts["(empty)"] += 1

        if safe not in {"", "true", "false"}:
            errors.append(f"row {index}: safe_to_apply_automatically must be empty, true, or false; got {safe!r}")
        if safe == "true":
            safe_true_count += 1
            if not decision or decision in BLOCKED_DECISIONS:
                errors.append(f"row {index}: safe_to_apply_automatically=true requires a non-blocked reviewer_role_decision")
        if decision in BLOCKED_DECISIONS and safe == "true":
            errors.append(f"row {index}: reviewer_role_decision={decision!r} cannot be safe_to_apply_automatically=true")
        if not decision or decision in BLOCKED_DECISIONS or safe != "true":
            blocked_count += 1

    return {
        "review_csv_path": str(review_path),
        "source_pack_path": str(source_pack),
        "family": family,
        "valid": not errors,
        "errors": errors,
        "warnings": warnings,
        "total_review_rows": len(rows),
        "matched_unknown_inventory_rows": sum(1 for row in rows if _clean(row.get("article_number")) in unknown_articles),
        "unmatched_rows": unmatched_rows,
        "duplicate_review_rows": duplicate_review_rows,
        "decision_counts": dict(sorted(decision_counts.items())),
        "safe_to_apply_true_count": safe_true_count,
        "blocked_count": blocked_count,
        "production_promotion_blocked": True,
        "ready_for_benchmark": False,
        "ready_for_customer_view": False,
        "diagnostic_only_note": "Validation report only; no source-pack, inventory, role classification, TECE promotion, or ACO canonical export changes are applied.",
    }


def _text(report: dict[str, Any]) -> str:
    lines = [
        f"review_csv_path: {report['review_csv_path']}",
        f"source_pack_path: {report['source_pack_path']}",
        f"family: {report['family']}",
        f"valid: {report['valid']}",
        f"total_review_rows: {report['total_review_rows']}",
        f"matched_unknown_inventory_rows: {report['matched_unknown_inventory_rows']}",
        f"unmatched_rows: {len(report['unmatched_rows'])}",
        f"duplicate_review_rows: {len(report['duplicate_review_rows'])}",
        f"decision_counts: {report['decision_counts']}",
        f"safe_to_apply_true_count: {report['safe_to_apply_true_count']}",
        f"blocked_count: {report['blocked_count']}",
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
    parser = argparse.ArgumentParser(description="Validate diagnostic-only TECE unknown-role manual review CSV.")
    parser.add_argument("--review-csv", required=True, help="Path to the manually filled TECE unknown-role review CSV.")
    parser.add_argument("--source-pack", required=True, help="Path to current TECE source pack directory or file.")
    parser.add_argument("--family", default="TECEdrainline", help="TECE family to validate; default: TECEdrainline.")
    parser.add_argument("--json", action="store_true", help="Emit JSON instead of text.")
    parser.add_argument("--out", help="Optional output path.")
    args = parser.parse_args(argv)

    report = validate_unknown_role_review_csv(args.review_csv, args.source_pack, family=args.family)
    if args.json:
        write_json_output(report, out=args.out)
    else:
        write_text_output(_text(report), out=args.out)
    return 0 if report["valid"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
