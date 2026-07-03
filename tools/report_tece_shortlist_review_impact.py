from __future__ import annotations

import argparse
import csv
import json
import sys
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any

if __package__ in {None, ""}:
    repo_root = Path(__file__).resolve().parents[1]
    if str(repo_root) not in sys.path:
        sys.path.insert(0, str(repo_root))

from tools.export_tece_high_priority_pair_shortlist import BLOCKED_UNKNOWN_ARTICLES, CSV_COLUMNS
from tools.report_tece_unknown_role_contexts import _clean
from tools.tece_report_output import write_json_output, write_text_output

BLOCKING_DECISIONS = {"ambiguous", "incompatible", "keep_for_later", "not_a_pair", ""}
SAMPLE_LIMIT = 25
DIAGNOSTIC_ONLY_NOTE = (
    "Diagnostic-only impact report for manually reviewed TECE high-priority pair shortlist CSVs. This tool reads the "
    "review CSV plus its validation report and optionally writes only this impact report. It does not mutate "
    "source-pack files, TECE extraction/classification logic, TECE production promotion flags, ACO canonical export, "
    "Products, Comparison, BOM_Options, Final_Assemblies, or Final_Set_Details. Production promotion remains blocked."
)
VARIANT_MARKERS = (
    "colour", "color", "black", "white", "gold", "red gold", "brass", "copper", "nickel", "chrome", "stainless",
    "special", "variant", "farb", "schwarz", "weiß", "weiss", "gold", "messing", "kupfer", "edelstahl",
)


def _norm_bool(value: Any) -> str:
    return _clean(value).lower()


def _is_true(value: Any) -> bool:
    return _norm_bool(value) == "true" or value is True


def _read_review_csv(path: str | Path) -> tuple[list[dict[str, Any]], list[str]]:
    review_path = Path(path)
    errors: list[str] = []
    try:
        with review_path.open(encoding="utf-8-sig", newline="") as fh:
            reader = csv.DictReader(fh)
            fieldnames = reader.fieldnames or []
            missing = [column for column in CSV_COLUMNS if column not in fieldnames]
            if missing:
                errors.append(f"missing_required_columns: {', '.join(missing)}")
            return list(reader), errors
    except Exception as exc:
        return [], [f"review CSV could not be read: {exc}"]


def _load_validation_report(path: str | Path) -> tuple[dict[str, Any], list[str]]:
    try:
        with Path(path).open(encoding="utf-8") as fh:
            data = json.load(fh)
    except Exception as exc:
        return {}, [f"validation report could not be read as JSON: {exc}"]
    if not isinstance(data, dict):
        return {}, ["validation report is not a JSON object"]
    if data.get("valid") is not True:
        return data, ["validation report is invalid"] + [str(e) for e in data.get("errors", [])]
    return data, []


def _sample(row_number: int, row: dict[str, Any]) -> dict[str, Any]:
    return {
        "row_number": row_number,
        "shortlist_id": _clean(row.get("shortlist_id")),
        "drain_body_article": _clean(row.get("drain_body_article")),
        "candidate_article": _clean(row.get("candidate_article")),
        "nominal_length_mm": _clean(row.get("nominal_length_mm")),
        "reviewer_pair_decision": _clean(row.get("reviewer_pair_decision")).lower(),
        "safe_to_apply_automatically": _norm_bool(row.get("safe_to_apply_automatically")),
    }


def _variant_text(row: dict[str, Any]) -> str:
    fields = [
        "candidate_role", "drain_body_evidence_text_snippet", "candidate_evidence_text_snippet", "reviewer_notes",
        "drain_body_source_file", "candidate_source_file",
    ]
    return " ".join(_clean(row.get(field)).lower() for field in fields)


def _has_color_or_special_variant_marker(row: dict[str, Any]) -> bool:
    text = _variant_text(row)
    return any(marker in text for marker in VARIANT_MARKERS)


def _nested_counter_to_dict(counter: dict[str, Counter[str]]) -> dict[str, dict[str, int]]:
    return {decision: dict(sorted(values.items())) for decision, values in sorted(counter.items())}


def report_shortlist_review_impact(review_csv: str | Path, validation_report: str | Path, family: str = "TECEdrainline") -> dict[str, Any]:
    errors: list[str] = []
    warnings: list[str] = []
    validation_data, validation_errors = _load_validation_report(validation_report)
    errors.extend(validation_errors)
    rows, csv_errors = _read_review_csv(review_csv)
    errors.extend(csv_errors)

    decision_counts: Counter[str] = Counter()
    length_distribution: dict[str, Counter[str]] = defaultdict(Counter)
    drain_body_distribution: dict[str, Counter[str]] = defaultdict(Counter)
    candidate_distribution: dict[str, Counter[str]] = defaultdict(Counter)
    compatible_safe_pairs_sample: list[dict[str, Any]] = []
    ambiguous_pairs_sample: list[dict[str, Any]] = []
    incompatible_pairs_sample: list[dict[str, Any]] = []
    safe_to_apply_true_count = 0
    compatible_safe_pair_count = 0
    production_safe_leak_count = 0

    for row_number, row in enumerate(rows, start=2):
        decision = _clean(row.get("reviewer_pair_decision")).lower() or "(empty)"
        raw_decision = "" if decision == "(empty)" else decision
        safe = _is_true(row.get("safe_to_apply_automatically"))
        body = _clean(row.get("drain_body_article"))
        candidate = _clean(row.get("candidate_article"))
        length = _clean(row.get("nominal_length_mm"))

        decision_counts[decision] += 1
        length_distribution[decision][length or "(empty)"] += 1
        drain_body_distribution[decision][body or "(empty)"] += 1
        candidate_distribution[decision][candidate or "(empty)"] += 1

        if safe:
            safe_to_apply_true_count += 1
        if raw_decision == "compatible" and safe:
            compatible_safe_pair_count += 1
            if len(compatible_safe_pairs_sample) < SAMPLE_LIMIT:
                compatible_safe_pairs_sample.append(_sample(row_number, row))
        if raw_decision == "ambiguous" and len(ambiguous_pairs_sample) < SAMPLE_LIMIT:
            ambiguous_pairs_sample.append(_sample(row_number, row))
        if raw_decision == "incompatible" and len(incompatible_pairs_sample) < SAMPLE_LIMIT:
            incompatible_pairs_sample.append(_sample(row_number, row))

        if safe and raw_decision != "compatible":
            errors.append(f"row {row_number}: safe_to_apply_automatically=true is only allowed for compatible rows")
        if raw_decision in BLOCKING_DECISIONS and safe:
            errors.append(f"row {row_number}: {raw_decision or 'empty'} row must remain not safe")
        if _is_true(row.get("production_safe")):
            production_safe_leak_count += 1
            errors.append(f"row {row_number}: production_safe=true is forbidden in diagnostic impact reports")
        if _is_true(row.get("ready_for_benchmark")):
            errors.append(f"row {row_number}: ready_for_benchmark=true is forbidden")
        if _is_true(row.get("ready_for_customer_view")):
            errors.append(f"row {row_number}: ready_for_customer_view=true is forbidden")
        blocked = sorted({body, candidate} & BLOCKED_UNKNOWN_ARTICLES)
        if blocked:
            errors.append(f"row {row_number}: blocked article appears: {', '.join(blocked)}")
        if raw_decision == "ambiguous" and _has_color_or_special_variant_marker(row) and safe:
            errors.append(f"row {row_number}: color/special variant ambiguous row must remain not safe")

    if safe_to_apply_true_count != compatible_safe_pair_count:
        errors.append(
            "safe_to_apply_true_count must equal compatible rows with safe_to_apply_automatically=true "
            f"({safe_to_apply_true_count} != {compatible_safe_pair_count})"
        )
    if validation_data and validation_data.get("review_csv_path") and str(validation_data.get("review_csv_path")) != str(review_csv):
        warnings.append("validation report review_csv_path differs from --review-csv")

    ambiguous_count = decision_counts.get("ambiguous", 0)
    incompatible_count = decision_counts.get("incompatible", 0)
    not_safe_count = len(rows) - safe_to_apply_true_count

    return {
        "valid": not errors,
        "errors": errors,
        "warnings": warnings,
        "review_csv_path": str(Path(review_csv)),
        "validation_report_path": str(Path(validation_report)),
        "family": family,
        "total_review_rows": len(rows),
        "decision_counts": dict(sorted(decision_counts.items())),
        "safe_to_apply_true_count": safe_to_apply_true_count,
        "compatible_safe_pair_count": compatible_safe_pair_count,
        "ambiguous_count": ambiguous_count,
        "incompatible_count": incompatible_count,
        "not_safe_count": not_safe_count,
        "compatible_safe_pairs_sample": compatible_safe_pairs_sample,
        "ambiguous_pairs_sample": ambiguous_pairs_sample,
        "incompatible_pairs_sample": incompatible_pairs_sample,
        "length_distribution_by_decision": _nested_counter_to_dict(length_distribution),
        "drain_body_distribution_by_decision": _nested_counter_to_dict(drain_body_distribution),
        "candidate_distribution_by_decision": _nested_counter_to_dict(candidate_distribution),
        "production_safe_candidate_count": 0,
        "production_promotion_blocked": True,
        "ready_for_benchmark": False,
        "ready_for_customer_view": False,
        "diagnostic_only_note": DIAGNOSTIC_ONLY_NOTE,
    }


def _text(report: dict[str, Any]) -> str:
    return "\n".join([
        f"valid: {report['valid']}",
        f"review_csv_path: {report['review_csv_path']}",
        f"validation_report_path: {report['validation_report_path']}",
        f"family: {report['family']}",
        f"total_review_rows: {report['total_review_rows']}",
        f"decision_counts: {report['decision_counts']}",
        f"safe_to_apply_true_count: {report['safe_to_apply_true_count']}",
        f"compatible_safe_pair_count: {report['compatible_safe_pair_count']}",
        f"ambiguous_count: {report['ambiguous_count']}",
        f"incompatible_count: {report['incompatible_count']}",
        "production_safe_candidate_count: 0",
        "production_promotion_blocked: true",
        "ready_for_benchmark: false",
        "ready_for_customer_view: false",
    ])


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Report diagnostic-only impact of reviewed TECE high-priority shortlist CSV.")
    parser.add_argument("--review-csv", required=True)
    parser.add_argument("--validation-report", required=True)
    parser.add_argument("--family", default="TECEdrainline")
    parser.add_argument("--json", action="store_true")
    parser.add_argument("--out")
    args = parser.parse_args(argv)
    report = report_shortlist_review_impact(args.review_csv, args.validation_report, family=args.family)
    if args.json:
        write_json_output(report, out=args.out)
    else:
        write_text_output(_text(report), out=args.out)
    return 0 if report["valid"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
