from __future__ import annotations

import argparse
import csv
import json
import sys
from collections import Counter
from pathlib import Path
from typing import Any

if __package__ in {None, ""}:
    repo_root = Path(__file__).resolve().parents[1]
    if str(repo_root) not in sys.path:
        sys.path.insert(0, str(repo_root))

from tools.export_tece_high_priority_pair_shortlist import BLOCKED_UNKNOWN_ARTICLES, CSV_COLUMNS
from tools.export_tece_reviewed_compatibility_pairs import OUTPUT_COLUMNS
from tools.report_tece_shortlist_review_impact import _is_true
from tools.report_tece_unknown_role_contexts import _clean

EXPECTED_REVIEWED_PAIRS_ROW_COUNT = 34
EXPECTED_REPORT_VALUES = {
    "exported_pair_count": 34,
    "expected_exported_pair_count": 34,
    "excluded_ambiguous_count": 22,
    "excluded_incompatible_count": 24,
    "production_safe_candidate_count": 0,
    "production_promotion_blocked": True,
    "ready_for_benchmark": False,
    "ready_for_customer_view": False,
}
EXPECTED_LENGTH_DISTRIBUTION = {"700": 34}
EXPECTED_DRAIN_BODY_DISTRIBUTION = {
    "600700": 8, "600701": 8, "600702": 8, "600703": 8, "600705": 1, "600707": 1,
}
EXPECTED_CANDIDATE_DISTRIBUTION = {
    "600710": 4, "600711": 4, "600751": 4, "600770": 4,
    "600772": 4, "600782": 4, "600783": 4, "600785": 6,
}
BLOCKING_DECISIONS = {"ambiguous", "incompatible", "keep_for_later", "not_a_pair", ""}
DIAGNOSTIC_ONLY_NOTE = (
    "This report is read-only and diagnostic-only. It validates exported manually reviewed TECEdrainline compatibility "
    "pairs and does not mutate source-pack files, TECE logic, ACO export, Products, Comparison, BOM_Options, "
    "Final_Assemblies, or Final_Set_Details. Production promotion remains blocked."
)


def _load_json(path: str | Path, label: str) -> tuple[dict[str, Any], list[str]]:
    try:
        data = json.loads(Path(path).read_text(encoding="utf-8"))
    except Exception as exc:
        return {}, [f"{label} is missing or invalid: {exc}"]
    if not isinstance(data, dict):
        return {}, [f"{label} is not a JSON object"]
    if data.get("valid") is not True:
        return data, [f"{label} is invalid"] + [str(e) for e in data.get("errors", [])]
    return data, []


def _read_csv(path: str | Path, required: list[str], label: str) -> tuple[list[dict[str, Any]], list[str]]:
    try:
        with Path(path).open(encoding="utf-8-sig", newline="") as fh:
            reader = csv.DictReader(fh)
            fields = reader.fieldnames or []
            missing = [c for c in required if c not in fields]
            errors = [f"{label} missing_required_columns: {', '.join(missing)}"] if missing else []
            return list(reader), errors
    except Exception as exc:
        return [], [f"{label} is missing or could not be read: {exc}"]


def _key(row: dict[str, Any]) -> tuple[str, str, str]:
    return (_clean(row.get("drain_body_article")), _clean(row.get("candidate_article")), _clean(row.get("nominal_length_mm")))


def _sample(row_number: int, row: dict[str, Any], reason: str | None = None) -> dict[str, Any]:
    item: dict[str, Any] = {
        "row_number": row_number,
        "reviewed_pair_id": _clean(row.get("reviewed_pair_id")),
        "source_shortlist_id": _clean(row.get("source_shortlist_id")),
        "drain_body_article": _clean(row.get("drain_body_article")),
        "candidate_article": _clean(row.get("candidate_article")),
        "nominal_length_mm": _clean(row.get("nominal_length_mm")),
        "reviewer_pair_decision": _clean(row.get("reviewer_pair_decision")).lower(),
        "safe_to_apply_automatically": _clean(row.get("safe_to_apply_automatically")).lower(),
    }
    if reason:
        item["reason"] = reason
    return item


def report_reviewed_compatibility_pairs_qa(
    reviewed_pairs_csv: str | Path,
    reviewed_pairs_report: str | Path,
    review_csv: str | Path,
    validation_report: str | Path,
    impact_report: str | Path,
    family: str = "TECEdrainline",
    out: str | Path | None = None,
) -> dict[str, Any]:
    errors: list[str] = []
    warnings: list[str] = []
    exported, e = _read_csv(reviewed_pairs_csv, OUTPUT_COLUMNS, "reviewed pairs CSV"); errors.extend(e)
    source, e = _read_csv(review_csv, CSV_COLUMNS, "review CSV"); errors.extend(e)
    reviewed_report, e = _load_json(reviewed_pairs_report, "reviewed pairs report"); errors.extend(e)
    validation, e = _load_json(validation_report, "validation report"); errors.extend(e)
    impact, e = _load_json(impact_report, "impact report"); errors.extend(e)

    for label, data in (("reviewed pairs report", reviewed_report), ("validation report", validation), ("impact report", impact)):
        data_family = _clean(data.get("family"))
        if data_family and data_family != family:
            errors.append(f"family mismatch: {label} family {data_family} != {family}")
    for label, rows in (("reviewed pairs CSV", exported), ("review CSV", source)):
        families = {_clean(r.get("family")) for r in rows if _clean(r.get("family"))}
        if families and families != {family}:
            errors.append(f"family mismatch: {label} families {sorted(families)} != {family}")

    for k, expected in EXPECTED_REPORT_VALUES.items():
        if reviewed_report and reviewed_report.get(k) != expected:
            errors.append(f"reviewed pairs report mismatch: {k}={reviewed_report.get(k)!r} expected {expected!r}")
    if len(exported) != EXPECTED_REVIEWED_PAIRS_ROW_COUNT:
        errors.append(f"exported CSV row count is not {EXPECTED_REVIEWED_PAIRS_ROW_COUNT}: {len(exported)}")

    source_by_key = {_key(r): r for r in source}
    compatible_safe_keys = {_key(r) for r in source if _clean(r.get("reviewer_pair_decision")).lower() == "compatible" and _is_true(r.get("safe_to_apply_automatically"))}
    missing_from_review: list[dict[str, Any]] = []
    unexpected_exported: list[dict[str, Any]] = []
    invalid_ids: list[dict[str, Any]] = []
    invalid_decisions: list[dict[str, Any]] = []
    invalid_safe: list[dict[str, Any]] = []
    invalid_diagnostic: list[dict[str, Any]] = []
    production_leakage: list[dict[str, Any]] = []
    readiness_leakage: list[dict[str, Any]] = []

    key_counts = Counter(_key(r) for r in exported)
    duplicate_keys = [{"drain_body_article": k[0], "candidate_article": k[1], "nominal_length_mm": k[2]} for k, c in sorted(key_counts.items()) if c > 1]
    blocked_present = sorted({a for r in exported for a in (_clean(r.get("drain_body_article")), _clean(r.get("candidate_article"))) if a in BLOCKED_UNKNOWN_ARTICLES})

    for idx, row in enumerate(exported, start=1):
        expected_id = f"TECE-RP-{idx:04d}"
        key = _key(row)
        if _clean(row.get("reviewed_pair_id")) != expected_id:
            invalid_ids.append(_sample(idx + 1, row, f"expected {expected_id}"))
        if _clean(row.get("reviewer_pair_decision")).lower() != "compatible":
            invalid_decisions.append(_sample(idx + 1, row, "exported decision must be compatible"))
        if not _is_true(row.get("safe_to_apply_automatically")):
            invalid_safe.append(_sample(idx + 1, row, "safe_to_apply_automatically must be true"))
        if _clean(row.get("diagnostic_only")).lower() != "true":
            invalid_diagnostic.append(_sample(idx + 1, row, "diagnostic_only must be true"))
        if _is_true(row.get("production_safe")) or _clean(row.get("production_promotion_blocked")).lower() != "true":
            production_leakage.append(_sample(idx + 1, row, "production flags leaked or unblocked"))
        if _is_true(row.get("ready_for_benchmark")) or _is_true(row.get("ready_for_customer_view")):
            readiness_leakage.append(_sample(idx + 1, row, "readiness flag leaked"))
        source_row = source_by_key.get(key)
        if source_row is None or key not in compatible_safe_keys:
            missing_from_review.append(_sample(idx + 1, row, "not present in source review as compatible + safe"))
            decision = _clean(source_row.get("reviewer_pair_decision")).lower() if source_row else "(missing)"
            if decision in BLOCKING_DECISIONS or decision == "(missing)":
                unexpected_exported.append(_sample(idx + 1, row, f"source decision {decision}"))

    length_dist = dict(sorted(Counter(_clean(r.get("nominal_length_mm")) for r in exported).items()))
    body_dist = dict(sorted(Counter(_clean(r.get("drain_body_article")) for r in exported).items()))
    cand_dist = dict(sorted(Counter(_clean(r.get("candidate_article")) for r in exported).items()))
    if length_dist != EXPECTED_LENGTH_DISTRIBUTION:
        errors.append(f"length_distribution mismatch: {length_dist!r} expected {EXPECTED_LENGTH_DISTRIBUTION!r}")
    if body_dist != EXPECTED_DRAIN_BODY_DISTRIBUTION:
        errors.append(f"drain_body_distribution mismatch: {body_dist!r} expected {EXPECTED_DRAIN_BODY_DISTRIBUTION!r}")
    if cand_dist != EXPECTED_CANDIDATE_DISTRIBUTION:
        errors.append(f"candidate_distribution mismatch: {cand_dist!r} expected {EXPECTED_CANDIDATE_DISTRIBUTION!r}")

    for name, items in (("exported pairs missing from review", missing_from_review), ("unexpected exported pairs", unexpected_exported), ("duplicate pair keys", duplicate_keys), ("blocked articles present", blocked_present), ("invalid reviewed_pair_id sequence", invalid_ids), ("invalid decision rows", invalid_decisions), ("invalid safe flag rows", invalid_safe), ("invalid diagnostic flag rows", invalid_diagnostic), ("production leakage rows", production_leakage), ("readiness leakage rows", readiness_leakage)):
        if items:
            errors.append(f"{name}: {len(items)}")

    report = {
        "valid": not errors, "errors": errors, "warnings": warnings,
        "reviewed_pairs_csv_path": str(Path(reviewed_pairs_csv)), "reviewed_pairs_report_path": str(Path(reviewed_pairs_report)),
        "review_csv_path": str(Path(review_csv)), "validation_report_path": str(Path(validation_report)), "impact_report_path": str(Path(impact_report)),
        "family": family, "reviewed_pairs_row_count": len(exported), "expected_reviewed_pairs_row_count": EXPECTED_REVIEWED_PAIRS_ROW_COUNT,
        "compatible_safe_source_rows_count": len(compatible_safe_keys), "exported_pairs_missing_from_review": missing_from_review,
        "unexpected_exported_pairs": unexpected_exported, "duplicate_export_pair_keys": duplicate_keys,
        "blocked_articles_present": blocked_present, "invalid_reviewed_pair_ids": invalid_ids,
        "invalid_decision_rows": invalid_decisions, "invalid_safe_flag_rows": invalid_safe,
        "invalid_diagnostic_flag_rows": invalid_diagnostic, "production_leakage_rows": production_leakage,
        "readiness_leakage_rows": readiness_leakage, "length_distribution": length_dist,
        "drain_body_distribution": body_dist, "candidate_distribution": cand_dist,
        "reviewed_pairs_sample": exported[:10], "production_safe_candidate_count": 0,
        "production_promotion_blocked": True, "ready_for_benchmark": False, "ready_for_customer_view": False,
        "diagnostic_only_note": DIAGNOSTIC_ONLY_NOTE,
    }
    if out:
        Path(out).write_text(json.dumps(report, indent=2, sort_keys=True), encoding="utf-8")
    return report


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="QA report for diagnostic-only exported TECE reviewed compatibility pairs.")
    parser.add_argument("--reviewed-pairs-csv", required=True)
    parser.add_argument("--reviewed-pairs-report", required=True)
    parser.add_argument("--review-csv", required=True)
    parser.add_argument("--validation-report", required=True)
    parser.add_argument("--impact-report", required=True)
    parser.add_argument("--family", default="TECEdrainline")
    parser.add_argument("--json", action="store_true")
    parser.add_argument("--out", required=True)
    args = parser.parse_args(argv)
    report = report_reviewed_compatibility_pairs_qa(args.reviewed_pairs_csv, args.reviewed_pairs_report, args.review_csv, args.validation_report, args.impact_report, args.family, args.out)
    if args.json:
        print(json.dumps(report, indent=2, sort_keys=True))
    else:
        print(f"valid: {report['valid']}")
    return 0 if report["valid"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
