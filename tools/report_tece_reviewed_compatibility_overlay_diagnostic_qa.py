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

from tools.export_tece_high_priority_pair_shortlist import BLOCKED_UNKNOWN_ARTICLES
from tools.export_tece_reviewed_compatibility_overlay_diagnostic import OUTPUT_COLUMNS as OVERLAY_COLUMNS
from tools.export_tece_reviewed_compatibility_pairs import OUTPUT_COLUMNS as REVIEWED_PAIR_COLUMNS
from tools.report_tece_shortlist_review_impact import _is_true
from tools.report_tece_unknown_role_contexts import _clean

EXPECTED_ROW_COUNT = 34
EXPECTED_LENGTH_DISTRIBUTION = {"700": 34}
EXPECTED_DRAIN_BODY_DISTRIBUTION = {
    "600700": 8, "600701": 8, "600702": 8, "600703": 8, "600705": 1, "600707": 1,
}
EXPECTED_CANDIDATE_DISTRIBUTION = {
    "600710": 4, "600711": 4, "600751": 4, "600770": 4,
    "600772": 4, "600782": 4, "600783": 4, "600785": 6,
}
REQUIRED_FORBIDDEN_DOWNSTREAM_USE = (
    "Products", "Comparison", "BOM_Options", "Final_Assemblies", "Final_Set_Details", "customer_view", "production_generation",
)
DIAGNOSTIC_ONLY_NOTE = (
    "This report is read-only and diagnostic-only. It validates the TECEdrainline reviewed compatibility diagnostic "
    "overlay and does not mutate source-pack files, TECE logic, ACO export, Products, Comparison, BOM_Options, "
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
            return list(reader), ([f"{label} missing_required_columns: {', '.join(missing)}"] if missing else [])
    except Exception as exc:
        return [], [f"{label} is missing or could not be read: {exc}"]


def _row_key(row: dict[str, Any]) -> tuple[str, str, str, str, str]:
    return (
        _clean(row.get("reviewed_pair_id")), _clean(row.get("source_shortlist_id")),
        _clean(row.get("drain_body_article")), _clean(row.get("candidate_article")), _clean(row.get("nominal_length_mm")),
    )


def _pair_key(row: dict[str, Any]) -> tuple[str, str, str]:
    return (_clean(row.get("drain_body_article")), _clean(row.get("candidate_article")), _clean(row.get("nominal_length_mm")))


def _sample(row_number: int, row: dict[str, Any], reason: str | None = None) -> dict[str, Any]:
    item = {
        "row_number": row_number, "overlay_pair_id": _clean(row.get("overlay_pair_id")),
        "reviewed_pair_id": _clean(row.get("reviewed_pair_id")), "source_shortlist_id": _clean(row.get("source_shortlist_id")),
        "drain_body_article": _clean(row.get("drain_body_article")), "candidate_article": _clean(row.get("candidate_article")),
        "nominal_length_mm": _clean(row.get("nominal_length_mm")),
    }
    if reason:
        item["reason"] = reason
    return item


def _duplicates(values: list[Any]) -> list[Any]:
    counts = Counter(values)
    return sorted(v for v, c in counts.items() if c > 1)


def _distribution(rows: list[dict[str, Any]], column: str) -> dict[str, int]:
    return dict(sorted(Counter(_clean(r.get(column)) for r in rows).items()))


def _family_errors(family: str, labeled_reports: list[tuple[str, dict[str, Any]]], labeled_rows: list[tuple[str, list[dict[str, Any]]]]) -> list[str]:
    errors: list[str] = []
    for label, data in labeled_reports:
        data_family = _clean(data.get("family"))
        if data_family and data_family != family:
            errors.append(f"family mismatch: {label} family {data_family} != {family}")
    for label, rows in labeled_rows:
        families = {_clean(r.get("family")) for r in rows if _clean(r.get("family"))}
        if families and families != {family}:
            errors.append(f"family mismatch: {label} families {sorted(families)} != {family}")
    return errors


def report_overlay_diagnostic_qa(
    overlay_csv: str | Path,
    overlay_report: str | Path,
    reviewed_pairs_csv: str | Path,
    reviewed_pairs_report: str | Path,
    reviewed_pairs_qa_report: str | Path,
    family: str = "TECEdrainline",
    out: str | Path | None = None,
) -> dict[str, Any]:
    errors: list[str] = []
    warnings: list[str] = []
    overlay_rows, e = _read_csv(overlay_csv, OVERLAY_COLUMNS, "overlay CSV"); errors.extend(e)
    reviewed_rows, e = _read_csv(reviewed_pairs_csv, REVIEWED_PAIR_COLUMNS, "reviewed pairs CSV"); errors.extend(e)
    overlay_json, e = _load_json(overlay_report, "overlay report"); errors.extend(e)
    reviewed_json, e = _load_json(reviewed_pairs_report, "reviewed pairs report"); errors.extend(e)
    qa_json, e = _load_json(reviewed_pairs_qa_report, "reviewed pairs QA report"); errors.extend(e)

    errors.extend(_family_errors(family, [("overlay report", overlay_json), ("reviewed pairs report", reviewed_json), ("reviewed pairs QA report", qa_json)], [("overlay CSV", overlay_rows), ("reviewed pairs CSV", reviewed_rows)]))

    expected_overlay_row_count = overlay_json.get("expected_overlay_row_count", EXPECTED_ROW_COUNT)
    source_reviewed_pair_count = overlay_json.get("source_reviewed_pair_count", reviewed_json.get("exported_pair_count", len(reviewed_rows)))
    count_checks = [
        ("overlay CSV row count", len(overlay_rows)), ("overlay_report.overlay_row_count", overlay_json.get("overlay_row_count")),
        ("overlay_report.expected_overlay_row_count", expected_overlay_row_count), ("overlay_report.source_reviewed_pair_count", source_reviewed_pair_count),
        ("reviewed pairs CSV row count", len(reviewed_rows)), ("reviewed_pairs_report.exported_pair_count", reviewed_json.get("exported_pair_count")),
        ("reviewed_pairs_report.reviewed_pairs_row_count", reviewed_json.get("reviewed_pairs_row_count", EXPECTED_ROW_COUNT)),
        ("reviewed_pairs_qa_report.reviewed_pairs_row_count", qa_json.get("reviewed_pairs_row_count")),
        ("reviewed_pairs_qa_report.compatible_safe_source_rows_count", qa_json.get("compatible_safe_source_rows_count")),
    ]
    for label, actual in count_checks:
        if actual != EXPECTED_ROW_COUNT:
            errors.append(f"{label} is not {EXPECTED_ROW_COUNT}: {actual!r}")

    overlay_by_key = {_row_key(r): r for r in overlay_rows}
    reviewed_by_key = {_row_key(r): r for r in reviewed_rows}
    overlay_rows_missing_reviewed_pair = [_sample(i + 2, r, "not present in reviewed pairs CSV") for i, r in enumerate(overlay_rows) if _row_key(r) not in reviewed_by_key]
    reviewed_pairs_missing_overlay_row = [_sample(i + 2, r, "not present in overlay CSV") for i, r in enumerate(reviewed_rows) if _row_key(r) not in overlay_by_key]
    overlay_reviewed_pair_mismatch_rows = []
    for i, row in enumerate(overlay_rows):
        reviewed = next((r for r in reviewed_rows if _clean(r.get("reviewed_pair_id")) == _clean(row.get("reviewed_pair_id"))), None)
        if reviewed is not None and _row_key(row) != _row_key(reviewed):
            overlay_reviewed_pair_mismatch_rows.append(_sample(i + 2, row, "reviewed_pair_id row fields differ from reviewed pairs CSV"))

    duplicate_overlay_pair_ids = _duplicates([_clean(r.get("overlay_pair_id")) for r in overlay_rows])
    duplicate_reviewed_pair_ids = _duplicates([_clean(r.get("reviewed_pair_id")) for r in overlay_rows]) + [x for x in _duplicates([_clean(r.get("reviewed_pair_id")) for r in reviewed_rows]) if x not in _duplicates([_clean(r.get("reviewed_pair_id")) for r in overlay_rows])]
    key_counts = Counter(_pair_key(r) for r in overlay_rows)
    duplicate_pair_keys = [{"drain_body_article": k[0], "candidate_article": k[1], "nominal_length_mm": k[2]} for k, c in sorted(key_counts.items()) if c > 1]
    blocked_articles_present = sorted({a for r in overlay_rows for a in (_clean(r.get("drain_body_article")), _clean(r.get("candidate_article"))) if a in BLOCKED_UNKNOWN_ARTICLES})

    invalid_overlay_pair_ids=[]; invalid_reviewed_pair_ids=[]; invalid_decision_rows=[]; invalid_safe_flag_rows=[]; invalid_diagnostic_flag_rows=[]; invalid_overlay_status_rows=[]; production_leakage_rows=[]; readiness_leakage_rows=[]; forbidden_downstream_use_missing_rows=[]
    for idx, row in enumerate(overlay_rows, 1):
        if _clean(row.get("overlay_pair_id")) != f"TECE-DO-{idx:04d}": invalid_overlay_pair_ids.append(_sample(idx + 1, row, f"expected TECE-DO-{idx:04d}"))
        if _clean(row.get("reviewed_pair_id")) != f"TECE-RP-{idx:04d}": invalid_reviewed_pair_ids.append(_sample(idx + 1, row, f"expected TECE-RP-{idx:04d}"))
        if _clean(row.get("reviewer_pair_decision")).lower() != "compatible": invalid_decision_rows.append(_sample(idx + 1, row, "decision must be compatible"))
        if not _is_true(row.get("safe_to_apply_automatically")): invalid_safe_flag_rows.append(_sample(idx + 1, row, "safe_to_apply_automatically must be true"))
        if _clean(row.get("diagnostic_only")).lower() != "true": invalid_diagnostic_flag_rows.append(_sample(idx + 1, row, "diagnostic_only must be true"))
        overlay_status_bad = any([
            _clean(row.get("compatibility_evidence_type")) != "manual_reviewed_diagnostic_pair",
            _clean(row.get("compatibility_review_status")) != "compatible_reviewed_diagnostic_only",
            _clean(row.get("diagnostic_overlay_status")) != "active_diagnostic_overlay_pair",
            _clean(row.get("allowed_downstream_use")) != "diagnostic_analysis_only",
        ])
        if overlay_status_bad: invalid_overlay_status_rows.append(_sample(idx + 1, row, "diagnostic overlay status/evidence/use fields are invalid"))
        if _is_true(row.get("production_safe")) or _clean(row.get("production_promotion_blocked")).lower() != "true": production_leakage_rows.append(_sample(idx + 1, row, "production flags leaked or promotion unblocked"))
        if _is_true(row.get("ready_for_benchmark")) or _is_true(row.get("ready_for_customer_view")): readiness_leakage_rows.append(_sample(idx + 1, row, "readiness flag leaked"))
        forbidden = _clean(row.get("forbidden_downstream_use"))
        if any(token not in forbidden for token in REQUIRED_FORBIDDEN_DOWNSTREAM_USE): forbidden_downstream_use_missing_rows.append(_sample(idx + 1, row, "forbidden_downstream_use missing required blockers"))

    length_distribution = _distribution(overlay_rows, "nominal_length_mm")
    drain_body_distribution = _distribution(overlay_rows, "drain_body_article")
    candidate_distribution = _distribution(overlay_rows, "candidate_article")
    for name, actual, expected in (("length_distribution", length_distribution, EXPECTED_LENGTH_DISTRIBUTION), ("drain_body_distribution", drain_body_distribution, EXPECTED_DRAIN_BODY_DISTRIBUTION), ("candidate_distribution", candidate_distribution, EXPECTED_CANDIDATE_DISTRIBUTION)):
        if actual != expected: errors.append(f"{name} mismatch: {actual!r} expected {expected!r}")
    for name, items in (("overlay rows missing reviewed pair", overlay_rows_missing_reviewed_pair), ("reviewed pairs missing overlay row", reviewed_pairs_missing_overlay_row), ("overlay/reviewed pair mismatch rows", overlay_reviewed_pair_mismatch_rows), ("duplicate overlay pair IDs", duplicate_overlay_pair_ids), ("duplicate reviewed pair IDs", duplicate_reviewed_pair_ids), ("duplicate pair keys", duplicate_pair_keys), ("blocked articles present", blocked_articles_present), ("invalid overlay_pair_id sequence", invalid_overlay_pair_ids), ("invalid reviewed_pair_id sequence", invalid_reviewed_pair_ids), ("invalid decision rows", invalid_decision_rows), ("invalid safe flag rows", invalid_safe_flag_rows), ("invalid diagnostic flag rows", invalid_diagnostic_flag_rows), ("invalid overlay status rows", invalid_overlay_status_rows), ("production leakage rows", production_leakage_rows), ("readiness leakage rows", readiness_leakage_rows), ("forbidden downstream use missing rows", forbidden_downstream_use_missing_rows)):
        if items: errors.append(f"{name}: {len(items)}")

    report = {"valid": not errors, "errors": errors, "warnings": warnings, "overlay_csv_path": str(Path(overlay_csv)), "overlay_report_path": str(Path(overlay_report)), "reviewed_pairs_csv_path": str(Path(reviewed_pairs_csv)), "reviewed_pairs_report_path": str(Path(reviewed_pairs_report)), "reviewed_pairs_qa_report_path": str(Path(reviewed_pairs_qa_report)), "family": family, "overlay_row_count": len(overlay_rows), "expected_overlay_row_count": expected_overlay_row_count, "reviewed_pairs_row_count": len(reviewed_rows), "source_reviewed_pair_count": source_reviewed_pair_count, "overlay_rows_missing_reviewed_pair": overlay_rows_missing_reviewed_pair, "reviewed_pairs_missing_overlay_row": reviewed_pairs_missing_overlay_row, "overlay_reviewed_pair_mismatch_rows": overlay_reviewed_pair_mismatch_rows, "duplicate_overlay_pair_ids": duplicate_overlay_pair_ids, "duplicate_reviewed_pair_ids": duplicate_reviewed_pair_ids, "duplicate_pair_keys": duplicate_pair_keys, "blocked_articles_present": blocked_articles_present, "invalid_overlay_pair_ids": invalid_overlay_pair_ids, "invalid_reviewed_pair_ids": invalid_reviewed_pair_ids, "invalid_decision_rows": invalid_decision_rows, "invalid_safe_flag_rows": invalid_safe_flag_rows, "invalid_diagnostic_flag_rows": invalid_diagnostic_flag_rows, "invalid_overlay_status_rows": invalid_overlay_status_rows, "production_leakage_rows": production_leakage_rows, "readiness_leakage_rows": readiness_leakage_rows, "forbidden_downstream_use_missing_rows": forbidden_downstream_use_missing_rows, "length_distribution": length_distribution, "drain_body_distribution": drain_body_distribution, "candidate_distribution": candidate_distribution, "overlay_pairs_sample": overlay_rows[:10], "production_safe_candidate_count": 0, "production_promotion_blocked": True, "ready_for_benchmark": False, "ready_for_customer_view": False, "diagnostic_only_note": DIAGNOSTIC_ONLY_NOTE}
    if out:
        Path(out).write_text(json.dumps(report, indent=2, sort_keys=True), encoding="utf-8")
    return report


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Read-only QA report for TECE reviewed compatibility diagnostic overlay.")
    parser.add_argument("--overlay-csv", required=True)
    parser.add_argument("--overlay-report", required=True)
    parser.add_argument("--reviewed-pairs-csv", required=True)
    parser.add_argument("--reviewed-pairs-report", required=True)
    parser.add_argument("--reviewed-pairs-qa-report", required=True)
    parser.add_argument("--family", default="TECEdrainline")
    parser.add_argument("--json", action="store_true")
    parser.add_argument("--out", required=True)
    args = parser.parse_args(argv)
    report = report_overlay_diagnostic_qa(args.overlay_csv, args.overlay_report, args.reviewed_pairs_csv, args.reviewed_pairs_report, args.reviewed_pairs_qa_report, args.family, args.out)
    if args.json:
        print(json.dumps(report, indent=2, sort_keys=True))
    else:
        print(f"valid: {report['valid']}")
    return 0 if report["valid"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
