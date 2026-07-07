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
from tools.export_tece_reviewed_compatibility_pairs import OUTPUT_COLUMNS as REVIEWED_PAIR_COLUMNS
from tools.report_tece_shortlist_review_impact import _is_true
from tools.report_tece_unknown_role_contexts import _clean

EXPECTED_OVERLAY_ROW_COUNT = 34
EXPECTED_LENGTH_DISTRIBUTION = {"700": 34}
EXPECTED_DRAIN_BODY_DISTRIBUTION = {
    "600700": 8, "600701": 8, "600702": 8, "600703": 8, "600705": 1, "600707": 1,
}
EXPECTED_CANDIDATE_DISTRIBUTION = {
    "600710": 4, "600711": 4, "600751": 4, "600770": 4,
    "600772": 4, "600782": 4, "600783": 4, "600785": 6,
}
BLOCKING_DECISIONS = {"ambiguous", "incompatible", "keep_for_later", "not_a_pair", ""}
FORBIDDEN_DOWNSTREAM_USE = (
    "Products; Comparison; BOM_Options; Final_Assemblies; Final_Set_Details; "
    "customer_view; production_generation"
)
PRODUCTION_STATUS_NOTE = (
    "diagnostic-only reviewed compatibility overlay pair; not production-safe; "
    "no Products/BOM/assembly/customer-view promotion"
)
DIAGNOSTIC_ONLY_NOTE = (
    "This export is diagnostic-only. It creates a reviewed TECEdrainline compatibility overlay artifact for analysis only "
    "and does not mutate source-pack files, TECE logic, ACO export, Products, Comparison, BOM_Options, "
    "Final_Assemblies, or Final_Set_Details. Production promotion remains blocked."
)
OUTPUT_COLUMNS = [
    "overlay_pair_id", "reviewed_pair_id", "source_shortlist_id", "family", "pair_type", "nominal_length_mm",
    "drain_body_article", "candidate_article", "candidate_role", "drain_body_source_file", "candidate_source_file",
    "drain_body_evidence_text_snippet", "candidate_evidence_text_snippet", "reviewer_pair_decision",
    "reviewer_notes", "safe_to_apply_automatically", "compatibility_evidence_type", "compatibility_review_status",
    "diagnostic_overlay_status", "diagnostic_only", "production_safe", "production_promotion_blocked",
    "ready_for_benchmark", "ready_for_customer_view", "allowed_downstream_use", "forbidden_downstream_use",
    "production_status_note",
]


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


def _read_csv(path: str | Path) -> tuple[list[dict[str, Any]], list[str]]:
    try:
        with Path(path).open(encoding="utf-8-sig", newline="") as fh:
            reader = csv.DictReader(fh)
            fields = reader.fieldnames or []
            missing = [c for c in REVIEWED_PAIR_COLUMNS if c not in fields]
            return list(reader), ([f"reviewed pairs CSV missing_required_columns: {', '.join(missing)}"] if missing else [])
    except Exception as exc:
        return [], [f"reviewed pairs CSV is missing or could not be read: {exc}"]


def _pair_key(row: dict[str, Any]) -> tuple[str, str, str]:
    return (_clean(row.get("drain_body_article")), _clean(row.get("candidate_article")), _clean(row.get("nominal_length_mm")))


def _sample(idx: int, row: dict[str, Any], reason: str | None = None) -> dict[str, Any]:
    item: dict[str, Any] = {
        "row_number": idx,
        "overlay_pair_id": _clean(row.get("overlay_pair_id")),
        "reviewed_pair_id": _clean(row.get("reviewed_pair_id")),
        "drain_body_article": _clean(row.get("drain_body_article")),
        "candidate_article": _clean(row.get("candidate_article")),
        "nominal_length_mm": _clean(row.get("nominal_length_mm")),
    }
    if reason:
        item["reason"] = reason
    return item


def _overlay_row(idx: int, row: dict[str, Any]) -> dict[str, str]:
    return {
        "overlay_pair_id": f"TECE-DO-{idx:04d}",
        "reviewed_pair_id": _clean(row.get("reviewed_pair_id")),
        "source_shortlist_id": _clean(row.get("source_shortlist_id")),
        "family": _clean(row.get("family")),
        "pair_type": _clean(row.get("pair_type")),
        "nominal_length_mm": _clean(row.get("nominal_length_mm")),
        "drain_body_article": _clean(row.get("drain_body_article")),
        "candidate_article": _clean(row.get("candidate_article")),
        "candidate_role": _clean(row.get("candidate_role")),
        "drain_body_source_file": _clean(row.get("drain_body_source_file")),
        "candidate_source_file": _clean(row.get("candidate_source_file")),
        "drain_body_evidence_text_snippet": _clean(row.get("drain_body_evidence_text_snippet")),
        "candidate_evidence_text_snippet": _clean(row.get("candidate_evidence_text_snippet")),
        "reviewer_pair_decision": "compatible",
        "reviewer_notes": _clean(row.get("reviewer_notes")),
        "safe_to_apply_automatically": "true",
        "compatibility_evidence_type": "manual_reviewed_diagnostic_pair",
        "compatibility_review_status": "compatible_reviewed_diagnostic_only",
        "diagnostic_overlay_status": "active_diagnostic_overlay_pair",
        "diagnostic_only": "true",
        "production_safe": "false",
        "production_promotion_blocked": "true",
        "ready_for_benchmark": "false",
        "ready_for_customer_view": "false",
        "allowed_downstream_use": "diagnostic_analysis_only",
        "forbidden_downstream_use": FORBIDDEN_DOWNSTREAM_USE,
        "production_status_note": PRODUCTION_STATUS_NOTE,
    }


def _duplicates(values: list[Any]) -> list[Any]:
    counts = Counter(values)
    return sorted(v for v, c in counts.items() if c > 1)


def _forbidden_missing(row: dict[str, Any]) -> bool:
    text = _clean(row.get("forbidden_downstream_use"))
    required = ["Products", "Comparison", "BOM_Options", "Final_Assemblies", "Final_Set_Details", "customer_view", "production_generation"]
    return any(token not in text for token in required)


def _validate_overlay_rows(rows: list[dict[str, Any]]) -> dict[str, Any]:
    duplicate_overlay_ids = _duplicates([_clean(r.get("overlay_pair_id")) for r in rows])
    duplicate_reviewed_ids = _duplicates([_clean(r.get("reviewed_pair_id")) for r in rows])
    key_counts = Counter(_pair_key(r) for r in rows)
    duplicate_keys = [{"drain_body_article": k[0], "candidate_article": k[1], "nominal_length_mm": k[2]} for k, c in sorted(key_counts.items()) if c > 1]
    blocked = sorted({a for r in rows for a in (_clean(r.get("drain_body_article")), _clean(r.get("candidate_article"))) if a in BLOCKED_UNKNOWN_ARTICLES})
    invalid_overlay_ids = []
    invalid_reviewed_ids = []
    invalid_decisions = []
    invalid_safe = []
    invalid_diag = []
    production_leakage = []
    readiness_leakage = []
    forbidden_missing = []
    for idx, row in enumerate(rows, 1):
        if _clean(row.get("overlay_pair_id")) != f"TECE-DO-{idx:04d}":
            invalid_overlay_ids.append(_sample(idx + 1, row, f"expected TECE-DO-{idx:04d}"))
        if _clean(row.get("reviewed_pair_id")) != f"TECE-RP-{idx:04d}":
            invalid_reviewed_ids.append(_sample(idx + 1, row, f"expected TECE-RP-{idx:04d}"))
        decision = _clean(row.get("reviewer_pair_decision")).lower()
        if decision != "compatible" or decision in BLOCKING_DECISIONS:
            invalid_decisions.append(_sample(idx + 1, row, "decision must be compatible"))
        if not _is_true(row.get("safe_to_apply_automatically")):
            invalid_safe.append(_sample(idx + 1, row, "safe_to_apply_automatically must be true"))
        if _clean(row.get("diagnostic_only")).lower() != "true":
            invalid_diag.append(_sample(idx + 1, row, "diagnostic_only must be true"))
        if _is_true(row.get("production_safe")) or _clean(row.get("production_promotion_blocked")).lower() != "true":
            production_leakage.append(_sample(idx + 1, row, "production-safe leakage or promotion unblocked"))
        if _is_true(row.get("ready_for_benchmark")) or _is_true(row.get("ready_for_customer_view")):
            readiness_leakage.append(_sample(idx + 1, row, "readiness leakage"))
        if _forbidden_missing(row):
            forbidden_missing.append(_sample(idx + 1, row, "forbidden_downstream_use missing required blockers"))
    return {
        "duplicate_overlay_pair_ids": duplicate_overlay_ids,
        "duplicate_reviewed_pair_ids": duplicate_reviewed_ids,
        "duplicate_pair_keys": duplicate_keys,
        "blocked_articles_present": blocked,
        "invalid_overlay_pair_ids": invalid_overlay_ids,
        "invalid_reviewed_pair_ids": invalid_reviewed_ids,
        "invalid_decision_rows": invalid_decisions,
        "invalid_safe_flag_rows": invalid_safe,
        "invalid_diagnostic_flag_rows": invalid_diag,
        "production_leakage_rows": production_leakage,
        "readiness_leakage_rows": readiness_leakage,
        "forbidden_downstream_use_missing_rows": forbidden_missing,
    }


def export_overlay_diagnostic(reviewed_pairs_csv: str | Path, reviewed_pairs_report: str | Path, qa_report: str | Path, family: str = "TECEdrainline", out: str | Path | None = None, json_out: str | Path | None = None) -> dict[str, Any]:
    errors: list[str] = []
    warnings: list[str] = []
    source_rows, e = _read_csv(reviewed_pairs_csv); errors.extend(e)
    pairs_report, e = _load_json(reviewed_pairs_report, "reviewed pairs report"); errors.extend(e)
    qa, e = _load_json(qa_report, "QA report"); errors.extend(e)

    for label, data in (("reviewed pairs report", pairs_report), ("QA report", qa)):
        data_family = _clean(data.get("family"))
        if data_family and data_family != family:
            errors.append(f"family mismatch: {label} family {data_family} != {family}")
    families = {_clean(r.get("family")) for r in source_rows if _clean(r.get("family"))}
    if families and families != {family}:
        errors.append(f"family mismatch: reviewed pairs CSV families {sorted(families)} != {family}")

    count_candidates = [pairs_report.get("reviewed_pairs_row_count"), pairs_report.get("exported_pair_count"), qa.get("reviewed_pairs_row_count"), qa.get("exported_pair_count")]
    expected_candidates = [pairs_report.get("expected_reviewed_pairs_row_count"), pairs_report.get("expected_exported_pair_count"), qa.get("expected_reviewed_pairs_row_count"), qa.get("expected_exported_pair_count")]
    for label, vals in (("reviewed pair count", count_candidates), ("expected reviewed pair count", expected_candidates)):
        present = [v for v in vals if v is not None]
        if not present or any(v != EXPECTED_OVERLAY_ROW_COUNT for v in present):
            errors.append(f"{label} strict pilot mismatch: {present!r} expected {EXPECTED_OVERLAY_ROW_COUNT}")
    strict_checks = {
        "compatible_safe_source_rows_count": EXPECTED_OVERLAY_ROW_COUNT,
        "production_safe_candidate_count": 0,
        "production_promotion_blocked": True,
        "ready_for_benchmark": False,
        "ready_for_customer_view": False,
    }
    for key, expected in strict_checks.items():
        for label, data in (("reviewed pairs report", pairs_report), ("QA report", qa)):
            if key in data and data.get(key) != expected:
                errors.append(f"{label} strict pilot mismatch: {key}={data.get(key)!r} expected {expected!r}")
        if key == "compatible_safe_source_rows_count" and qa and qa.get(key) != expected:
            errors.append(f"QA report strict pilot mismatch: {key}={qa.get(key)!r} expected {expected!r}")

    invalid_source_decisions = []
    invalid_source_safe = []
    source_production_leakage = []
    source_readiness_leakage = []
    for idx, row in enumerate(source_rows, 1):
        decision = _clean(row.get("reviewer_pair_decision")).lower()
        if decision != "compatible" or decision in BLOCKING_DECISIONS:
            invalid_source_decisions.append(_sample(idx + 1, row, "source decision must be compatible"))
        if not _is_true(row.get("safe_to_apply_automatically")):
            invalid_source_safe.append(_sample(idx + 1, row, "source safe_to_apply_automatically must be true"))
        if _is_true(row.get("production_safe")) or _clean(row.get("production_promotion_blocked")).lower() != "true":
            source_production_leakage.append(_sample(idx + 1, row, "source production-safe leakage or promotion unblocked"))
        if _is_true(row.get("ready_for_benchmark")) or _is_true(row.get("ready_for_customer_view")):
            source_readiness_leakage.append(_sample(idx + 1, row, "source readiness leakage"))

    overlay_rows = [_overlay_row(i, r) for i, r in enumerate(source_rows, 1)]
    validation = _validate_overlay_rows(overlay_rows)
    validation["invalid_decision_rows"].extend(invalid_source_decisions)
    validation["invalid_safe_flag_rows"].extend(invalid_source_safe)
    validation["production_leakage_rows"].extend(source_production_leakage)
    validation["readiness_leakage_rows"].extend(source_readiness_leakage)
    if len(source_rows) != EXPECTED_OVERLAY_ROW_COUNT:
        errors.append(f"reviewed pair count is not {EXPECTED_OVERLAY_ROW_COUNT}: {len(source_rows)}")
    if len(overlay_rows) != EXPECTED_OVERLAY_ROW_COUNT:
        errors.append(f"overlay row count is not {EXPECTED_OVERLAY_ROW_COUNT}: {len(overlay_rows)}")
    for name, items in validation.items():
        if items:
            errors.append(f"{name}: {len(items)}")

    length_dist = dict(sorted(Counter(_clean(r.get("nominal_length_mm")) for r in overlay_rows).items()))
    body_dist = dict(sorted(Counter(_clean(r.get("drain_body_article")) for r in overlay_rows).items()))
    cand_dist = dict(sorted(Counter(_clean(r.get("candidate_article")) for r in overlay_rows).items()))
    for name, actual, expected in (("length_distribution", length_dist, EXPECTED_LENGTH_DISTRIBUTION), ("drain_body_distribution", body_dist, EXPECTED_DRAIN_BODY_DISTRIBUTION), ("candidate_distribution", cand_dist, EXPECTED_CANDIDATE_DISTRIBUTION)):
        if actual != expected:
            errors.append(f"{name} mismatch: {actual!r} expected {expected!r}")

    report = {
        "valid": not errors, "errors": errors, "warnings": warnings,
        "reviewed_pairs_csv_path": str(Path(reviewed_pairs_csv)), "reviewed_pairs_report_path": str(Path(reviewed_pairs_report)),
        "qa_report_path": str(Path(qa_report)), "output_csv_path": str(Path(out)) if out else "", "family": family,
        "source_reviewed_pair_count": len(source_rows), "overlay_row_count": len(overlay_rows),
        "expected_overlay_row_count": EXPECTED_OVERLAY_ROW_COUNT, **validation,
        "length_distribution": length_dist, "drain_body_distribution": body_dist, "candidate_distribution": cand_dist,
        "overlay_pairs_sample": overlay_rows[:10], "production_safe_candidate_count": 0,
        "production_promotion_blocked": True, "ready_for_benchmark": False, "ready_for_customer_view": False,
        "diagnostic_only_note": DIAGNOSTIC_ONLY_NOTE,
    }
    if out and report["valid"]:
        with Path(out).open("w", encoding="utf-8", newline="") as fh:
            writer = csv.DictWriter(fh, fieldnames=OUTPUT_COLUMNS)
            writer.writeheader(); writer.writerows(overlay_rows)
    if json_out:
        Path(json_out).write_text(json.dumps(report, indent=2, sort_keys=True), encoding="utf-8")
    return report


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Export diagnostic-only TECEdrainline reviewed compatibility overlay.")
    parser.add_argument("--reviewed-pairs-csv", required=True)
    parser.add_argument("--reviewed-pairs-report", required=True)
    parser.add_argument("--qa-report", required=True)
    parser.add_argument("--family", default="TECEdrainline")
    parser.add_argument("--out", required=True)
    parser.add_argument("--json-out", required=True)
    args = parser.parse_args(argv)
    report = export_overlay_diagnostic(args.reviewed_pairs_csv, args.reviewed_pairs_report, args.qa_report, args.family, args.out, args.json_out)
    print(json.dumps(report, indent=2, sort_keys=True))
    return 0 if report["valid"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
