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
from tools.report_tece_shortlist_review_impact import _is_true
from tools.report_tece_unknown_role_contexts import _clean

EXPECTED_EXPORTED_PAIR_COUNT = 34
EXPECTED_IMPACT_SHAPE = {
    "total_review_rows": 80,
    "compatible_safe_pair_count": 34,
    "ambiguous_count": 22,
    "incompatible_count": 24,
    "production_safe_candidate_count": 0,
    "production_promotion_blocked": True,
    "ready_for_benchmark": False,
    "ready_for_customer_view": False,
}
PRODUCTION_STATUS_NOTE = (
    "diagnostic-only reviewed compatibility pair; not production-safe; no Products/BOM/assembly/customer-view promotion"
)
DIAGNOSTIC_ONLY_NOTE = (
    "Diagnostic-only export of manually reviewed TECEdrainline compatible pairs. This tool writes only the requested "
    "CSV/JSON artifacts and does not mutate source-pack files, TECE extraction/classification logic, production "
    "promotion flags, ACO canonical export, Products, Comparison, BOM_Options, Final_Assemblies, or Final_Set_Details."
)
OUTPUT_COLUMNS = [
    "reviewed_pair_id", "source_shortlist_id", "family", "pair_type", "nominal_length_mm", "drain_body_article",
    "candidate_article", "candidate_role", "drain_body_source_file", "candidate_source_file",
    "drain_body_evidence_text_snippet", "candidate_evidence_text_snippet", "reviewer_pair_decision",
    "reviewer_notes", "safe_to_apply_automatically", "diagnostic_only", "production_safe",
    "production_promotion_blocked", "ready_for_benchmark", "ready_for_customer_view", "production_status_note",
]
BLOCKING_DECISIONS = {"ambiguous", "incompatible", "keep_for_later", "not_a_pair", ""}


def _load_json(path: str | Path, label: str) -> tuple[dict[str, Any], list[str]]:
    try:
        data = json.loads(Path(path).read_text(encoding="utf-8"))
    except Exception as exc:
        return {}, [f"{label} is missing or could not be read as JSON: {exc}"]
    if not isinstance(data, dict):
        return {}, [f"{label} is not a JSON object"]
    if data.get("valid") is not True:
        return data, [f"{label} is invalid"] + [str(e) for e in data.get("errors", [])]
    return data, []


def _read_review_csv(path: str | Path) -> tuple[list[dict[str, Any]], list[str]]:
    try:
        with Path(path).open(encoding="utf-8-sig", newline="") as fh:
            reader = csv.DictReader(fh)
            fieldnames = reader.fieldnames or []
            missing = [c for c in CSV_COLUMNS if c not in fieldnames]
            return list(reader), ([f"missing_required_columns: {', '.join(missing)}"] if missing else [])
    except Exception as exc:
        return [], [f"review CSV could not be read: {exc}"]


def _family_values(rows: list[dict[str, Any]]) -> set[str]:
    return {_clean(row.get("family")) for row in rows if _clean(row.get("family"))}


def _pair_key(row: dict[str, Any]) -> str:
    return "|".join([_clean(row.get("drain_body_article")), _clean(row.get("candidate_article")), _clean(row.get("nominal_length_mm"))])


def _export_row(idx: int, row: dict[str, Any]) -> dict[str, str]:
    return {
        "reviewed_pair_id": f"TECE-RP-{idx:04d}",
        "source_shortlist_id": _clean(row.get("shortlist_id")),
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
        "diagnostic_only": "true",
        "production_safe": "false",
        "production_promotion_blocked": "true",
        "ready_for_benchmark": "false",
        "ready_for_customer_view": "false",
        "production_status_note": PRODUCTION_STATUS_NOTE,
    }


def export_reviewed_pairs(review_csv: str | Path, validation_report: str | Path, impact_report: str | Path, family: str = "TECEdrainline", out: str | Path | None = None, json_out: str | Path | None = None) -> dict[str, Any]:
    errors: list[str] = []
    warnings: list[str] = []
    rows, csv_errors = _read_review_csv(review_csv)
    errors.extend(csv_errors)
    validation, validation_errors = _load_json(validation_report, "validation report")
    impact, impact_errors = _load_json(impact_report, "impact report")
    errors.extend(validation_errors)
    errors.extend(impact_errors)

    for label, data in (("validation report", validation), ("impact report", impact)):
        data_family = _clean(data.get("family"))
        if data_family and data_family != family:
            errors.append(f"family mismatch: {label} family {data_family} != {family}")
    families = _family_values(rows)
    if families and families != {family}:
        errors.append(f"family mismatch: review CSV families {sorted(families)} != {family}")
    for key, expected in EXPECTED_IMPACT_SHAPE.items():
        if impact and impact.get(key) != expected:
            errors.append(f"impact report strict shape mismatch: {key}={impact.get(key)!r} expected {expected!r}")

    exported_source = [r for r in rows if _clean(r.get("reviewer_pair_decision")).lower() == "compatible" and _is_true(r.get("safe_to_apply_automatically"))]
    exported = [_export_row(i, r) for i, r in enumerate(exported_source, start=1)]
    key_counts = Counter(_pair_key(r) for r in exported)
    duplicate_keys = sorted(k for k, c in key_counts.items() if c > 1)
    blocked_present = sorted({a for r in exported for a in (_clean(r.get("drain_body_article")), _clean(r.get("candidate_article"))) if a in BLOCKED_UNKNOWN_ARTICLES})

    for idx, source_row in enumerate(exported_source, start=1):
        if _is_true(source_row.get("production_safe")) or _is_true(source_row.get("ready_for_benchmark")) or _is_true(source_row.get("ready_for_customer_view")):
            errors.append(f"exported source row {idx}: production/readiness leakage")
    for idx, row in enumerate(exported, start=1):
        if row["reviewer_pair_decision"] != "compatible" or row["safe_to_apply_automatically"] != "true":
            errors.append(f"exported row {idx}: not compatible + safe")
        if _is_true(row.get("production_safe")) or _is_true(row.get("ready_for_benchmark")) or _is_true(row.get("ready_for_customer_view")):
            errors.append(f"exported row {idx}: production/readiness leakage")
    if len(exported) != EXPECTED_EXPORTED_PAIR_COUNT:
        errors.append(f"exported pair count is not {EXPECTED_EXPORTED_PAIR_COUNT}: {len(exported)}")
    if duplicate_keys:
        errors.append(f"duplicate exported pair key exists: {', '.join(duplicate_keys)}")
    if blocked_present:
        errors.append(f"blocked articles present: {', '.join(blocked_present)}")

    decision_counts = Counter((_clean(r.get("reviewer_pair_decision")).lower() or "") for r in rows)
    not_safe_count = sum(1 for r in rows if not _is_true(r.get("safe_to_apply_automatically")))
    report = {
        "valid": not errors, "errors": errors, "warnings": warnings,
        "review_csv_path": str(Path(review_csv)), "validation_report_path": str(Path(validation_report)),
        "impact_report_path": str(Path(impact_report)), "output_csv_path": str(Path(out)) if out else "", "family": family,
        "input_review_rows": len(rows), "exported_pair_count": len(exported),
        "expected_exported_pair_count": EXPECTED_EXPORTED_PAIR_COUNT,
        "excluded_ambiguous_count": decision_counts.get("ambiguous", 0),
        "excluded_incompatible_count": decision_counts.get("incompatible", 0),
        "excluded_not_safe_count": not_safe_count,
        "duplicate_export_pair_keys": duplicate_keys, "blocked_articles_present": blocked_present,
        "length_distribution": dict(sorted(Counter(r["nominal_length_mm"] for r in exported).items())),
        "drain_body_distribution": dict(sorted(Counter(r["drain_body_article"] for r in exported).items())),
        "candidate_distribution": dict(sorted(Counter(r["candidate_article"] for r in exported).items())),
        "exported_pairs_sample": exported[:10], "production_safe_candidate_count": 0,
        "production_promotion_blocked": True, "ready_for_benchmark": False, "ready_for_customer_view": False,
        "diagnostic_only_note": DIAGNOSTIC_ONLY_NOTE,
    }
    if out and report["valid"]:
        with Path(out).open("w", encoding="utf-8", newline="") as fh:
            writer = csv.DictWriter(fh, fieldnames=OUTPUT_COLUMNS)
            writer.writeheader(); writer.writerows(exported)
    if json_out:
        Path(json_out).write_text(json.dumps(report, indent=2, sort_keys=True), encoding="utf-8")
    return report


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Export diagnostic-only reviewed TECE compatibility pairs.")
    parser.add_argument("--review-csv", required=True)
    parser.add_argument("--validation-report", required=True)
    parser.add_argument("--impact-report", required=True)
    parser.add_argument("--family", default="TECEdrainline")
    parser.add_argument("--out", required=True)
    parser.add_argument("--json-out", required=True)
    args = parser.parse_args(argv)
    report = export_reviewed_pairs(args.review_csv, args.validation_report, args.impact_report, args.family, args.out, args.json_out)
    print(json.dumps(report, indent=2, sort_keys=True))
    return 0 if report["valid"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
