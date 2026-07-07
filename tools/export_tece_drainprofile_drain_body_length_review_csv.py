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

from tools.report_tece_drainprofile_length_extraction_diagnostic import _article, _clean, _evidence_text, _family, _role, _source_file
from tools.report_tece_source_inventory import load_source_pack
from tools.tece_report_output import write_json_output, write_text_output

DEFAULT_SOURCE_PACK = Path("local_source_packs/tece/pilot_001")
EXPECTED_FAMILY = "TECEdrainprofile"
EXPECTED_ROLE = "drain_body"
EXPECTED_REVIEW_ROW_COUNT = 12
RECOMMENDED_NEXT_ACTION = "manually review TECEdrainprofile drain body nominal length before applying any length overlay or compatibility pairing"
PRODUCTION_STATUS_NOTE = "diagnostic-only TECEdrainprofile drain body length review export; no source-pack mutation; no Products/BOM/Final_Assemblies/Final_Set_Details/customer-view promotion"
DIAGNOSTIC_ONLY_NOTE = "This report is read-only and diagnostic-only. It exports TECEdrainprofile drain body rows for manual nominal length review and does not mutate source-pack files, TECE logic, ACO export, Products, Comparison, BOM_Options, Final_Assemblies, or Final_Set_Details. Production promotion remains blocked."

CSV_COLUMNS = [
    "review_id", "family", "article_number", "article_role", "source_file", "evidence_text_snippet",
    "current_extraction_status", "current_candidate_length_values_mm", "current_selected_diagnostic_length_mm",
    "reviewed_nominal_length_mm", "reviewer_decision", "reviewer_notes", "safe_to_apply_automatically",
    "diagnostic_only", "production_safe", "production_promotion_blocked", "ready_for_benchmark",
    "ready_for_customer_view", "recommended_next_action", "production_status_note",
]


def _snippet(text: str) -> str:
    return " ".join((text or "").split())[:500]


def _review_row(row: Any, idx: int, diagnostic_by_article: dict[str, dict[str, str]] | None = None) -> dict[str, str]:
    article = _article(row)
    diagnostic = (diagnostic_by_article or {}).get(article, {})
    return {
        "review_id": f"TECE-DP-DB-LEN-REVIEW-{idx:06d}",
        "family": EXPECTED_FAMILY,
        "article_number": article,
        "article_role": EXPECTED_ROLE,
        "source_file": _source_file(row),
        "evidence_text_snippet": _snippet(_evidence_text(row)),
        "current_extraction_status": _clean(diagnostic.get("extraction_status") or diagnostic.get("current_extraction_status")),
        "current_candidate_length_values_mm": _clean(diagnostic.get("candidate_length_values_mm") or diagnostic.get("current_candidate_length_values_mm")),
        "current_selected_diagnostic_length_mm": _clean(diagnostic.get("selected_diagnostic_length_mm") or diagnostic.get("current_selected_diagnostic_length_mm")),
        "reviewed_nominal_length_mm": "",
        "reviewer_decision": "",
        "reviewer_notes": "",
        "safe_to_apply_automatically": "",
        "diagnostic_only": "true",
        "production_safe": "false",
        "production_promotion_blocked": "true",
        "ready_for_benchmark": "false",
        "ready_for_customer_view": "false",
        "recommended_next_action": RECOMMENDED_NEXT_ACTION,
        "production_status_note": PRODUCTION_STATUS_NOTE,
    }


def _read_diagnostic_csv(path: str | Path | None) -> list[dict[str, str]]:
    if not path:
        return []
    with Path(path).open(encoding="utf-8-sig", newline="") as fh:
        return list(csv.DictReader(fh))


def _read_diagnostic_report(path: str | Path | None) -> dict[str, Any]:
    if not path:
        return {}
    with Path(path).open(encoding="utf-8") as fh:
        return json.load(fh)


def _diagnostic_by_article(csv_rows: list[dict[str, str]]) -> dict[str, dict[str, str]]:
    return {_clean(r.get("article_number")): r for r in csv_rows if _clean(r.get("article_number"))}


def _crosscheck(rows: list[dict[str, Any]], report_path: str | Path | None, csv_path: str | Path | None) -> tuple[str, list[str]]:
    errors: list[str] = []
    csv_rows = _read_diagnostic_csv(csv_path)
    diag_report = _read_diagnostic_report(report_path)
    expected = {
        _clean(r.get("article_number")) for r in csv_rows
        if _clean(r.get("family")) == EXPECTED_FAMILY and _clean(r.get("article_role")) == EXPECTED_ROLE and _clean(r.get("extraction_status")) == "no_length_candidate_found"
    }
    exported = {_clean(r.get("article_number")) for r in rows}
    if csv_path and exported != expected:
        errors.append(f"diagnostic CSV no_length_candidate_found drain_body article set mismatch: exported={sorted(exported)} expected={sorted(expected)}")
    if report_path:
        if diag_report.get("family") not in {None, EXPECTED_FAMILY}:
            errors.append("diagnostic report family is not TECEdrainprofile")
        status_counts = diag_report.get("extraction_status_counts") or {}
        if status_counts and status_counts.get("no_length_candidate_found") != EXPECTED_REVIEW_ROW_COUNT:
            errors.append("diagnostic report no_length_candidate_found count is not 12")
    return ("pass" if not errors else "fail"), errors


def validate_rows(rows: list[dict[str, Any]]) -> dict[str, list[Any]]:
    review_counts = Counter(_clean(r.get("review_id")) for r in rows)
    article_counts = Counter(_clean(r.get("article_number")) for r in rows)
    return {
        "duplicate_review_ids": [k for k, v in sorted(review_counts.items()) if k and v > 1],
        "duplicate_article_numbers": [k for k, v in sorted(article_counts.items()) if k and v > 1],
        "invalid_family_rows": [r for r in rows if _clean(r.get("family")) != EXPECTED_FAMILY],
        "invalid_role_rows": [r for r in rows if _clean(r.get("article_role")) != EXPECTED_ROLE],
        "prefilled_review_field_rows": [r for r in rows if _clean(r.get("reviewed_nominal_length_mm")) or _clean(r.get("reviewer_decision")) or _clean(r.get("reviewer_notes")) or _clean(r.get("safe_to_apply_automatically"))],
        "production_leakage_rows": [r for r in rows if _clean(r.get("production_safe")).lower() == "true"],
        "readiness_leakage_rows": [r for r in rows if _clean(r.get("ready_for_benchmark")).lower() == "true" or _clean(r.get("ready_for_customer_view")).lower() == "true"],
        "diagnostic_only_leakage_rows": [r for r in rows if _clean(r.get("diagnostic_only")).lower() != "true"],
    }


def build_report(source_pack: str | Path = DEFAULT_SOURCE_PACK, family: str = EXPECTED_FAMILY, length_diagnostic_report: str | Path | None = None, length_diagnostic_csv: str | Path | None = None, out: str | Path | None = None) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    errors: list[str] = []
    warnings: list[str] = []
    inventory_rows: list[Any] = []
    if family != EXPECTED_FAMILY:
        errors.append("family must be TECEdrainprofile")
    try:
        source_report = load_source_pack(source_pack)
        inventory_rows = list(source_report.rows)
    except Exception as exc:
        errors.append(f"source-pack cannot be loaded: {exc}")
    family_rows = [r for r in inventory_rows if _family(r) == EXPECTED_FAMILY]
    drain_body_rows = [r for r in family_rows if _role(r) == EXPECTED_ROLE]
    drain_body_rows.sort(key=lambda r: (_article(r), _source_file(r)))
    diag_csv_rows = _read_diagnostic_csv(length_diagnostic_csv) if length_diagnostic_csv else []
    rows = [_review_row(row, idx, _diagnostic_by_article(diag_csv_rows)) for idx, row in enumerate(drain_body_rows, 1)]
    gates = validate_rows(rows)
    if len(rows) != EXPECTED_REVIEW_ROW_COUNT:
        errors.append("exported_review_row_count != 12")
    if EXPECTED_REVIEW_ROW_COUNT != 12:
        errors.append("expected_review_row_count != 12")
    for key, message in (("invalid_family_rows", "non-TECEdrainprofile rows included"), ("invalid_role_rows", "roles other than drain_body included"), ("prefilled_review_field_rows", "manual review fields are prefilled"), ("production_leakage_rows", "production_safe leakage found"), ("readiness_leakage_rows", "readiness leakage found"), ("diagnostic_only_leakage_rows", "diagnostic_only leakage found"), ("duplicate_review_ids", "duplicate review IDs exist"), ("duplicate_article_numbers", "duplicate article numbers exist")):
        if gates[key]:
            errors.append(message)
    cross_enabled = bool(length_diagnostic_report or length_diagnostic_csv)
    cross_status = "not_enabled"
    cross_errors: list[str] = []
    if cross_enabled:
        cross_status, cross_errors = _crosscheck(rows, length_diagnostic_report, length_diagnostic_csv)
        errors.extend(cross_errors)
    if out:
        Path(out).parent.mkdir(parents=True, exist_ok=True)
        with Path(out).open("w", encoding="utf-8-sig", newline="") as fh:
            writer = csv.DictWriter(fh, fieldnames=CSV_COLUMNS)
            writer.writeheader(); writer.writerows([{c: r.get(c, "") for c in CSV_COLUMNS} for r in rows])
    report = {
        "valid": not errors, "errors": errors, "warnings": warnings, "source_pack_path": str(source_pack), "family": family,
        "input_inventory_row_count": len(inventory_rows), "family_inventory_row_count": len(family_rows), "drain_body_count": len(drain_body_rows),
        "exported_review_row_count": len(rows), "expected_review_row_count": EXPECTED_REVIEW_ROW_COUNT,
        "length_diagnostic_report_path": str(length_diagnostic_report or ""), "length_diagnostic_csv_path": str(length_diagnostic_csv or ""),
        "diagnostic_crosscheck_enabled": cross_enabled, "diagnostic_crosscheck_status": cross_status, "diagnostic_crosscheck_errors": cross_errors,
        "exported_article_numbers": [r["article_number"] for r in rows], **gates, "output_csv_path": str(out or ""),
        "production_safe_candidate_count": sum(1 for r in rows if _clean(r.get("production_safe")).lower() == "true"),
        "production_promotion_blocked": True, "ready_for_benchmark": False, "ready_for_customer_view": False,
        "diagnostic_only_note": DIAGNOSTIC_ONLY_NOTE,
    }
    report["valid"] = not report["errors"]
    return rows, report


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Export read-only TECEdrainprofile drain-body length manual review CSV.")
    parser.add_argument("--source-pack", default=str(DEFAULT_SOURCE_PACK))
    parser.add_argument("--family", default=EXPECTED_FAMILY)
    parser.add_argument("--length-diagnostic-report")
    parser.add_argument("--length-diagnostic-csv")
    parser.add_argument("--out", required=True)
    parser.add_argument("--json-out", required=True)
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args(argv)
    _rows, report = build_report(args.source_pack, args.family, args.length_diagnostic_report, args.length_diagnostic_csv, args.out)
    write_json_output(report, out=args.json_out)
    if args.json:
        write_json_output(report)
    else:
        write_text_output("valid: " + str(report["valid"]))
    return 0 if report["valid"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
