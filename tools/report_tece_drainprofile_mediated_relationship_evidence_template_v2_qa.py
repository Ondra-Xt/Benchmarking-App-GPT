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

from tools.tece_report_output import write_json_output  # noqa: E402
from tools import validate_tece_drainprofile_mediated_relationship_evidence_template_v2_csv as validator  # noqa: E402

DEFAULT_SOURCE_PACK = validator.DEFAULT_SOURCE_PACK
EXPECTED_FAMILY = validator.EXPECTED_FAMILY
EXPECTED_QA_ROW_COUNT = 15
QA_ROW_ID_PREFIX = "TECE-DP-MEDIATED-EVIDENCE-TEMPLATE-V2-QA"
EXPECTED_QA_ROW_IDS = [f"{QA_ROW_ID_PREFIX}-{i:06d}" for i in range(1, EXPECTED_QA_ROW_COUNT + 1)]

DIAGNOSTIC_ONLY_NOTE = "This TECEdrainprofile mediated relationship evidence template v2 QA report is read-only and diagnostic-only. It confirms that reviewed official evidence is complete for the future mediated diagnostic evidence scope: drain-body-to-Duschprofil relationship, Duschprofil/profile-cover scope confirmation, and proposed profile-cover article scope. It does not generate direct or mediated compatibility pairs, generate candidate pair matrices, create length overlays, mutate source-pack files, change TECE logic, change ACO export, or promote Products, Comparison, BOM_Options, Final_Assemblies, or Final_Set_Details. Production promotion remains blocked."
QA_NOTE = "Reviewed evidence row passes diagnostic-only QA for future mediated diagnostic design evidence scope."
PRODUCTION_STATUS_NOTE = "Diagnostic-only QA blocks generation, mutation, promotion, benchmark readiness, and customer-view readiness."

CSV_COLUMNS = [
    "qa_row_id", "template_v2_row_id", "family", "article_number", "evidence_collection_area",
    "evidence_target_type", "reviewer_decision", "source_evidence_complete",
    "safe_to_use_for_future_diagnostic_design", "qa_status", "qa_blocking_status",
    "diagnostic_scope_confirmed", "future_diagnostic_design_ready", "direct_pair_generation_allowed",
    "mediated_pair_generation_allowed", "candidate_matrix_generation_allowed", "evidence_acceptance_allowed",
    "evidence_complete_flag_allowed", "ready_for_future_diagnostic_design_flag_allowed",
    "source_pack_mutation_allowed", "extraction_logic_change_allowed", "role_overlay_allowed",
    "length_overlay_allowed", "production_promotion_allowed", "benchmark_ready_allowed",
    "customer_view_allowed", "diagnostic_only", "qa_note", "production_status_note",
]

FALSE_ALLOWED_FIELDS = [
    "direct_pair_generation_allowed", "mediated_pair_generation_allowed", "candidate_matrix_generation_allowed",
    "evidence_acceptance_allowed", "evidence_complete_flag_allowed",
    "ready_for_future_diagnostic_design_flag_allowed", "source_pack_mutation_allowed",
    "extraction_logic_change_allowed", "role_overlay_allowed", "length_overlay_allowed",
    "production_promotion_allowed", "benchmark_ready_allowed", "customer_view_allowed",
]
GENERATED_COUNTERS = [
    "generated_direct_pair_count", "generated_mediated_pair_count", "generated_candidate_matrix_row_count",
    "generated_length_overlay_row_count", "proposed_source_pack_mutation_count",
]


def _clean(value: Any) -> str:
    return "" if value is None else str(value).strip()


def _bool(value: Any) -> bool:
    return value is True or _clean(value).lower() in {"1", "true", "yes", "y"}


def _counts(values) -> dict[str, int]:
    return dict(sorted(Counter(values).items()))


def _read_json(path: str | Path, label: str, errors: list[str]) -> dict[str, Any]:
    try:
        return json.loads(Path(path).read_text(encoding="utf-8"))
    except Exception as exc:
        errors.append(f"{label} missing or cannot be loaded: {exc}")
        return {}


def _read_csv(path: str | Path, errors: list[str]) -> list[dict[str, str]]:
    try:
        with Path(path).open(encoding="utf-8-sig", newline="") as fh:
            return list(csv.DictReader(fh))
    except Exception as exc:
        errors.append(f"evidence CSV missing or cannot be loaded: {exc}")
        return []


def _source_evidence_complete(row: dict[str, Any]) -> bool:
    return all(_clean(row.get(field)) for field in validator.SOURCE_EVIDENCE_FIELDS)


def build_qa_rows(evidence_rows: list[dict[str, Any]]) -> list[dict[str, str]]:
    qa_rows: list[dict[str, str]] = []
    for idx, row in enumerate(evidence_rows, 1):
        qa = {
            "qa_row_id": f"{QA_ROW_ID_PREFIX}-{idx:06d}",
            "template_v2_row_id": _clean(row.get("template_v2_row_id")),
            "family": _clean(row.get("family")),
            "article_number": _clean(row.get("article_number")),
            "evidence_collection_area": _clean(row.get("evidence_collection_area")),
            "evidence_target_type": _clean(row.get("evidence_target_type")),
            "reviewer_decision": _clean(row.get("reviewer_decision")),
            "source_evidence_complete": "true" if _source_evidence_complete(row) else "false",
            "safe_to_use_for_future_diagnostic_design": "true" if _bool(row.get("safe_to_use_for_future_diagnostic_design")) else "false",
            "qa_status": "pass" if _source_evidence_complete(row) and _bool(row.get("safe_to_use_for_future_diagnostic_design")) else "fail",
            "qa_blocking_status": "diagnostic_only_generation_blocked",
            "diagnostic_scope_confirmed": "true",
            "future_diagnostic_design_ready": "true",
            "diagnostic_only": "true",
            "qa_note": QA_NOTE,
            "production_status_note": PRODUCTION_STATUS_NOTE,
        }
        for field in FALSE_ALLOWED_FIELDS:
            qa[field] = "false"
        qa_rows.append(qa)
    return qa_rows


def _write_csv(path: str | Path, rows: list[dict[str, str]]) -> None:
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    with Path(path).open("w", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=CSV_COLUMNS, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def generate_report(validation_report: str | Path, evidence_csv: str | Path, template_v2_report: str | Path, source_pack: str | Path = DEFAULT_SOURCE_PACK, family: str = EXPECTED_FAMILY) -> tuple[dict[str, Any], list[dict[str, str]]]:
    errors: list[str] = []
    warnings: list[str] = []
    validation = _read_json(validation_report, "validation report", errors)
    template = _read_json(template_v2_report, "template v2 report", errors)
    evidence_rows = _read_csv(evidence_csv, errors)
    qa_rows = build_qa_rows(evidence_rows)

    validation_report_valid = validation.get("valid") is True
    template_v2_report_valid = template.get("valid") is True
    if not validation_report_valid:
        errors.append("validation report valid must be true")
    if validation.get("errors") not in ([], None):
        errors.append("validation report errors must be empty")
    if not template_v2_report_valid:
        errors.append("template v2 report valid must be true")

    baseline = validator.validate(evidence_csv, template_v2_report, source_pack, family)
    for err in baseline.get("errors", []):
        if err not in errors:
            errors.append(err)

    qa_ids = [_clean(r.get("qa_row_id")) for r in qa_rows]
    report: dict[str, Any] = {
        "valid": False, "errors": errors, "warnings": warnings, "family": family,
        "source_pack_path": str(source_pack), "validation_report_path": str(validation_report),
        "evidence_csv_path": str(evidence_csv), "template_v2_report_path": str(template_v2_report),
        "validation_report_valid": validation_report_valid, "template_v2_report_valid": template_v2_report_valid,
        "input_inventory_row_count": baseline.get("input_inventory_row_count", 0),
        "family_inventory_row_count": baseline.get("family_inventory_row_count", 0),
        "current_machine_role_counts": baseline.get("current_machine_role_counts", {}),
        "evidence_csv_row_count": len(evidence_rows), "template_v2_row_count": int(baseline.get("template_v2_row_count", len(evidence_rows)) or 0),
        "qa_row_count": len(qa_rows), "qa_row_ids": qa_ids,
        "evidence_collection_area_counts": _counts(_clean(r.get("evidence_collection_area")) for r in evidence_rows),
        "evidence_target_type_counts": _counts(_clean(r.get("evidence_target_type")) for r in evidence_rows),
        "reviewer_decision_counts": _counts(_clean(r.get("reviewer_decision")) for r in evidence_rows),
        "qa_status_counts": _counts(_clean(r.get("qa_status")) for r in qa_rows),
        "qa_blocking_status_counts": _counts(_clean(r.get("qa_blocking_status")) for r in qa_rows),
        "retained_drain_body_articles": validator.RETAINED_DRAIN_BODY_ARTICLES,
        "proposed_profile_cover_articles": validator.PROPOSED_PROFILE_COVER_ARTICLES,
        "accepted_drain_body_to_duschprofil_evidence_count": baseline.get("accepted_drain_body_to_duschprofil_evidence_count", 0),
        "accepted_duschprofil_profile_cover_scope_count": baseline.get("accepted_duschprofil_profile_cover_scope_count", 0),
        "accepted_profile_cover_article_scope_count": baseline.get("accepted_profile_cover_article_scope_count", 0),
        "accepted_mediated_evidence_row_count": baseline.get("accepted_mediated_evidence_row_count", 0),
        "reviewed_evidence_complete_row_count": baseline.get("reviewed_evidence_complete_row_count", 0),
        "reviewed_evidence_incomplete_row_count": baseline.get("reviewed_evidence_incomplete_row_count", 0),
        "safe_to_use_for_future_diagnostic_design_true_count": baseline.get("safe_to_use_for_future_diagnostic_design_true_count", 0),
        "ready_for_future_diagnostic_design": baseline.get("ready_for_future_diagnostic_design") is True,
        "qa_pass_count": sum(_clean(r.get("qa_status")) == "pass" for r in qa_rows),
        "qa_fail_count": sum(_clean(r.get("qa_status")) != "pass" for r in qa_rows),
        "diagnostic_scope_confirmed_count": sum(_bool(r.get("diagnostic_scope_confirmed")) for r in qa_rows),
        "future_diagnostic_design_ready_count": sum(_bool(r.get("future_diagnostic_design_ready")) for r in qa_rows),
        "invalid_direct_pairing_claim_rows": baseline.get("invalid_direct_pairing_claim_rows", []),
        "invalid_direct_pairing_claim_details": baseline.get("invalid_direct_pairing_claim_details", []),
        "production_safe_candidate_count": int(template.get("production_safe_candidate_count", baseline.get("production_safe_candidate_count", 0)) or 0),
        "production_promotion_blocked": True,
        "ready_for_benchmark": False,
        "ready_for_customer_view": False,
        "diagnostic_only_note": DIAGNOSTIC_ONLY_NOTE,
    }
    for field in FALSE_ALLOWED_FIELDS:
        report[f"{field}_count"] = sum(_bool(r.get(field)) for r in qa_rows)
    for counter in GENERATED_COUNTERS:
        report[counter] = int(template.get(counter, baseline.get(counter, 0)) or 0)

    checks = [
        (family == EXPECTED_FAMILY, "family must be TECEdrainprofile"),
        (report["input_inventory_row_count"] == validator.EXPECTED_INPUT_INVENTORY_ROW_COUNT and report["family_inventory_row_count"] == validator.EXPECTED_FAMILY_INVENTORY_ROW_COUNT and report["current_machine_role_counts"] == validator.EXPECTED_MACHINE_ROLE_COUNTS, "source-pack baseline counts differ from expected"),
        (report["evidence_csv_row_count"] == 15, "evidence_csv_row_count must be 15"),
        (report["template_v2_row_count"] == 15, "template_v2_row_count must be 15"),
        (report["qa_row_count"] == 15, "qa_row_count must be 15"),
        (qa_ids == EXPECTED_QA_ROW_IDS and len(set(qa_ids)) == len(qa_ids), "QA row IDs must be deterministic and unique"),
        (report["accepted_mediated_evidence_row_count"] == 15, "accepted_mediated_evidence_row_count must be 15"),
        (report["reviewed_evidence_complete_row_count"] == 15, "reviewed_evidence_complete_row_count must be 15"),
        (report["reviewed_evidence_incomplete_row_count"] == 0, "reviewed_evidence_incomplete_row_count must be 0"),
        (report["safe_to_use_for_future_diagnostic_design_true_count"] == 15, "safe_to_use_for_future_diagnostic_design_true_count must be 15"),
        (report["ready_for_future_diagnostic_design"] is True, "ready_for_future_diagnostic_design must be true"),
        (not report["invalid_direct_pairing_claim_rows"] and not report["invalid_direct_pairing_claim_details"], "invalid direct pairing claims must be empty"),
        (report["qa_fail_count"] == 0 and report["qa_pass_count"] == 15, "all QA rows must pass"),
        (report["diagnostic_scope_confirmed_count"] == 15, "diagnostic_scope_confirmed must be true for all rows"),
        (report["future_diagnostic_design_ready_count"] == 15, "future_diagnostic_design_ready must be true for all rows"),
        (sum(not _bool(r.get("diagnostic_only")) for r in qa_rows) == 0, "diagnostic_only must be true for all rows"),
        (report["production_safe_candidate_count"] == 0, "production_safe_candidate_count must be 0"),
        (report["production_promotion_blocked"] is True, "production_promotion_blocked must be true"),
        (report["ready_for_benchmark"] is False, "ready_for_benchmark must be false"),
        (report["ready_for_customer_view"] is False, "ready_for_customer_view must be false"),
    ]
    for field in FALSE_ALLOWED_FIELDS:
        checks.append((report[f"{field}_count"] == 0, f"{field}_count must be 0"))
    for counter in GENERATED_COUNTERS:
        checks.append((report[counter] == 0, f"{counter} must be 0"))
    for ok, message in checks:
        if not ok and message not in errors:
            errors.append(message)
    report["valid"] = not errors
    return report, qa_rows


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Create diagnostic-only QA report for reviewed TECEdrainprofile mediated relationship evidence template v2 validation.")
    parser.add_argument("--validation-report", required=True)
    parser.add_argument("--evidence-csv", required=True)
    parser.add_argument("--template-v2-report", required=True)
    parser.add_argument("--source-pack", default=str(DEFAULT_SOURCE_PACK))
    parser.add_argument("--family", default=EXPECTED_FAMILY)
    parser.add_argument("--out", required=True)
    parser.add_argument("--json-out", required=True)
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args(argv)
    report, qa_rows = generate_report(args.validation_report, args.evidence_csv, args.template_v2_report, args.source_pack, args.family)
    _write_csv(args.out, qa_rows)
    write_json_output(report, out=args.json_out)
    if args.json:
        write_json_output(report)
    return 0 if report["valid"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
