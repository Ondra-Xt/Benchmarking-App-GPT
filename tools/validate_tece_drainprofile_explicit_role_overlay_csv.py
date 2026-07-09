from __future__ import annotations

import argparse
import csv
import json
import sys
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from tools.report_tece_drainprofile_reviewed_role_overlay_preview_qa import (  # noqa: E402
    EXPECTED_ARTICLES,
    EXPECTED_CANONICAL_ROLE_CANDIDATE,
    EXPECTED_CURRENT_SOURCE_PACK_ROLE,
    EXPECTED_REVIEWED_ROLE_DIAGNOSTIC,
    EXPECTED_ROLE_OVERLAY_ACTION,
    _bool,
    _clean,
    _counts,
    _duplicates,
)
from tools.tece_report_output import write_json_output  # noqa: E402
from tools.validate_tece_drainprofile_drain_body_length_role_review_csv import EXPECTED_FAMILY  # noqa: E402

REQUIRED_COLUMNS = [
    "overlay_id", "family", "article_number", "current_source_pack_role", "reviewed_role_diagnostic",
    "canonical_role_candidate", "overlay_action", "overlay_scope", "evidence_chain_ids", "evidence_chain_status",
    "role_overlay_apply_allowed", "source_pack_mutation_allowed", "extraction_logic_change_allowed", "length_overlay_allowed",
    "compatibility_pairing_allowed", "production_promotion_allowed", "benchmark_ready_allowed", "customer_view_allowed",
    "diagnostic_only", "production_safe", "production_promotion_blocked", "ready_for_benchmark", "ready_for_customer_view",
    "recommended_next_action", "production_status_note",
]
EXPECTED_OVERLAY_IDS = [f"TECE-DP-EXPLICIT-ROLE-OVERLAY-{index:06d}" for index in range(1, 13)]
EXPECTED_OVERLAY_SCOPE = "diagnostic_tecedrainprofile_pilot_12_articles"
EXPECTED_EVIDENCE_CHAIN_STATUS = "validated_manual_review_to_design_chain"
REQUIRED_EVIDENCE_TOKENS = [
    "manual_review_csv", "role_correction_impact_report", "reviewed_role_overlay_preview",
    "reviewed_role_overlay_preview_qa", "explicit_role_overlay_design_report",
]
RECOMMENDED_NEXT_ACTION = "Validate this explicit role overlay artifact only as diagnostic evidence; do not apply overlay, mutate source-pack, generate compatibility pairs, or promote production/customer outputs."
PRODUCTION_STATUS_NOTE = "diagnostic-only TECEdrainprofile explicit role overlay artifact; validation only; no source-pack mutation; no extraction logic change; no length overlay; no compatibility pairing; no Products/BOM/Final_Assemblies/Final_Set_Details/customer-view promotion"
DIAGNOSTIC_ONLY_NOTE = "This validator is read-only and diagnostic-only. It validates a TECEdrainprofile explicit role overlay CSV against the validated manual review, preview QA, and design chain and does not apply overlays, mutate source-pack files, change TECE logic, change ACO export, Products, Comparison, BOM_Options, Final_Assemblies, or Final_Set_Details. Production promotion remains blocked."
ZERO_TRUE_FIELDS = [
    "role_overlay_apply_allowed", "source_pack_mutation_allowed", "extraction_logic_change_allowed", "length_overlay_allowed",
    "compatibility_pairing_allowed", "production_promotion_allowed", "benchmark_ready_allowed", "customer_view_allowed",
]


def _read_csv(path: str | Path, errors: list[str], label: str) -> list[dict[str, str]]:
    try:
        with Path(path).open(encoding="utf-8-sig", newline="") as fh:
            return list(csv.DictReader(fh))
    except Exception as exc:
        errors.append(f"{label} missing or cannot be loaded: {exc}")
        return []


def _read_json(path: str | Path, errors: list[str], label: str) -> dict[str, Any]:
    try:
        return json.loads(Path(path).read_text(encoding="utf-8"))
    except Exception as exc:
        errors.append(f"{label} missing or cannot be loaded: {exc}")
        return {}


def _row_ref(row: dict[str, str], index: int) -> dict[str, str]:
    return {"row_number": str(index), "overlay_id": _clean(row.get("overlay_id")), "article_number": _clean(row.get("article_number"))}


def validate_overlay_csv(overlay_csv: str | Path, design_report: str | Path, design_csv: str | Path, qa_report: str | Path, qa_csv: str | Path, family: str = EXPECTED_FAMILY) -> dict[str, Any]:
    errors: list[str] = []
    warnings: list[str] = []
    rows = _read_csv(overlay_csv, errors, "overlay CSV")
    design = _read_json(design_report, errors, "design report")
    qa = _read_json(qa_report, errors, "QA report")
    design_rows = _read_csv(design_csv, errors, "design CSV")
    qa_rows = _read_csv(qa_csv, errors, "QA CSV")

    fieldnames = list(rows[0].keys()) if rows else []
    required_columns_missing = [c for c in REQUIRED_COLUMNS if c not in fieldnames]
    article_numbers = [_clean(r.get("article_number")) for r in rows]
    overlay_ids = [_clean(r.get("overlay_id")) for r in rows]
    duplicate_overlay_ids = _duplicates(overlay_ids)
    duplicate_article_numbers = _duplicates(article_numbers)

    invalid_family_rows: list[dict[str, str]] = []
    invalid_overlay_id_rows: list[dict[str, str]] = []
    invalid_article_mapping_rows: list[dict[str, str]] = []
    invalid_overlay_scope_rows: list[dict[str, str]] = []
    invalid_evidence_chain_rows: list[dict[str, str]] = []
    invalid_blocking_rows: list[dict[str, str]] = []
    production_leakage_rows: list[dict[str, str]] = []
    readiness_leakage_rows: list[dict[str, str]] = []
    diagnostic_only_leakage_rows: list[dict[str, str]] = []

    for index, row in enumerate(rows, start=1):
        ref = _row_ref(row, index)
        article = ref["article_number"]
        if _clean(row.get("family")) != EXPECTED_FAMILY:
            invalid_family_rows.append(ref)
        if _clean(row.get("overlay_id")) != f"TECE-DP-EXPLICIT-ROLE-OVERLAY-{index:06d}":
            invalid_overlay_id_rows.append(ref)
        if (
            _clean(row.get("current_source_pack_role")) != EXPECTED_CURRENT_SOURCE_PACK_ROLE.get(article)
            or _clean(row.get("reviewed_role_diagnostic")) != EXPECTED_REVIEWED_ROLE_DIAGNOSTIC.get(article)
            or _clean(row.get("canonical_role_candidate")) != EXPECTED_CANONICAL_ROLE_CANDIDATE.get(article)
            or _clean(row.get("overlay_action")) != EXPECTED_ROLE_OVERLAY_ACTION.get(article)
        ):
            invalid_article_mapping_rows.append(ref)
        if _clean(row.get("overlay_scope")) != EXPECTED_OVERLAY_SCOPE:
            invalid_overlay_scope_rows.append(ref)
        evidence = _clean(row.get("evidence_chain_ids"))
        if not evidence or any(token not in evidence for token in REQUIRED_EVIDENCE_TOKENS) or _clean(row.get("evidence_chain_status")) != EXPECTED_EVIDENCE_CHAIN_STATUS:
            invalid_evidence_chain_rows.append(ref)
        blocking_ok = (
            all(not _bool(row.get(name)) for name in ZERO_TRUE_FIELDS)
            and _bool(row.get("diagnostic_only"))
            and not _bool(row.get("production_safe"))
            and _bool(row.get("production_promotion_blocked"))
            and not _bool(row.get("ready_for_benchmark"))
            and not _bool(row.get("ready_for_customer_view"))
            and _clean(row.get("recommended_next_action")) == RECOMMENDED_NEXT_ACTION
            and _clean(row.get("production_status_note")) == PRODUCTION_STATUS_NOTE
        )
        if not blocking_ok:
            invalid_blocking_rows.append(ref)
        if _bool(row.get("production_safe")) or not _bool(row.get("production_promotion_blocked")):
            production_leakage_rows.append(ref)
        if _bool(row.get("ready_for_benchmark")) or _bool(row.get("ready_for_customer_view")):
            readiness_leakage_rows.append(ref)
        if not _bool(row.get("diagnostic_only")):
            diagnostic_only_leakage_rows.append(ref)

    design_report_valid = design.get("valid") is True
    qa_report_valid = qa.get("valid") is True
    design_rule_count = design.get("design_rule_count", len(design_rows))
    qa_row_count = qa.get("qa_row_count")
    total_qa_csv_rows = len(qa_rows)
    checks = [
        (family == EXPECTED_FAMILY, "family must be TECEdrainprofile"),
        (design_report_valid, "design report valid must be true"),
        (qa_report_valid, "QA report valid must be true"),
        (qa.get("qa_row_count") == 12, "QA report qa_row_count must be 12"),
        (total_qa_csv_rows == 12, "QA CSV row count must be 12"),
        (int(design_rule_count or 0) >= 12, "design_rule_count must be at least 12"),
        (design.get("mapping_status_pass_count") in (None, 12), "design report mapping_status_pass_count must be 12"),
        (design.get("blocking_status_pass_count") in (None, 12), "design report blocking_status_pass_count must be 12"),
        (design.get("qa_status_pass_count") in (None, 12), "design report qa_status_pass_count must be 12"),
        (design.get("source_pack_mutation_allowed_count") in (None, 0), "design report source_pack_mutation_allowed_count must be 0"),
        (design.get("extraction_logic_change_allowed_count") in (None, 0), "design report extraction_logic_change_allowed_count must be 0"),
        (design.get("length_overlay_allowed_count") in (None, 0), "design report length_overlay_allowed_count must be 0"),
        (design.get("compatibility_pairing_allowed_count") in (None, 0), "design report compatibility_pairing_allowed_count must be 0"),
        (design.get("production_promotion_allowed_count") in (None, 0), "design report production_promotion_allowed_count must be 0"),
        (design.get("benchmark_ready_allowed_count") in (None, 0), "design report benchmark_ready_allowed_count must be 0"),
        (design.get("customer_view_allowed_count") in (None, 0), "design report customer_view_allowed_count must be 0"),
        (design.get("proposed_source_pack_mutation_count") in (None, 0), "design report proposed_source_pack_mutation_count must be 0"),
        (design.get("production_safe_candidate_count") in (None, 0), "design report production_safe_candidate_count must be 0"),
        (design.get("production_promotion_blocked") in (None, True), "design report production_promotion_blocked must be true"),
        (design.get("ready_for_benchmark") in (None, False), "design report ready_for_benchmark must be false"),
        (design.get("ready_for_customer_view") in (None, False), "design report ready_for_customer_view must be false"),
        (qa.get("role_overlay_apply_allowed_count") in (None, 0), "QA report role_overlay_apply_allowed_count must be 0"),
        (qa.get("source_pack_mutation_allowed_count") in (None, 0), "QA report source_pack_mutation_allowed_count must be 0"),
        (qa.get("length_overlay_allowed_count") in (None, 0), "QA report length_overlay_allowed_count must be 0"),
        (qa.get("compatibility_pairing_allowed_count") in (None, 0), "QA report compatibility_pairing_allowed_count must be 0"),
        (qa.get("production_safe_candidate_count") in (None, 0), "QA report production_safe_candidate_count must be 0"),
        (qa.get("production_promotion_blocked") in (None, True), "QA report production_promotion_blocked must be true"),
        (qa.get("ready_for_benchmark") in (None, False), "QA report ready_for_benchmark must be false"),
        (qa.get("ready_for_customer_view") in (None, False), "QA report ready_for_customer_view must be false"),
        (not required_columns_missing, "required overlay columns are missing"),
        (len(rows) == 12, "overlay row count must be 12"),
        (sorted(article_numbers) == EXPECTED_ARTICLES, "article set differs from expected 12 articles"),
        (overlay_ids == EXPECTED_OVERLAY_IDS, "overlay_id sequence differs from expected deterministic IDs"),
        (not duplicate_overlay_ids, "duplicate overlay_id detected"),
        (not duplicate_article_numbers, "duplicate article_number detected"),
        (not invalid_family_rows, "invalid family rows detected"),
        (not invalid_article_mapping_rows, "invalid article mapping rows detected"),
        (not invalid_overlay_scope_rows, "invalid overlay_scope rows detected"),
        (not invalid_evidence_chain_rows, "invalid evidence chain rows detected"),
        (not invalid_blocking_rows, "invalid blocking rows detected"),
        (not production_leakage_rows, "production leakage detected"),
        (not readiness_leakage_rows, "readiness leakage detected"),
        (not diagnostic_only_leakage_rows, "diagnostic_only must be true for every row"),
    ]
    for ok, message in checks:
        if not ok and message not in errors:
            errors.append(message)

    return {
        "valid": not errors, "errors": errors, "warnings": warnings, "family": family,
        "overlay_csv_path": str(overlay_csv), "design_report_path": str(design_report), "design_csv_path": str(design_csv),
        "qa_report_path": str(qa_report), "qa_csv_path": str(qa_csv), "design_report_valid": design_report_valid,
        "qa_report_valid": qa_report_valid, "overlay_row_count": len(rows), "design_rule_count": design_rule_count,
        "qa_row_count": qa_row_count, "total_qa_csv_rows": total_qa_csv_rows, "article_numbers": sorted(article_numbers),
        "overlay_id_count": len(set(overlay_ids)), "duplicate_overlay_ids": duplicate_overlay_ids,
        "duplicate_article_numbers": duplicate_article_numbers, "required_columns_missing": required_columns_missing,
        "current_source_pack_role_counts": _counts([_clean(r.get("current_source_pack_role")) for r in rows]),
        "reviewed_role_diagnostic_counts": _counts([_clean(r.get("reviewed_role_diagnostic")) for r in rows]),
        "canonical_role_candidate_counts": _counts([_clean(r.get("canonical_role_candidate")) for r in rows]),
        "overlay_action_counts": _counts([_clean(r.get("overlay_action")) for r in rows]),
        "overlay_scope_counts": _counts([_clean(r.get("overlay_scope")) for r in rows]),
        "evidence_chain_status_counts": _counts([_clean(r.get("evidence_chain_status")) for r in rows]),
        "invalid_family_rows": invalid_family_rows, "invalid_overlay_id_rows": invalid_overlay_id_rows,
        "invalid_article_mapping_rows": invalid_article_mapping_rows, "invalid_overlay_scope_rows": invalid_overlay_scope_rows,
        "invalid_evidence_chain_rows": invalid_evidence_chain_rows, "invalid_blocking_rows": invalid_blocking_rows,
        "production_leakage_rows": production_leakage_rows, "readiness_leakage_rows": readiness_leakage_rows,
        "diagnostic_only_leakage_rows": diagnostic_only_leakage_rows,
        **{f"{name}_count": sum(_bool(r.get(name)) for r in rows) for name in ZERO_TRUE_FIELDS},
        "proposed_source_pack_mutation_count": sum(_bool(r.get("proposed_source_pack_mutation")) for r in rows),
        "production_safe_candidate_count": sum(_bool(r.get("production_safe")) for r in rows),
        "production_promotion_blocked": not production_leakage_rows, "ready_for_benchmark": any(_bool(r.get("ready_for_benchmark")) for r in rows),
        "ready_for_customer_view": any(_bool(r.get("ready_for_customer_view")) for r in rows), "diagnostic_only_note": DIAGNOSTIC_ONLY_NOTE,
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Validate read-only TECEdrainprofile explicit role overlay CSV diagnostic artifact.")
    parser.add_argument("--overlay-csv", required=True); parser.add_argument("--design-report", required=True); parser.add_argument("--design-csv", required=True)
    parser.add_argument("--qa-report", required=True); parser.add_argument("--qa-csv", required=True); parser.add_argument("--family", default=EXPECTED_FAMILY)
    parser.add_argument("--json-out", required=True); parser.add_argument("--json", action="store_true")
    args = parser.parse_args(argv)
    report = validate_overlay_csv(args.overlay_csv, args.design_report, args.design_csv, args.qa_report, args.qa_csv, family=args.family)
    write_json_output(report, out=args.json_out)
    if args.json:
        write_json_output(report)
    return 0 if report["valid"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
