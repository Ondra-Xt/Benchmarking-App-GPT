from __future__ import annotations

import argparse, csv, json, re, sys
from collections import Counter
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from tools.export_tece_drainprofile_explicit_compatibility_evidence_collection_template import (  # noqa:E402
    BLOCKING_FIELDS, CSV_COLUMNS, DEFAULT_SOURCE_PACK, EXPECTED_FAMILY,
    EXPECTED_FAMILY_INVENTORY_ROW_COUNT, EXPECTED_INPUT_INVENTORY_ROW_COUNT,
    EXPECTED_MACHINE_ROLE_COUNTS, MANUAL_EVIDENCE_FIELDS, REQUIRED_TEMPLATE_IDS,
    RETAINED_DRAIN_BODY_ARTICLE_NUMBERS, _bool, _clean, _counts, _dups, _int,
)
from tools.report_tece_source_inventory import load_source_pack  # noqa:E402
from tools.tece_report_output import write_json_output  # noqa:E402

DIAGNOSTIC_ONLY_NOTE = "This evidence collection validator is read-only and diagnostic-only. It validates manually collected TECEdrainprofile compatibility evidence for structural completeness and future diagnostic-design eligibility only, and confirms that compatibility pair generation, candidate pair matrix generation, length overlay, source-pack mutation, TECE logic change, ACO export change, Products, Comparison, BOM_Options, Final_Assemblies, or Final_Set_Details promotion remain blocked. Production promotion remains blocked."

ALLOWED_DECISIONS = {"", "accepted_explicit_nominal_length_evidence", "accepted_length_independence_evidence", "accepted_explicit_compatibility_matrix", "accepted_explicit_compatibility_statement", "accepted_profile_cover_scope_confirmation", "rejected_insufficient_evidence", "rejected_inferred_only", "rejected_wrong_family", "rejected_wrong_article_scope", "rejected_unofficial_source", "evidence_ambiguous"}
ACCEPTED_BY_AREA = {
    "retained_drain_body_nominal_length_evidence": {"accepted_explicit_nominal_length_evidence", "accepted_length_independence_evidence"},
    "article_level_drain_body_to_profile_cover_compatibility_matrix": {"accepted_explicit_compatibility_matrix", "accepted_explicit_compatibility_statement"},
    "profile_cover_scope_confirmation": {"accepted_profile_cover_scope_confirmation"},
}
REJECTED_INFERENCE_PATTERNS = [r"same\s+length\s+inferred", r"same\s+family", r"nearby\s+row", r"article\s+number\s+inferred", r"generic\s+compatibility\s+assumption"]
EXPECTED_ROW_MAPPING = {
    "TECE-DP-COMPAT-EVIDENCE-TEMPLATE-000001": ("673001", "retained_drain_body_nominal_length_evidence", "retained_drain_body"),
    "TECE-DP-COMPAT-EVIDENCE-TEMPLATE-000002": ("673002", "retained_drain_body_nominal_length_evidence", "retained_drain_body"),
    "TECE-DP-COMPAT-EVIDENCE-TEMPLATE-000003": ("673003", "retained_drain_body_nominal_length_evidence", "retained_drain_body"),
    "TECE-DP-COMPAT-EVIDENCE-TEMPLATE-000004": ("", "article_level_drain_body_to_profile_cover_compatibility_matrix", "compatibility_matrix_or_statement"),
    "TECE-DP-COMPAT-EVIDENCE-TEMPLATE-000005": ("", "profile_cover_scope_confirmation", "profile_cover_scope"),
}
REPORT_ZERO_FIELDS = ["generated_compatibility_pair_count", "generated_candidate_pair_matrix_row_count", "generated_length_overlay_row_count", "proposed_source_pack_mutation_count", "production_safe_candidate_count"]
REPORT_ALLOWED_ZERO_FIELDS = ["pairing_design_allowed_after_collection_count", "compatibility_pair_generation_allowed_count", "candidate_pair_matrix_allowed_count", "length_overlay_allowed_count", "source_pack_mutation_allowed_count", "production_promotion_allowed_count", "benchmark_ready_allowed_count", "customer_view_allowed_count"]


def _read_csv(path: str | Path, errors: list[str], label: str) -> tuple[list[dict[str, str]], list[str]]:
    try:
        with Path(path).open(encoding="utf-8-sig", newline="") as fh:
            reader = csv.DictReader(fh)
            return list(reader), list(reader.fieldnames or [])
    except Exception as exc:
        errors.append(f"{label} missing or cannot be loaded: {exc}")
        return [], []


def _read_json(path: str | Path, errors: list[str]) -> dict[str, Any]:
    try:
        return json.loads(Path(path).read_text(encoding="utf-8"))
    except Exception as exc:
        errors.append(f"template report missing or cannot be loaded: {exc}")
        return {}


def _source_pack_counts(source_pack: str | Path, family: str, errors: list[str]) -> tuple[int, int, dict[str, int]]:
    try:
        report = load_source_pack(source_pack)
    except Exception as exc:
        errors.append(f"source-pack cannot be loaded: {exc}")
        return 0, 0, {}
    rows = list(getattr(report, "rows", []) or [])
    fam = [r for r in rows if (_clean(getattr(r, "tece_family_candidate", "")) or _clean(getattr(r, "product_family", ""))) == family]
    roles = [_clean(getattr(r, "tece_article_role_candidate", "")) or _clean(getattr(r, "article_role", "")) or "unknown" for r in fam]
    return len(rows), len(fam), _counts(roles)


def _manual_blank(row: dict[str, str]) -> bool:
    return all(not _clean(row.get(f)) for f in MANUAL_EVIDENCE_FIELDS)


def validate_evidence_collection_csv(evidence_csv: str | Path, template_report: str | Path, template_csv: str | Path, source_pack: str | Path = DEFAULT_SOURCE_PACK, family: str = EXPECTED_FAMILY) -> dict[str, Any]:
    errors: list[str] = []; warnings: list[str] = []
    evidence_rows, evidence_cols = _read_csv(evidence_csv, errors, "evidence CSV")
    template_rows, _template_cols = _read_csv(template_csv, errors, "template CSV")
    report_json = _read_json(template_report, errors)
    input_count, family_count, machine_counts = _source_pack_counts(source_pack, family, errors)

    required_missing = sorted(c for c in CSV_COLUMNS if c not in evidence_cols)
    template_ids = [_clean(r.get("template_row_id")) for r in evidence_rows]
    duplicate_ids = _dups(template_ids)
    article_keys = [_clean(r.get("article_number")) for r in evidence_rows if _clean(r.get("evidence_target_type")) == "retained_drain_body"]
    duplicate_articles = _dups(article_keys)

    invalid_family: list[str] = []; invalid_template: list[str] = []; invalid_decision: list[str] = []; invalid_evidence: list[str] = []; invalid_inference: list[str] = []
    invalid_blocking: list[str] = []; production_leakage: list[str] = []; readiness_leakage: list[str] = []; diagnostic_leakage: list[str] = []
    row_statuses: list[str] = []; complete = 0; rejected = 0; missing = 0; safe_true = 0
    acc_len = acc_comp = acc_scope = 0

    for idx, row in enumerate(evidence_rows, 1):
        rid = _clean(row.get("template_row_id")) or str(idx)
        area = _clean(row.get("evidence_collection_area")); decision = _clean(row.get("reviewer_decision")); safe = _bool(row.get("safe_to_use_for_future_diagnostic_design"))
        accepted_ok = decision in ACCEPTED_BY_AREA.get(area, set())
        if _clean(row.get("family")) != EXPECTED_FAMILY: invalid_family.append(rid)
        exp = EXPECTED_ROW_MAPPING.get(rid)
        if not exp or (_clean(row.get("article_number")), area, _clean(row.get("evidence_target_type"))) != exp: invalid_template.append(rid)
        if decision not in ALLOWED_DECISIONS: invalid_decision.append(rid)
        if any(_bool(row.get(f)) for f in BLOCKING_FIELDS): invalid_blocking.append(rid); production_leakage.append(rid)
        if _bool(row.get("benchmark_ready_allowed")) or _bool(row.get("customer_view_allowed")): readiness_leakage.append(rid)
        if not _bool(row.get("diagnostic_only")): diagnostic_leakage.append(rid)
        if safe: safe_true += 1
        req_fields_ok = bool(_clean(row.get("source_document_name")) and (_clean(row.get("source_page_or_section")) or _clean(row.get("source_url_or_path"))) and _clean(row.get("source_text_excerpt")) and _clean(row.get("reviewed_evidence_summary")) and _clean(row.get("reviewer_notes")))
        text = f"{row.get('reviewed_evidence_summary','')} {row.get('reviewer_notes','')}".lower()
        if decision.startswith("accepted_") and any(re.search(p, text) for p in REJECTED_INFERENCE_PATTERNS): invalid_inference.append(rid)
        if safe and not accepted_ok: invalid_decision.append(rid)
        if safe and not req_fields_ok: invalid_evidence.append(rid)
        if accepted_ok and safe and req_fields_ok and rid not in invalid_inference:
            row_statuses.append("evidence_complete"); complete += 1
            if area == "retained_drain_body_nominal_length_evidence": acc_len += 1
            elif area == "article_level_drain_body_to_profile_cover_compatibility_matrix": acc_comp += 1
            elif area == "profile_cover_scope_confirmation": acc_scope += 1
        elif decision.startswith("rejected_") or decision == "evidence_ambiguous":
            row_statuses.append("evidence_rejected_or_ambiguous"); rejected += 1
        elif decision == "" and _manual_blank(row):
            row_statuses.append("missing_manual_evidence"); missing += 1
        else:
            row_statuses.append("evidence_incomplete")

    invalid_decision = sorted(set(invalid_decision)); invalid_evidence = sorted(set(invalid_evidence))
    counts = {f: sum(_bool(r.get(f)) for r in evidence_rows) for f in BLOCKING_FIELDS}
    generated = {f: _int(report_json.get(f)) for f in REPORT_ZERO_FIELDS}
    checks = [
        (report_json.get("valid") is True, "template report valid must be true"), (family == EXPECTED_FAMILY and report_json.get("family") == EXPECTED_FAMILY, "family must be TECEdrainprofile"),
        (input_count == EXPECTED_INPUT_INVENTORY_ROW_COUNT and family_count == EXPECTED_FAMILY_INVENTORY_ROW_COUNT and machine_counts == EXPECTED_MACHINE_ROLE_COUNTS and report_json.get("input_inventory_row_count") == EXPECTED_INPUT_INVENTORY_ROW_COUNT and report_json.get("family_inventory_row_count") == EXPECTED_FAMILY_INVENTORY_ROW_COUNT and report_json.get("current_machine_role_counts") == EXPECTED_MACHINE_ROLE_COUNTS, "source-pack baseline counts differ from expected"),
        (report_json.get("requirements_csv_row_count") == 5 and report_json.get("requirement_row_count") == 5 and report_json.get("template_row_count") == 5, "template report row counts must be 5"),
        (report_json.get("retained_drain_body_article_numbers") == RETAINED_DRAIN_BODY_ARTICLE_NUMBERS, "retained drain body article numbers differ"),
        (report_json.get("retained_drain_body_template_row_count") == 3 and report_json.get("compatibility_matrix_template_row_count") == 1 and report_json.get("profile_cover_scope_template_row_count") == 1, "template report template area counts differ"),
        (report_json.get("source_pack_profile_cover_count") == 30 and report_json.get("explicit_article_level_compatibility_evidence_count") == 0, "template report compatibility baseline differs"),
        (all(_int(report_json.get(f)) == 0 for f in REPORT_ALLOWED_ZERO_FIELDS + REPORT_ZERO_FIELDS), "generation/mutation/promotion counters must be 0"),
        (report_json.get("production_promotion_blocked") is True and report_json.get("ready_for_benchmark") is False and report_json.get("ready_for_customer_view") is False, "production/readiness report gates differ"),
        (len(evidence_rows) == 5, "evidence CSV must contain exactly 5 rows"), (len(template_rows) == 5, "template CSV must contain exactly 5 rows"),
        (set(REQUIRED_TEMPLATE_IDS).issubset(template_ids), "required template IDs are missing"), (not duplicate_ids, "duplicate template row IDs detected"), (not duplicate_articles, "duplicate retained drain body article rows detected"),
        (not required_missing, "required columns missing"), (not invalid_family and not invalid_template, "invalid family or template row mapping detected"), (not invalid_decision, "invalid manual decisions detected"), (not invalid_evidence, "invalid manual evidence rows detected"), (not invalid_inference, "accepted evidence uses rejected inference wording"),
        (not invalid_blocking and not production_leakage and not readiness_leakage and not diagnostic_leakage, "production/readiness/diagnostic leakage detected"), (all(v == 0 for v in generated.values()), "generated counters must be 0"),
    ]
    for ok, msg in checks:
        if not ok and msg not in errors: errors.append(msg)
    valid = not errors
    ready = valid and complete == 5 and acc_len == 3 and acc_comp == 1 and acc_scope == 1 and all(v == 0 for v in counts.values()) and all(v == 0 for v in generated.values()) and report_json.get("production_promotion_blocked") is True and report_json.get("ready_for_benchmark") is False and report_json.get("ready_for_customer_view") is False
    return {"valid": valid, "errors": errors, "warnings": warnings, "family": family, "source_pack_path": str(source_pack), "evidence_csv_path": str(evidence_csv), "template_report_path": str(template_report), "template_csv_path": str(template_csv), "template_report_valid": report_json.get("valid") is True, "input_inventory_row_count": input_count, "family_inventory_row_count": family_count, "current_machine_role_counts": machine_counts, "evidence_csv_row_count": len(evidence_rows), "template_csv_row_count": len(template_rows), "template_report_row_count": _int(report_json.get("template_row_count")), "template_row_ids": template_ids, "evidence_collection_area_counts": _counts([_clean(r.get("evidence_collection_area")) for r in evidence_rows]), "evidence_target_type_counts": _counts([_clean(r.get("evidence_target_type")) for r in evidence_rows]), "retained_drain_body_article_numbers": [a for a in article_keys if a], "retained_drain_body_row_count": len(article_keys), "compatibility_matrix_row_count": sum(_clean(r.get("evidence_target_type")) == "compatibility_matrix_or_statement" for r in evidence_rows), "profile_cover_scope_row_count": sum(_clean(r.get("evidence_target_type")) == "profile_cover_scope" for r in evidence_rows), "reviewer_decision_counts": _counts([_clean(r.get("reviewer_decision")) for r in evidence_rows]), "row_status_counts": _counts(row_statuses), "evidence_complete_row_count": complete, "evidence_incomplete_row_count": len(evidence_rows) - complete, "evidence_rejected_or_ambiguous_row_count": rejected, "missing_manual_evidence_row_count": missing, "accepted_nominal_length_or_length_independence_count": acc_len, "accepted_compatibility_evidence_count": acc_comp, "accepted_profile_cover_scope_confirmation_count": acc_scope, "safe_to_use_for_future_diagnostic_design_true_count": safe_true, "ready_for_future_diagnostic_design": ready, **{f"{k}_count": v for k, v in counts.items()}, **generated, "duplicate_template_row_ids": duplicate_ids, "duplicate_article_template_rows": duplicate_articles, "required_columns_missing": required_missing, "invalid_family_rows": invalid_family, "invalid_template_rows": invalid_template, "invalid_manual_decision_rows": invalid_decision, "invalid_manual_evidence_rows": invalid_evidence, "invalid_inference_rows": invalid_inference, "invalid_blocking_rows": invalid_blocking, "production_leakage_rows": production_leakage, "readiness_leakage_rows": readiness_leakage, "diagnostic_only_leakage_rows": diagnostic_leakage, "production_promotion_blocked": report_json.get("production_promotion_blocked"), "ready_for_benchmark": report_json.get("ready_for_benchmark"), "ready_for_customer_view": report_json.get("ready_for_customer_view"), "diagnostic_only_note": DIAGNOSTIC_ONLY_NOTE}


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description="Validate read-only TECEdrainprofile explicit compatibility evidence collection CSV.")
    p.add_argument("--evidence-csv", required=True); p.add_argument("--template-report", required=True); p.add_argument("--template-csv", required=True)
    p.add_argument("--source-pack", default=str(DEFAULT_SOURCE_PACK)); p.add_argument("--family", default=EXPECTED_FAMILY); p.add_argument("--json-out", required=True); p.add_argument("--json", action="store_true")
    a = p.parse_args(argv)
    report = validate_evidence_collection_csv(a.evidence_csv, a.template_report, a.template_csv, a.source_pack, a.family)
    write_json_output(report, out=a.json_out)
    if a.json: write_json_output(report)
    return 0 if report["valid"] else 1

if __name__ == "__main__":
    raise SystemExit(main())
