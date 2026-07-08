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
    _bool,
    _clean,
    _counts,
    _duplicates,
)
from tools.tece_report_output import write_json_output  # noqa: E402
from tools.validate_tece_drainprofile_drain_body_length_role_review_csv import EXPECTED_FAMILY  # noqa: E402

CSV_COLUMNS = [
    "design_rule_id", "family", "rule_name", "rule_category", "rule_status", "rule_summary",
    "required_input_artifacts", "required_output_artifacts", "applies_to_articles", "source_pack_mutation_allowed",
    "extraction_logic_change_allowed", "length_overlay_allowed", "compatibility_pairing_allowed",
    "production_promotion_allowed", "benchmark_ready_allowed", "customer_view_allowed", "diagnostic_only",
    "recommended_next_action", "production_status_note",
]
REQUIRED_INPUT_ARTIFACTS = "manual review CSV; role-correction impact report; reviewed role overlay preview CSV/report; reviewed role overlay preview QA CSV/report"
REQUIRED_OUTPUT_ARTIFACTS = "explicit role overlay design CSV; explicit role overlay design JSON report"
RECOMMENDED_NEXT_ACTION = "Use this design report to implement a separate explicit role overlay validator and dry-run diagnostic consumer; do not mutate source-pack or promote production/customer outputs."
PRODUCTION_STATUS_NOTE = "diagnostic-only TECEdrainprofile explicit role overlay design; no source-pack mutation; no extraction logic change; no length overlay; no compatibility pairing; no Products/BOM/Final_Assemblies/Final_Set_Details/customer-view promotion"
DIAGNOSTIC_ONLY_NOTE = "This design report is read-only and diagnostic-only. It defines the proposed TECEdrainprofile explicit non-mutating role overlay workflow design and does not mutate source-pack files, TECE logic, ACO export, Products, Comparison, BOM_Options, Final_Assemblies, or Final_Set_Details. Production promotion remains blocked."
REQUIRED_RULES = [
    ("overlay_artifact_scope", "scope", "required", "Explicit role overlay must be a separate diagnostic artifact, not a source-pack mutation."),
    ("overlay_input_requirements", "input_contract", "required", "Overlay may only be created from validated manual review, impact report, preview, and QA report artifacts."),
    ("overlay_schema_required_columns", "schema", "required", "Overlay schema must include article_number, family, current_source_pack_role, reviewed_role_diagnostic, canonical_role_candidate, overlay_action, evidence_chain_ids, and all blocking flags."),
    ("exact_article_scope_gate", "validation_gate", "required", "TECEdrainprofile pilot overlay scope is restricted to the exact 12 reviewed articles unless a new reviewed QA chain expands it."),
    ("no_source_pack_mutation_gate", "safety_gate", "required", "Overlay consumption must never modify source-pack files or rewrite extracted inventory."),
    ("diagnostic_only_consumption_gate", "safety_gate", "required", "Overlay may only be consumed by diagnostic TECE tools until a separate production policy is explicitly implemented and validated."),
    ("no_length_overlay_gate", "safety_gate", "required", "Reviewed role overlay must not create or imply nominal length values."),
    ("no_compatibility_pairing_gate", "safety_gate", "required", "Reviewed role overlay must not create compatibility pairs or assembly candidates."),
    ("no_customer_or_benchmark_promotion_gate", "safety_gate", "required", "Overlay data must not promote TECE rows to benchmark/customer view."),
    ("source_pack_role_observation_handling", "semantics", "required", "Overlay must preserve distinction between review_csv_article_role, current_source_pack_role, and canonical_role_candidate."),
    ("expected_tecedrainprofile_pilot_mapping", "mapping_contract", "required", "Pilot mapping is 3 drain_body_unresolved_length, 4 spare_part, 3 water_trap, 1 accessory, and 1 profile_cover."),
    ("required_future_apply_validator", "future_work", "required_before_any_apply", "Before any overlay is consumed by broader TECE diagnostics, a separate overlay validator and dry-run consumer must be implemented."),
]


def _read_csv(path: str | Path, errors: list[str]) -> list[dict[str, str]]:
    try:
        with Path(path).open(encoding="utf-8-sig", newline="") as fh:
            return list(csv.DictReader(fh))
    except Exception as exc:
        errors.append(f"QA CSV missing or cannot be loaded: {exc}")
        return []


def _read_json(path: str | Path, errors: list[str]) -> dict[str, Any]:
    try:
        return json.loads(Path(path).read_text(encoding="utf-8"))
    except Exception as exc:
        errors.append(f"QA report missing or cannot be loaded: {exc}")
        return {}


def build_design_rows(family: str = EXPECTED_FAMILY) -> list[dict[str, str]]:
    articles = ";".join(EXPECTED_ARTICLES)
    rows = []
    for index, (name, category, status, summary) in enumerate(REQUIRED_RULES, start=1):
        rows.append({
            "design_rule_id": f"TECE-DP-ROLE-OVERLAY-DESIGN-{index:06d}", "family": family, "rule_name": name,
            "rule_category": category, "rule_status": status, "rule_summary": summary,
            "required_input_artifacts": REQUIRED_INPUT_ARTIFACTS, "required_output_artifacts": REQUIRED_OUTPUT_ARTIFACTS,
            "applies_to_articles": articles, "source_pack_mutation_allowed": "false", "extraction_logic_change_allowed": "false",
            "length_overlay_allowed": "false", "compatibility_pairing_allowed": "false", "production_promotion_allowed": "false",
            "benchmark_ready_allowed": "false", "customer_view_allowed": "false", "diagnostic_only": "true",
            "recommended_next_action": RECOMMENDED_NEXT_ACTION, "production_status_note": PRODUCTION_STATUS_NOTE,
        })
    return rows


def build_design_report(qa_report: str | Path, qa_csv: str | Path, out: str | Path | None = None, family: str = EXPECTED_FAMILY, design_rows: list[dict[str, str]] | None = None) -> dict[str, Any]:
    errors: list[str] = []
    warnings: list[str] = []
    qa = _read_json(qa_report, errors)
    qa_rows = _read_csv(qa_csv, errors)
    rows = list(design_rows) if design_rows is not None else build_design_rows(family)
    qa_report_valid = qa.get("valid") is True
    qa_row_count = qa.get("qa_row_count")
    total_qa_csv_rows = len(qa_rows)
    article_numbers = sorted(_clean(r.get("article_number")) for r in qa_rows if _clean(r.get("article_number")))
    mapping_pass = int((qa.get("mapping_status_counts") or {}).get("pass", 0) or 0)
    blocking_pass = int((qa.get("blocking_status_counts") or {}).get("pass", 0) or 0)
    qa_pass = int((qa.get("qa_status_counts") or {}).get("pass", 0) or 0)

    invalid_family_rows = [r for r in rows if _clean(r.get("family")) != EXPECTED_FAMILY]
    invalid_design_rule_rows = [r for r in rows if any(_clean(r.get(c)) == "" for c in CSV_COLUMNS)]
    duplicate_ids = _duplicates([_clean(r.get("design_rule_id")) for r in rows])
    duplicate_names = _duplicates([_clean(r.get("rule_name")) for r in rows])
    production_leakage_rows = [r for r in rows if _bool(r.get("source_pack_mutation_allowed")) or _bool(r.get("extraction_logic_change_allowed")) or _bool(r.get("compatibility_pairing_allowed")) or _bool(r.get("production_promotion_allowed"))]
    readiness_leakage_rows = [r for r in rows if _bool(r.get("benchmark_ready_allowed")) or _bool(r.get("customer_view_allowed"))]
    diagnostic_only_leakage_rows = [r for r in rows if not _bool(r.get("diagnostic_only"))]

    counts = {name: sum(_bool(r.get(name)) for r in rows) for name in ["source_pack_mutation_allowed", "extraction_logic_change_allowed", "length_overlay_allowed", "compatibility_pairing_allowed", "production_promotion_allowed", "benchmark_ready_allowed", "customer_view_allowed"]}
    production_promotion_blocked = qa.get("production_promotion_blocked") is True
    ready_for_benchmark = qa.get("ready_for_benchmark") is True
    ready_for_customer_view = qa.get("ready_for_customer_view") is True
    checks = [
        (qa_report_valid, "QA report valid must be true"), (family == EXPECTED_FAMILY, "family must be TECEdrainprofile"),
        (total_qa_csv_rows == 12, "QA CSV row count must be 12"), (qa_row_count == 12, "QA report qa_row_count must be 12"),
        (article_numbers == EXPECTED_ARTICLES, "article set differs from expected 12 articles"), (mapping_pass == 12, "mapping_status_counts.pass must be 12"),
        (blocking_pass == 12, "blocking_status_counts.pass must be 12"), (qa_pass == 12, "qa_status_counts.pass must be 12"),
        (qa.get("role_overlay_apply_allowed_count") == 0, "role_overlay_apply_allowed_count must be 0"),
        (qa.get("length_overlay_allowed_count") == 0, "QA report length_overlay_allowed_count must be 0"),
        (qa.get("compatibility_pairing_allowed_count") == 0, "QA report compatibility_pairing_allowed_count must be 0"),
        (qa.get("source_pack_mutation_allowed_count") == 0, "QA report source_pack_mutation_allowed_count must be 0"),
        (qa.get("proposed_source_pack_mutation_count") == 0, "proposed_source_pack_mutation_count must be 0"),
        (qa.get("production_safe_candidate_count") == 0, "production_safe_candidate_count must be 0"),
        (len(rows) >= 12, "design_rule_count must be at least 12"), (not invalid_family_rows, "invalid family rows detected"),
        (not duplicate_ids, "duplicate design_rule_id detected"), (not duplicate_names, "duplicate rule_name detected"),
        (not any(counts.values()), "one or more design rows allow blocked production/readiness behavior"),
        (not diagnostic_only_leakage_rows, "diagnostic_only must be true for every design row"), (production_promotion_blocked, "production_promotion_blocked must be true"),
        (not ready_for_benchmark, "ready_for_benchmark must be false"), (not ready_for_customer_view, "ready_for_customer_view must be false"),
    ]
    for ok, msg in checks:
        if not ok and msg not in errors:
            errors.append(msg)
    if out:
        Path(out).parent.mkdir(parents=True, exist_ok=True)
        with Path(out).open("w", encoding="utf-8-sig", newline="") as fh:
            writer = csv.DictWriter(fh, fieldnames=CSV_COLUMNS); writer.writeheader(); writer.writerows(rows)
    return {
        "valid": not errors, "errors": errors, "warnings": warnings, "family": family, "qa_report_path": str(qa_report), "qa_csv_path": str(qa_csv),
        "qa_report_valid": qa_report_valid, "qa_row_count": qa_row_count, "total_qa_csv_rows": total_qa_csv_rows, "article_numbers": article_numbers,
        "design_rule_count": len(rows), "design_rule_names": [_clean(r.get("rule_name")) for r in rows], "design_rule_category_counts": _counts([_clean(r.get("rule_category")) for r in rows]),
        "design_rule_status_counts": _counts([_clean(r.get("rule_status")) for r in rows]), "required_input_artifacts": REQUIRED_INPUT_ARTIFACTS,
        "required_output_artifacts": REQUIRED_OUTPUT_ARTIFACTS, "pilot_reviewed_role_diagnostic_counts": qa.get("reviewed_role_diagnostic_counts") or _counts(list(EXPECTED_REVIEWED_ROLE_DIAGNOSTIC.values())),
        "pilot_canonical_role_candidate_counts": qa.get("canonical_role_candidate_counts") or _counts(list(EXPECTED_CANONICAL_ROLE_CANDIDATE.values())),
        "pilot_current_source_pack_role_counts": qa.get("current_source_pack_role_counts") or _counts(list(EXPECTED_CURRENT_SOURCE_PACK_ROLE.values())),
        "mapping_status_pass_count": mapping_pass, "blocking_status_pass_count": blocking_pass, "qa_status_pass_count": qa_pass,
        "invalid_family_rows": invalid_family_rows, "invalid_design_rule_rows": invalid_design_rule_rows, "duplicate_design_rule_ids": duplicate_ids,
        "duplicate_rule_names": duplicate_names, "production_leakage_rows": production_leakage_rows, "readiness_leakage_rows": readiness_leakage_rows,
        "diagnostic_only_leakage_rows": diagnostic_only_leakage_rows, **{f"{k}_count": v for k, v in counts.items()},
        "proposed_source_pack_mutation_count": qa.get("proposed_source_pack_mutation_count"), "production_safe_candidate_count": qa.get("production_safe_candidate_count"),
        "production_promotion_blocked": production_promotion_blocked, "ready_for_benchmark": ready_for_benchmark, "ready_for_customer_view": ready_for_customer_view,
        "output_csv_path": str(out) if out else None, "diagnostic_only_note": DIAGNOSTIC_ONLY_NOTE,
    }


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description="Build read-only TECEdrainprofile explicit role overlay design report.")
    p.add_argument("--qa-report", required=True); p.add_argument("--qa-csv", required=True); p.add_argument("--family", default=EXPECTED_FAMILY)
    p.add_argument("--out", required=True); p.add_argument("--json-out", required=True); p.add_argument("--json", action="store_true")
    args = p.parse_args(argv)
    report = build_design_report(args.qa_report, args.qa_csv, out=args.out, family=args.family)
    write_json_output(report, out=args.json_out)
    if args.json: write_json_output(report)
    return 0 if report["valid"] else 1

if __name__ == "__main__":
    raise SystemExit(main())
