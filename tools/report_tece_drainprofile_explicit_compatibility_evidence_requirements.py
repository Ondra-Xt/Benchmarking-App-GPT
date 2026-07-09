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

from tools.report_tece_source_inventory import load_source_pack  # noqa: E402
from tools.tece_report_output import write_json_output  # noqa: E402

DEFAULT_SOURCE_PACK = Path("local_source_packs/tece/pilot_001")
EXPECTED_FAMILY = "TECEdrainprofile"
EXPECTED_INPUT_INVENTORY_ROW_COUNT = 436
EXPECTED_FAMILY_INVENTORY_ROW_COUNT = 76
EXPECTED_MACHINE_ROLE_COUNTS = {"accessory": 5, "complete_set": 4, "drain_body": 12, "profile_cover": 30, "unknown": 25}
EXPECTED_READINESS_AUDIT_AREAS = [
    "role_overlay_chain_validity",
    "source_pack_baseline_stability",
    "reviewed_effective_role_scope",
    "retained_drain_body_length_status",
    "explicit_article_level_compatibility_evidence",
    "compatibility_pair_generation",
    "production_promotion_readiness",
    "recommended_evidence_collection",
]
RETAINED_DRAIN_BODY_ARTICLE_NUMBERS = ["673001", "673002", "673003"]
CSV_COLUMNS = [
    "requirement_id", "family", "requirement_area", "requirement_status", "evidence_needed",
    "affected_article_scope", "affected_article_count", "current_evidence_count", "required_evidence_type",
    "accepted_source_examples", "rejected_inference_methods", "why_required",
    "pairing_design_allowed_after_collection", "compatibility_pair_generation_allowed", "candidate_pair_matrix_allowed",
    "length_overlay_allowed", "source_pack_mutation_allowed", "production_promotion_allowed", "benchmark_ready_allowed",
    "customer_view_allowed", "diagnostic_only", "recommended_next_action", "production_status_note",
]
RECOMMENDED_NEXT_ACTION = "Collect and validate explicit TECEdrainprofile source evidence before designing any diagnostic pairing workflow; do not generate pair rows, mutate source-pack, or promote production/customer outputs."
PRODUCTION_STATUS_NOTE = "diagnostic-only TECEdrainprofile explicit compatibility evidence requirements report; no source-pack mutation; no extraction logic change; no length overlay; no compatibility pair generation; no candidate pair matrix; no Products/BOM/Final_Assemblies/Final_Set_Details/customer-view promotion"
DIAGNOSTIC_ONLY_NOTE = "This evidence requirements report is read-only and diagnostic-only. It defines the explicit TECEdrainprofile compatibility evidence that must be collected before any future diagnostic pairing workflow and confirms that compatibility pair generation, candidate pair matrix generation, length overlay, source-pack mutation, TECE logic change, ACO export change, Products, Comparison, BOM_Options, Final_Assemblies, or Final_Set_Details promotion remain blocked. Production promotion remains blocked."


def _clean(v: Any) -> str:
    return "" if v is None else str(v).strip()


def _bool(v: Any) -> bool:
    if isinstance(v, bool):
        return v
    return _clean(v).lower() in {"1", "true", "yes", "y"}


def _int(v: Any) -> int:
    try:
        return int(v or 0)
    except (TypeError, ValueError):
        return 0


def _counts(vals: list[str]) -> dict[str, int]:
    return dict(sorted(Counter(vals).items()))


def _dups(vals: list[str]) -> list[str]:
    c = Counter(vals)
    return sorted(k for k, v in c.items() if k and v > 1)


def _count_true(rows: list[dict[str, Any]], field: str) -> int:
    return sum(_bool(r.get(field)) for r in rows)


def _read_json(path: str | Path, errors: list[str], label: str) -> dict[str, Any]:
    try:
        return json.loads(Path(path).read_text(encoding="utf-8"))
    except Exception as exc:
        errors.append(f"{label} missing or cannot be loaded: {exc}")
        return {}


def _read_csv(path: str | Path, errors: list[str], label: str) -> list[dict[str, str]]:
    try:
        with Path(path).open(encoding="utf-8-sig", newline="") as fh:
            return list(csv.DictReader(fh))
    except Exception as exc:
        errors.append(f"{label} missing or cannot be loaded: {exc}")
        return []


def _source_pack_counts(source_pack: str | Path, family: str, errors: list[str]) -> tuple[int, int, dict[str, int]]:
    try:
        report = load_source_pack(source_pack)
    except Exception as exc:
        errors.append(f"source-pack cannot be loaded: {exc}")
        return 0, 0, {}
    rows = list(getattr(report, "rows", []) or [])
    family_rows = [r for r in rows if (_clean(getattr(r, "tece_family_candidate", "")) or _clean(getattr(r, "product_family", ""))) == family]
    roles = [_clean(getattr(r, "tece_article_role_candidate", "")) or _clean(getattr(r, "article_role", "")) or "unknown" for r in family_rows]
    return len(rows), len(family_rows), _counts(roles)


def _base_req(family: str, req_id: str, area: str, status: str, evidence_needed: str, scope: str, count: int, current: int, evidence_type: str, rejected: str, why: str) -> dict[str, str]:
    return {
        "requirement_id": req_id, "family": family, "requirement_area": area, "requirement_status": status,
        "evidence_needed": evidence_needed, "affected_article_scope": scope, "affected_article_count": str(count),
        "current_evidence_count": str(current), "required_evidence_type": evidence_type,
        "accepted_source_examples": "TECE catalog; TECE datasheet; explicit TECE article-level compatibility documentation",
        "rejected_inference_methods": rejected, "why_required": why,
        "pairing_design_allowed_after_collection": "false", "compatibility_pair_generation_allowed": "false",
        "candidate_pair_matrix_allowed": "false", "length_overlay_allowed": "false", "source_pack_mutation_allowed": "false",
        "production_promotion_allowed": "false", "benchmark_ready_allowed": "false", "customer_view_allowed": "false",
        "diagnostic_only": "true", "recommended_next_action": RECOMMENDED_NEXT_ACTION, "production_status_note": PRODUCTION_STATUS_NOTE,
    }


def requirement_rows(family: str = EXPECTED_FAMILY) -> list[dict[str, str]]:
    return [
        _base_req(family, "TECE-DP-COMPAT-EVIDENCE-REQ-000001", "retained_drain_body_nominal_length_evidence", "required", "explicit nominal length evidence for retained diagnostic drain bodies 673001, 673002, 673003, or explicit source confirmation that these drain bodies are length-independent", "673001;673002;673003", 3, 0, "explicit_catalog_or_datasheet_nominal_length_or_length_independence_statement", "article-number inference; nearby-row inference; profile-cover length inheritance; DN/water-seal/height inference", "retained drain bodies have unresolved nominal length and cannot support length-governed pairing"),
        _base_req(family, "TECE-DP-COMPAT-EVIDENCE-REQ-000002", "article_level_drain_body_to_profile_cover_compatibility_matrix", "required", "explicit TECEdrainprofile drain-body-to-profile-cover compatibility matrix or article-level compatibility statement", "retained_drain_bodies_and_tecedrainprofile_profile_covers", 33, 0, "explicit_article_level_compatibility_matrix_or_statement", "same family inference; same length inference; section proximity inference; generic profile-cover compatibility assumption", "post-overlay readiness audit found explicit_article_level_compatibility_evidence_count=0"),
        _base_req(family, "TECE-DP-COMPAT-EVIDENCE-REQ-000003", "profile_cover_scope_confirmation", "required", "confirm the 30 source-pack TECEdrainprofile profile_cover rows are complete and are the correct cover scope for compatibility evidence collection", "tecedrainprofile_profile_cover_rows", 30, 30, "source_pack_scope_confirmation_against_catalog_section", "generated pair matrix; production promotion; inferred compatibility", "profile-cover scope is needed before any future evidence matrix can be evaluated"),
        _base_req(family, "TECE-DP-COMPAT-EVIDENCE-REQ-000004", "explicit_pairing_blocker_resolution", "blocked_until_evidence_collected", "resolve unresolved length evidence and explicit compatibility evidence before any diagnostic pairing workflow", "readiness_audit_blockers", 2, 0, "combined_blocker_resolution_evidence", "dry-run role overlay evidence as pairing evidence; compatibility generated from role mapping only", "role overlay chain is diagnostic role evidence only and cannot authorize compatibility pairing"),
        _base_req(family, "TECE-DP-COMPAT-EVIDENCE-REQ-000005", "future_pairing_design_entry_gate", "required_before_future_design", "a future diagnostic pairing design may only be started after requirements 000001 and 000002 are satisfied and separately validated", "future_workflow_gate", 0, 0, "validated_evidence_requirements_report", "direct pair generation from this requirements report", "this report is requirements-only and must not be treated as pairing output"),
    ]


def build_evidence_requirements_report(readiness_report: str | Path, readiness_csv: str | Path, source_pack: str | Path = DEFAULT_SOURCE_PACK, family: str = EXPECTED_FAMILY, out: str | Path | None = None, rows: list[dict[str, str]] | None = None) -> tuple[list[dict[str, str]], dict[str, Any]]:
    errors: list[str] = []; warnings: list[str] = []
    readiness = _read_json(readiness_report, errors, "readiness report")
    readiness_rows = _read_csv(readiness_csv, errors, "readiness CSV")
    input_count, family_count, machine_counts = _source_pack_counts(source_pack, family, errors)
    req_rows = list(rows) if rows is not None else requirement_rows(family)

    expected_ids = [f"TECE-DP-COMPAT-EVIDENCE-REQ-{i:06d}" for i in range(1, 6)]
    expected_areas = [r["requirement_area"] for r in requirement_rows(EXPECTED_FAMILY)]
    req_ids = [_clean(r.get("requirement_id")) for r in req_rows]
    req_areas = [_clean(r.get("requirement_area")) for r in req_rows]
    readiness_areas = sorted(_clean(r.get("audit_area")) for r in readiness_rows)

    invalid_family_rows = [r.get("requirement_id", str(i)) for i, r in enumerate(req_rows, 1) if _clean(r.get("family")) != EXPECTED_FAMILY]
    invalid_requirement_rows = [r.get("requirement_id", str(i)) for i, r in enumerate(req_rows, 1) if _clean(r.get("requirement_id")) not in expected_ids or _clean(r.get("requirement_area")) not in expected_areas]
    invalid_blocking_rows = [r.get("requirement_id", str(i)) for i, r in enumerate(req_rows, 1) if _clean(r.get("requirement_status")) == "blocked_until_evidence_collected" and not _clean(r.get("evidence_needed"))]
    production_fields = ["pairing_design_allowed_after_collection", "compatibility_pair_generation_allowed", "candidate_pair_matrix_allowed", "length_overlay_allowed", "source_pack_mutation_allowed", "production_promotion_allowed"]
    readiness_fields = ["benchmark_ready_allowed", "customer_view_allowed"]
    production_leakage_rows = [r.get("requirement_id", str(i)) for i, r in enumerate(req_rows, 1) if any(_bool(r.get(f)) for f in production_fields)]
    readiness_leakage_rows = [r.get("requirement_id", str(i)) for i, r in enumerate(req_rows, 1) if any(_bool(r.get(f)) for f in readiness_fields)]
    diagnostic_only_leakage_rows = [r.get("requirement_id", str(i)) for i, r in enumerate(req_rows, 1) if not _bool(r.get("diagnostic_only"))]
    duplicate_ids = _dups(req_ids)

    generated_pairs = _int(readiness.get("generated_compatibility_pair_count"))
    generated_matrix = _int(readiness.get("generated_candidate_pair_matrix_row_count"))
    generated_length_overlay = _int(readiness.get("generated_length_overlay_row_count"))
    proposed_mutations = _int(readiness.get("proposed_source_pack_mutation_count"))
    production_safe = _int(readiness.get("production_safe_candidate_count"))
    source_pack_baseline_valid = input_count == EXPECTED_INPUT_INVENTORY_ROW_COUNT and family_count == EXPECTED_FAMILY_INVENTORY_ROW_COUNT and machine_counts == EXPECTED_MACHINE_ROLE_COUNTS

    checks = [
        (readiness.get("valid") is True, "readiness report valid must be true"),
        (family == EXPECTED_FAMILY and readiness.get("family") == EXPECTED_FAMILY, "family must be TECEdrainprofile"),
        (source_pack_baseline_valid, "source-pack baseline counts differ from expected"),
        (len(readiness_rows) == 8 and readiness_areas == sorted(EXPECTED_READINESS_AUDIT_AREAS), "readiness CSV must contain exactly the expected 8 audit areas"),
        (_int(readiness.get("audit_row_count")) == 8, "readiness report audit_row_count must be 8"),
        (readiness.get("role_overlay_chain_valid") is True, "role_overlay_chain_valid must be true"),
        (readiness.get("source_pack_baseline_valid") is True, "source_pack_baseline_valid must be true"),
        (_int(readiness.get("reviewed_effective_drain_body_count")) == 3, "reviewed_effective_drain_body_count must be 3"),
        (_int(readiness.get("reviewed_effective_accessory_count")) == 8, "reviewed_effective_accessory_count must be 8"),
        (_int(readiness.get("reviewed_effective_profile_cover_count")) == 1, "reviewed_effective_profile_cover_count must be 1"),
        (_int(readiness.get("retained_unresolved_length_count")) == 3, "retained_unresolved_length_count must be 3"),
        (_int(readiness.get("diagnostic_reclassified_from_unknown_count")) == 9, "diagnostic_reclassified_from_unknown_count must be 9"),
        (_int(readiness.get("source_pack_profile_cover_count")) == 30, "source_pack_profile_cover_count must be 30"),
        (_int(readiness.get("explicit_article_level_compatibility_evidence_count")) == 0, "explicit_article_level_compatibility_evidence_count must be 0"),
        (len(req_rows) == 5, "requirement_row_count must be 5"),
        (set(expected_ids).issubset(req_ids), "required requirement IDs are missing"),
        (set(expected_areas).issubset(req_areas), "required requirement areas are missing"),
        (not duplicate_ids, "duplicate requirement IDs detected"),
        (not production_leakage_rows and not readiness_leakage_rows and not diagnostic_only_leakage_rows, "production/readiness/diagnostic leakage detected"),
        (generated_pairs == 0, "generated_compatibility_pair_count must be 0"),
        (generated_matrix == 0, "generated_candidate_pair_matrix_row_count must be 0"),
        (generated_length_overlay == 0, "generated_length_overlay_row_count must be 0"),
        (proposed_mutations == 0, "proposed_source_pack_mutation_count must be 0"),
        (production_safe == 0, "production_safe_candidate_count must be 0"),
        (readiness.get("production_promotion_blocked") is True, "production_promotion_blocked must be true"),
        (readiness.get("ready_for_benchmark") is False, "ready_for_benchmark must be false"),
        (readiness.get("ready_for_customer_view") is False, "ready_for_customer_view must be false"),
        (not invalid_family_rows and not invalid_requirement_rows and not invalid_blocking_rows, "invalid requirement rows detected"),
    ]
    for ok, msg in checks:
        if not ok and msg not in errors:
            errors.append(msg)

    if out:
        Path(out).parent.mkdir(parents=True, exist_ok=True)
        with Path(out).open("w", encoding="utf-8-sig", newline="") as fh:
            writer = csv.DictWriter(fh, fieldnames=CSV_COLUMNS)
            writer.writeheader(); writer.writerows(req_rows)

    report = {
        "valid": not errors, "errors": errors, "warnings": warnings, "family": family,
        "source_pack_path": str(source_pack), "readiness_report_path": str(readiness_report), "readiness_csv_path": str(readiness_csv),
        "readiness_report_valid": readiness.get("valid") is True,
        "input_inventory_row_count": input_count, "family_inventory_row_count": family_count, "current_machine_role_counts": machine_counts,
        "readiness_audit_row_count": _int(readiness.get("audit_row_count")), "requirement_row_count": len(req_rows),
        "requirement_ids": req_ids, "requirement_area_counts": _counts(req_areas), "requirement_status_counts": _counts([_clean(r.get("requirement_status")) for r in req_rows]),
        "retained_drain_body_article_numbers": RETAINED_DRAIN_BODY_ARTICLE_NUMBERS, "retained_drain_body_article_count": len(RETAINED_DRAIN_BODY_ARTICLE_NUMBERS),
        "source_pack_profile_cover_count": _int(readiness.get("source_pack_profile_cover_count")),
        "reviewed_effective_drain_body_count": _int(readiness.get("reviewed_effective_drain_body_count")),
        "reviewed_effective_accessory_count": _int(readiness.get("reviewed_effective_accessory_count")),
        "reviewed_effective_profile_cover_count": _int(readiness.get("reviewed_effective_profile_cover_count")),
        "retained_unresolved_length_count": _int(readiness.get("retained_unresolved_length_count")),
        "diagnostic_reclassified_from_unknown_count": _int(readiness.get("diagnostic_reclassified_from_unknown_count")),
        "explicit_article_level_compatibility_evidence_count": _int(readiness.get("explicit_article_level_compatibility_evidence_count")),
        "current_evidence_total_count": sum(_int(r.get("current_evidence_count")) for r in req_rows),
        "required_evidence_area_count": len(set(req_areas)),
        "pairing_design_allowed_after_collection_count": _count_true(req_rows, "pairing_design_allowed_after_collection"),
        "compatibility_pair_generation_allowed_count": _count_true(req_rows, "compatibility_pair_generation_allowed"),
        "candidate_pair_matrix_allowed_count": _count_true(req_rows, "candidate_pair_matrix_allowed"),
        "length_overlay_allowed_count": _count_true(req_rows, "length_overlay_allowed"),
        "source_pack_mutation_allowed_count": _count_true(req_rows, "source_pack_mutation_allowed"),
        "production_promotion_allowed_count": _count_true(req_rows, "production_promotion_allowed"),
        "benchmark_ready_allowed_count": _count_true(req_rows, "benchmark_ready_allowed"),
        "customer_view_allowed_count": _count_true(req_rows, "customer_view_allowed"),
        "generated_compatibility_pair_count": generated_pairs, "generated_candidate_pair_matrix_row_count": generated_matrix,
        "generated_length_overlay_row_count": generated_length_overlay, "proposed_source_pack_mutation_count": proposed_mutations,
        "production_safe_candidate_count": production_safe, "duplicate_requirement_ids": duplicate_ids,
        "invalid_family_rows": invalid_family_rows, "invalid_requirement_rows": invalid_requirement_rows, "invalid_blocking_rows": invalid_blocking_rows,
        "production_leakage_rows": production_leakage_rows, "readiness_leakage_rows": readiness_leakage_rows,
        "diagnostic_only_leakage_rows": diagnostic_only_leakage_rows,
        "production_promotion_blocked": readiness.get("production_promotion_blocked"), "ready_for_benchmark": readiness.get("ready_for_benchmark"),
        "ready_for_customer_view": readiness.get("ready_for_customer_view"), "output_csv_path": str(out) if out else None,
        "diagnostic_only_note": DIAGNOSTIC_ONLY_NOTE,
    }
    return req_rows, report


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Build read-only TECEdrainprofile explicit compatibility evidence requirements report.")
    parser.add_argument("--readiness-report", required=True)
    parser.add_argument("--readiness-csv", required=True)
    parser.add_argument("--source-pack", default=str(DEFAULT_SOURCE_PACK))
    parser.add_argument("--family", default=EXPECTED_FAMILY)
    parser.add_argument("--out", required=True)
    parser.add_argument("--json-out", required=True)
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args(argv)
    _rows, report = build_evidence_requirements_report(args.readiness_report, args.readiness_csv, args.source_pack, args.family, args.out)
    write_json_output(report, out=args.json_out)
    if args.json:
        write_json_output(report)
    return 0 if report["valid"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
