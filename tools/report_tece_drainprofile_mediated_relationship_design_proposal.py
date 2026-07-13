from __future__ import annotations

import argparse, csv, json, sys
from collections import Counter
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from tools.tece_report_output import write_json_output  # noqa: E402
from tools import (
    validate_tece_drainprofile_mediated_relationship_evidence_template_v2_csv as validator,
)  # noqa: E402

DEFAULT_SOURCE_PACK = validator.DEFAULT_SOURCE_PACK
EXPECTED_FAMILY = validator.EXPECTED_FAMILY
RETAINED_DRAIN_BODY_ARTICLES = validator.RETAINED_DRAIN_BODY_ARTICLES
PROPOSED_PROFILE_COVER_ARTICLES = validator.PROPOSED_PROFILE_COVER_ARTICLES
INTERMEDIATE_SYSTEM_OBJECT = "TECEdrainprofile Duschprofil"
EXPECTED_RETAINED_DRAIN_BODY_ARTICLES = ["673001", "673002", "673003"]
EXPECTED_PROPOSED_PROFILE_COVER_ARTICLES = [
    "675000",
    "675001",
    "675010",
    "675011",
    "675012",
    "675013",
    "675014",
    "675015",
    "675019",
    "675024",
    "675025",
]
EXPECTED_INTERMEDIATE_SYSTEM_OBJECT = "TECEdrainprofile Duschprofil"
ROW_ID_PREFIX = "TECE-DP-MEDIATED-RELATIONSHIP-DESIGN-PROPOSAL"
EXPECTED_ROW_IDS = [f"{ROW_ID_PREFIX}-{i:06d}" for i in range(1, 8)]
DIAGNOSTIC_ONLY_NOTE = "This TECEdrainprofile mediated relationship design proposal is read-only and diagnostic-only. It documents a future diagnostic model shape in which retained TECEdrainprofile drain-body articles relate to TECEdrainprofile Duschprofil as an intermediate system object, and TECEdrainprofile profile-cover articles relate to that intermediate system object. It rejects direct drain-body-to-profile-cover matrix generation and does not generate direct or mediated compatibility pairs, generate candidate pair matrices, create length overlays, mutate source-pack files, change TECE logic, change ACO export, or promote Products, Comparison, BOM_Options, Final_Assemblies, or Final_Set_Details. Production promotion remains blocked."

CSV_COLUMNS = [
    "proposal_row_id",
    "family",
    "proposal_area",
    "proposal_status",
    "diagnostic_model_node",
    "source_evidence_scope",
    "retained_drain_body_articles",
    "intermediate_system_object",
    "proposed_profile_cover_articles",
    "relationship_model",
    "direct_pair_generation_allowed",
    "mediated_pair_generation_allowed",
    "candidate_matrix_generation_allowed",
    "source_pack_mutation_allowed",
    "extraction_logic_change_allowed",
    "role_overlay_allowed",
    "length_overlay_allowed",
    "production_promotion_allowed",
    "benchmark_ready_allowed",
    "customer_view_allowed",
    "diagnostic_only",
    "recommended_next_action",
    "proposal_note",
    "production_status_note",
]
FALSE_ALLOWED_FIELDS = [
    "direct_pair_generation_allowed",
    "mediated_pair_generation_allowed",
    "candidate_matrix_generation_allowed",
    "source_pack_mutation_allowed",
    "extraction_logic_change_allowed",
    "role_overlay_allowed",
    "length_overlay_allowed",
    "production_promotion_allowed",
    "benchmark_ready_allowed",
    "customer_view_allowed",
]
GENERATED_COUNTERS = [
    "generated_direct_pair_count",
    "generated_mediated_pair_count",
    "generated_candidate_matrix_row_count",
    "generated_length_overlay_row_count",
    "proposed_source_pack_mutation_count",
]


def _clean(v: Any) -> str:
    return "" if v is None else str(v).strip()


def _bool(v: Any) -> bool:
    return v is True or _clean(v).lower() in {"1", "true", "yes", "y"}


def _counts(vals) -> dict[str, int]:
    return dict(sorted(Counter(vals).items()))


def _read_json(path, label, errors):
    try:
        return json.loads(Path(path).read_text(encoding="utf-8"))
    except Exception as exc:
        errors.append(f"{label} missing or cannot be loaded: {exc}")
        return {}


def _read_csv(path, label, errors):
    try:
        with Path(path).open(encoding="utf-8-sig", newline="") as fh:
            return list(csv.DictReader(fh))
    except Exception as exc:
        errors.append(f"{label} missing or cannot be loaded: {exc}")
        return []


def _csv_list(items: list[str]) -> str:
    return ",".join(items)


def build_design_proposal_rows(family: str = EXPECTED_FAMILY) -> list[dict[str, str]]:
    specs = [
        (
            "drain_body_to_duschprofil_evidence_scope",
            "proposed_diagnostic_model_scope_confirmed",
            "retained_drain_body_articles",
            "reviewed_template_v2_evidence",
            "673001,673002,673003",
            "",
            "",
            "drain_body_to_intermediate_system_object",
        ),
        (
            "intermediate_duschprofil_system_object",
            "proposed_diagnostic_model_scope_confirmed",
            "TECEdrainprofile_Duschprofil",
            "reviewed_template_v2_evidence",
            "",
            INTERMEDIATE_SYSTEM_OBJECT,
            "",
            "intermediate_system_object_required",
        ),
        (
            "duschprofil_to_profile_cover_evidence_scope",
            "proposed_diagnostic_model_scope_confirmed",
            "proposed_profile_cover_articles",
            "reviewed_template_v2_evidence",
            "",
            "",
            _csv_list(PROPOSED_PROFILE_COVER_ARTICLES),
            "intermediate_system_object_to_profile_cover_scope",
        ),
        (
            "mediated_relationship_topology",
            "proposed_diagnostic_model_ready_for_future_design_only",
            "mediated_relationship_topology",
            "reviewed_template_v2_evidence",
            "",
            INTERMEDIATE_SYSTEM_OBJECT,
            "",
            "drain_body -> TECEdrainprofile Duschprofil -> profile_cover",
        ),
        (
            "direct_pairing_model_rejection",
            "direct_pairing_model_not_supported_by_reviewed_evidence",
            "direct_pairing_model_rejection",
            "reviewed_template_v2_evidence",
            "",
            "",
            "",
            "direct drain_body -> profile_cover matrix rejected",
        ),
        (
            "future_diagnostic_consumer_requirements",
            "requirements_defined_generation_blocked",
            "future_diagnostic_consumer_requirements",
            "reviewed_template_v2_evidence",
            "",
            "",
            "",
            "future consumer may use mediated topology only after separate implementation/validation",
        ),
        (
            "safety_and_promotion_blockers",
            "production_promotion_blocked",
            "safety_and_promotion_blockers",
            "reviewed_template_v2_evidence",
            "",
            "",
            "",
            "diagnostic-only, no Products/Comparison/BOM/Final promotion",
        ),
    ]
    rows = []
    for i, (area, status, node, scope, drains, inter, covers, rel) in enumerate(
        specs, 1
    ):
        r = {
            "proposal_row_id": f"{ROW_ID_PREFIX}-{i:06d}",
            "family": family,
            "proposal_area": area,
            "proposal_status": status,
            "diagnostic_model_node": node,
            "source_evidence_scope": scope,
            "retained_drain_body_articles": drains,
            "intermediate_system_object": inter,
            "proposed_profile_cover_articles": covers,
            "relationship_model": rel,
            "diagnostic_only": "true",
            "recommended_next_action": "separate future diagnostic implementation and validation required before any consumer use",
            "proposal_note": "Read-only diagnostic-only future design proposal; generation remains blocked.",
            "production_status_note": "No Products, Comparison, BOM_Options, Final_Assemblies, or Final_Set_Details promotion.",
        }
        for f in FALSE_ALLOWED_FIELDS:
            r[f] = "false"
        rows.append(r)
    return rows


def _write_csv(path, rows):
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    with Path(path).open("w", encoding="utf-8", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=CSV_COLUMNS, extrasaction="ignore")
        w.writeheader()
        w.writerows(rows)


def generate_report(
    qa_report,
    qa_csv,
    validation_report,
    evidence_csv,
    template_v2_report,
    source_pack=DEFAULT_SOURCE_PACK,
    family=EXPECTED_FAMILY,
):
    errors = []
    warnings = []
    qa = _read_json(qa_report, "QA report", errors)
    validation = _read_json(validation_report, "validation report", errors)
    template = _read_json(template_v2_report, "template v2 report", errors)
    qa_rows = _read_csv(qa_csv, "QA CSV", errors)
    evidence_rows = _read_csv(evidence_csv, "evidence CSV", errors)
    proposal_rows = build_design_proposal_rows(family)
    baseline = validator.validate(evidence_csv, template_v2_report, source_pack, family)
    for e in baseline.get("errors", []):
        if e not in errors:
            errors.append(e)
    qa_report_valid = qa.get("valid") is True
    validation_report_valid = validation.get("valid") is True
    template_v2_report_valid = template.get("valid") is True
    if not qa_report_valid:
        errors.append("QA report valid must be true")
    if not validation_report_valid:
        errors.append("validation report valid must be true")
    if not template_v2_report_valid:
        errors.append("template v2 report valid must be true")
    ids = [_clean(r.get("proposal_row_id")) for r in proposal_rows]
    report = {
        "valid": False,
        "errors": errors,
        "warnings": warnings,
        "family": family,
        "source_pack_path": str(source_pack),
        "qa_report_path": str(qa_report),
        "qa_csv_path": str(qa_csv),
        "validation_report_path": str(validation_report),
        "evidence_csv_path": str(evidence_csv),
        "template_v2_report_path": str(template_v2_report),
        "qa_report_valid": qa_report_valid,
        "validation_report_valid": validation_report_valid,
        "template_v2_report_valid": template_v2_report_valid,
        "input_inventory_row_count": baseline.get("input_inventory_row_count", 0),
        "family_inventory_row_count": baseline.get("family_inventory_row_count", 0),
        "current_machine_role_counts": baseline.get("current_machine_role_counts", {}),
        "evidence_csv_row_count": len(evidence_rows),
        "template_v2_row_count": int(baseline.get("template_v2_row_count", 0) or 0),
        "qa_row_count": len(qa_rows),
        "design_proposal_row_count": len(proposal_rows),
        "design_proposal_row_ids": ids,
        "proposal_area_counts": _counts(
            _clean(r.get("proposal_area")) for r in proposal_rows
        ),
        "proposal_status_counts": _counts(
            _clean(r.get("proposal_status")) for r in proposal_rows
        ),
        "retained_drain_body_articles": RETAINED_DRAIN_BODY_ARTICLES,
        "proposed_profile_cover_articles": PROPOSED_PROFILE_COVER_ARTICLES,
        "intermediate_system_object": INTERMEDIATE_SYSTEM_OBJECT,
        "mediated_relationship_model_proposed": any(
            "TECEdrainprofile Duschprofil -> profile_cover"
            in r.get("relationship_model", "")
            for r in proposal_rows
        ),
        "direct_pairing_model_rejected": any(
            "matrix rejected" in r.get("relationship_model", "") for r in proposal_rows
        ),
        "future_diagnostic_design_ready": baseline.get(
            "ready_for_future_diagnostic_design"
        )
        is True
        and qa.get("future_diagnostic_design_ready_count", 15) == 15,
        "future_diagnostic_consumer_required": any(
            r.get("proposal_area") == "future_diagnostic_consumer_requirements"
            for r in proposal_rows
        ),
        "accepted_mediated_evidence_row_count": baseline.get(
            "accepted_mediated_evidence_row_count", 0
        ),
        "reviewed_evidence_complete_row_count": baseline.get(
            "reviewed_evidence_complete_row_count", 0
        ),
        "reviewed_evidence_incomplete_row_count": baseline.get(
            "reviewed_evidence_incomplete_row_count", 0
        ),
        "safe_to_use_for_future_diagnostic_design_true_count": baseline.get(
            "safe_to_use_for_future_diagnostic_design_true_count", 0
        ),
        "invalid_direct_pairing_claim_rows": baseline.get(
            "invalid_direct_pairing_claim_rows", []
        ),
        "invalid_direct_pairing_claim_details": baseline.get(
            "invalid_direct_pairing_claim_details", []
        ),
        "production_safe_candidate_count": int(
            template.get(
                "production_safe_candidate_count",
                baseline.get("production_safe_candidate_count", 0),
            )
            or 0
        ),
        "production_promotion_blocked": True,
        "ready_for_benchmark": False,
        "ready_for_customer_view": False,
        "diagnostic_only_note": DIAGNOSTIC_ONLY_NOTE,
    }
    for f in FALSE_ALLOWED_FIELDS:
        report[f"{f}_count"] = sum(_bool(r.get(f)) for r in proposal_rows)
    for c in GENERATED_COUNTERS:
        report[c] = int(template.get(c, baseline.get(c, 0)) or 0)
    validate_design_proposal_report(
        report, proposal_rows, evidence_rows, qa_rows, family, errors
    )
    return report, proposal_rows


def validate_design_proposal_report(
    report: dict[str, Any],
    proposal_rows: list[dict[str, str]],
    evidence_rows: list[dict[str, str]],
    qa_rows: list[dict[str, str]],
    family: str,
    errors: list[str] | None = None,
) -> dict[str, Any]:
    """Apply hard gates to an already assembled design-proposal report.

    This small validation seam keeps fixed safety gates independently testable without
    adding any input that could enable generation, mutation, or promotion.
    """

    errors = errors if errors is not None else list(report.get("errors", []))
    ids = [_clean(r.get("proposal_row_id")) for r in proposal_rows]
    checks = [
        (family == EXPECTED_FAMILY, "family must be TECEdrainprofile"),
        (
            report["input_inventory_row_count"] == 436
            and report["family_inventory_row_count"] == 76
            and report["current_machine_role_counts"]
            == validator.EXPECTED_MACHINE_ROLE_COUNTS,
            "source-pack baseline counts differ from expected",
        ),
        (len(evidence_rows) == 15, "evidence_csv_row_count must be 15"),
        (report["template_v2_row_count"] == 15, "template_v2_row_count must be 15"),
        (len(qa_rows) == 15, "qa_row_count must be 15"),
        (len(proposal_rows) == 7, "design_proposal_row_count must be 7"),
        (
            ids == EXPECTED_ROW_IDS and len(set(ids)) == 7,
            "design proposal row IDs must be deterministic and unique",
        ),
        (
            report["retained_drain_body_articles"]
            == EXPECTED_RETAINED_DRAIN_BODY_ARTICLES,
            "retained drain bodies must match expected",
        ),
        (
            report["proposed_profile_cover_articles"]
            == EXPECTED_PROPOSED_PROFILE_COVER_ARTICLES,
            "proposed profile covers must match expected",
        ),
        (
            report["intermediate_system_object"] == EXPECTED_INTERMEDIATE_SYSTEM_OBJECT,
            "intermediate_system_object must be TECEdrainprofile Duschprofil",
        ),
        (
            report["mediated_relationship_model_proposed"] is True,
            "mediated_relationship_model_proposed must be true",
        ),
        (
            report["direct_pairing_model_rejected"] is True,
            "direct_pairing_model_rejected must be true",
        ),
        (
            report["accepted_mediated_evidence_row_count"] == 15,
            "accepted_mediated_evidence_row_count must be 15",
        ),
        (
            report["reviewed_evidence_complete_row_count"] == 15,
            "reviewed_evidence_complete_row_count must be 15",
        ),
        (
            report["reviewed_evidence_incomplete_row_count"] == 0,
            "reviewed_evidence_incomplete_row_count must be 0",
        ),
        (
            report["safe_to_use_for_future_diagnostic_design_true_count"] == 15,
            "safe_to_use_for_future_diagnostic_design_true_count must be 15",
        ),
        (
            not report["invalid_direct_pairing_claim_rows"]
            and not report["invalid_direct_pairing_claim_details"],
            "invalid direct pairing claims must be empty",
        ),
        (
            report["production_safe_candidate_count"] == 0,
            "production_safe_candidate_count must be 0",
        ),
        (
            report["production_promotion_blocked"] is True,
            "production_promotion_blocked must be true",
        ),
        (report["ready_for_benchmark"] is False, "ready_for_benchmark must be false"),
        (
            report["ready_for_customer_view"] is False,
            "ready_for_customer_view must be false",
        ),
        (
            all(_bool(r.get("diagnostic_only")) for r in proposal_rows),
            "diagnostic_only must be true for all proposal rows",
        ),
    ]
    for f in FALSE_ALLOWED_FIELDS:
        checks.append((report[f"{f}_count"] == 0, f"{f}_count must be 0"))
    for c in GENERATED_COUNTERS:
        checks.append((report[c] == 0, f"{c} must be 0"))
    for ok, msg in checks:
        if not ok and msg not in errors:
            errors.append(msg)
    report["errors"] = errors
    report["valid"] = not errors
    return report


def main(argv=None):
    p = argparse.ArgumentParser(
        description="Create read-only diagnostic-only TECEdrainprofile mediated relationship design proposal report."
    )
    p.add_argument("--qa-report", required=True)
    p.add_argument("--qa-csv", required=True)
    p.add_argument("--validation-report", required=True)
    p.add_argument("--evidence-csv", required=True)
    p.add_argument("--template-v2-report", required=True)
    p.add_argument("--source-pack", default=str(DEFAULT_SOURCE_PACK))
    p.add_argument("--family", default=EXPECTED_FAMILY)
    p.add_argument("--out", required=True)
    p.add_argument("--json-out", required=True)
    p.add_argument("--json", action="store_true")
    a = p.parse_args(argv)
    report, rows = generate_report(
        a.qa_report,
        a.qa_csv,
        a.validation_report,
        a.evidence_csv,
        a.template_v2_report,
        a.source_pack,
        a.family,
    )
    _write_csv(a.out, rows)
    write_json_output(report, out=a.json_out)
    if a.json:
        write_json_output(report)
    return 0 if report["valid"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
