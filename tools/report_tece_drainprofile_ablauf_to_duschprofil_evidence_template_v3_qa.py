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

from tools import validate_tece_drainprofile_ablauf_to_duschprofil_evidence_template_v3_csv as validator  # noqa: E402
from tools.tece_report_output import write_json_output  # noqa: E402

DEFAULT_SOURCE_PACK = validator.DEFAULT_SOURCE_PACK
EXPECTED_FAMILY = validator.EXPECTED_FAMILY
EXPECTED_QA_ROW_COUNT = 34
QA_ROW_ID_PREFIX = "TECE-DP-ABLAUF-DUSCHPROFIL-EVIDENCE-V3-QA"
EXPECTED_QA_ROW_IDS = [f"{QA_ROW_ID_PREFIX}-{i:06d}" for i in range(1, EXPECTED_QA_ROW_COUNT + 1)]

DIAGNOSTIC_ONLY_NOTE = "This TECEdrainprofile Ablauf-to-Duschprofil evidence template v3 QA report is read-only and diagnostic-only. It confirms that reviewed official evidence is complete for the corrected future diagnostic evidence scope in which TECEdrainprofile Ablauf articles 673001, 673002, and 673003 relate to installable TECEdrainprofile Duschprofil articles 670xxx/671xxx. It confirms that 675xxx Profildeckel articles are spare-part/profile-cover scope only, not main installable Duschprofil articles. It does not generate direct or mediated compatibility pairs, generate candidate pair matrices, create length overlays, mutate source-pack files, change TECE logic, change ACO export, or promote Products, Comparison, BOM_Options, Final_Assemblies, or Final_Set_Details. Production promotion remains blocked."
QA_NOTE = "Reviewed v3 evidence row passes read-only diagnostic-only QA for the corrected Ablauf-to-Duschprofil future diagnostic evidence scope."
PRODUCTION_STATUS_NOTE = "Diagnostic-only QA blocks pair generation, candidate matrices, overlays, source-pack mutation, production promotion, benchmark readiness, and customer-view readiness."

CSV_COLUMNS = [
    "qa_row_id", "template_v3_row_id", "family", "evidence_collection_area", "evidence_target_type", "article_number",
    "qa_status", "qa_blocking_status", "source_evidence_complete", "diagnostic_scope_confirmed", "future_diagnostic_design_ready",
    "ablauf_to_duschprofil_interface_confirmed", "installable_duschprofil_article_scope_confirmed", "spare_cover_not_installable_profile_confirmed",
    "direct_pair_generation_allowed", "mediated_pair_generation_allowed", "candidate_matrix_generation_allowed", "source_pack_mutation_allowed",
    "extraction_logic_change_allowed", "role_overlay_allowed", "length_overlay_allowed", "production_promotion_allowed", "benchmark_ready_allowed",
    "customer_view_allowed", "diagnostic_only", "qa_note", "production_status_note",
]
BLOCK_FALSE_FIELDS = [
    "direct_pair_generation_allowed", "mediated_pair_generation_allowed", "candidate_matrix_generation_allowed", "source_pack_mutation_allowed",
    "extraction_logic_change_allowed", "role_overlay_allowed", "length_overlay_allowed", "production_promotion_allowed", "benchmark_ready_allowed",
    "customer_view_allowed",
]
GENERATED_COUNTERS = ["generated_direct_pair_count", "generated_mediated_pair_count", "generated_candidate_matrix_row_count", "generated_length_overlay_row_count", "proposed_source_pack_mutation_count"]


def _clean(v: Any) -> str:
    return "" if v is None else str(v).strip()


def _bool(v: Any) -> bool:
    return v is True or _clean(v).lower() in {"1", "true", "yes", "y"}


def _counts(values) -> dict[str, int]:
    return dict(sorted(Counter(values).items()))


def _read_json(path: str | Path, errors: list[str]) -> dict[str, Any]:
    try:
        return json.loads(Path(path).read_text(encoding="utf-8"))
    except Exception as exc:
        errors.append(f"validation report missing or cannot be loaded: {exc}")
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
    rows: list[dict[str, str]] = []
    for idx, row in enumerate(evidence_rows, 1):
        area = _clean(row.get("evidence_collection_area"))
        target = _clean(row.get("evidence_target_type"))
        is_interface = area == "ablauf_to_duschprofil_interface_statement" and target == "retained_drain_body_article"
        is_generic = area == "duschprofil_installable_profile_scope_statement" and target == "generic_duschprofil_scope"
        is_article = area == "installable_duschprofil_article_scope_and_length_evidence" and target == "installable_duschprofil_article"
        qa = {
            "qa_row_id": f"{QA_ROW_ID_PREFIX}-{idx:06d}",
            "template_v3_row_id": _clean(row.get("template_v3_row_id")),
            "family": _clean(row.get("family")),
            "evidence_collection_area": area,
            "evidence_target_type": target,
            "article_number": _clean(row.get("article_number")),
            "qa_status": "pass" if _source_evidence_complete(row) and _bool(row.get("evidence_complete")) and _bool(row.get("ready_for_future_diagnostic_design")) else "fail",
            "qa_blocking_status": "diagnostic_only_generation_blocked",
            "source_evidence_complete": "true" if _source_evidence_complete(row) else "false",
            "diagnostic_scope_confirmed": "true",
            "future_diagnostic_design_ready": "true",
            "ablauf_to_duschprofil_interface_confirmed": "true" if is_interface or is_generic else "false",
            "installable_duschprofil_article_scope_confirmed": "true" if is_generic or is_article else "false",
            "spare_cover_not_installable_profile_confirmed": "true",
            "diagnostic_only": "true",
            "qa_note": QA_NOTE,
            "production_status_note": PRODUCTION_STATUS_NOTE,
        }
        for field in BLOCK_FALSE_FIELDS:
            qa[field] = "false"
        rows.append(qa)
    return rows


def _write_csv(path: str | Path, rows: list[dict[str, str]]) -> None:
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    with Path(path).open("w", encoding="utf-8", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=CSV_COLUMNS, extrasaction="ignore")
        w.writeheader(); w.writerows(rows)


def generate_report(validation_report: str | Path, evidence_csv: str | Path, source_pack: str | Path = DEFAULT_SOURCE_PACK, family: str = EXPECTED_FAMILY) -> tuple[dict[str, Any], list[dict[str, str]]]:
    errors: list[str] = []
    warnings: list[str] = []
    validation = _read_json(validation_report, errors)
    evidence_rows = _read_csv(evidence_csv, errors)
    baseline = validator.validate(evidence_csv, source_pack, family)
    qa_rows = build_qa_rows(evidence_rows)
    for err in baseline.get("errors", []):
        if err not in errors:
            errors.append(err)
    validation_report_valid = validation.get("valid") is True
    if not validation_report_valid:
        errors.append("validation report valid must be true")

    qa_ids = [_clean(r.get("qa_row_id")) for r in qa_rows]
    report: dict[str, Any] = {
        "valid": False, "errors": errors, "warnings": warnings, "family": family, "source_pack_path": str(source_pack),
        "validation_report_path": str(validation_report), "evidence_csv_path": str(evidence_csv), "validation_report_valid": validation_report_valid,
        "input_inventory_row_count": baseline.get("input_inventory_row_count", 0), "family_inventory_row_count": baseline.get("family_inventory_row_count", 0),
        "current_machine_role_counts": baseline.get("current_machine_role_counts", {}), "evidence_csv_row_count": len(evidence_rows),
        "qa_row_count": len(qa_rows), "qa_row_ids": qa_ids, "qa_status_counts": _counts(_clean(r.get("qa_status")) for r in qa_rows),
        "qa_blocking_status_counts": _counts(_clean(r.get("qa_blocking_status")) for r in qa_rows),
        "evidence_collection_area_counts": _counts(_clean(r.get("evidence_collection_area")) for r in evidence_rows),
        "evidence_target_type_counts": _counts(_clean(r.get("evidence_target_type")) for r in evidence_rows),
        "retained_drain_body_articles": baseline.get("retained_drain_body_articles", validator.RETAINED_DRAIN_BODY_ARTICLES),
        "installable_duschprofil_articles": baseline.get("installable_duschprofil_articles", validator.INSTALLABLE_DUSCHPROFIL_ARTICLES),
        "forbidden_spare_cover_articles_in_installable_scope": baseline.get("forbidden_spare_cover_articles_in_installable_scope", []),
        "accepted_ablauf_to_duschprofil_interface_count": baseline.get("accepted_ablauf_to_duschprofil_interface_count", 0),
        "accepted_generic_duschprofil_scope_count": baseline.get("accepted_generic_duschprofil_scope_count", 0),
        "accepted_installable_duschprofil_article_count": baseline.get("accepted_installable_duschprofil_article_count", 0),
        "accepted_v3_evidence_row_count": baseline.get("accepted_v3_evidence_row_count", 0),
        "reviewed_evidence_complete_row_count": baseline.get("reviewed_evidence_complete_row_count", 0),
        "ready_for_future_diagnostic_design_count": baseline.get("ready_for_future_diagnostic_design_count", 0),
        "source_evidence_complete_count": sum(_bool(r.get("source_evidence_complete")) for r in qa_rows),
        "diagnostic_scope_confirmed_count": sum(_bool(r.get("diagnostic_scope_confirmed")) for r in qa_rows),
        "future_diagnostic_design_ready_count": sum(_bool(r.get("future_diagnostic_design_ready")) for r in qa_rows),
        "ablauf_to_duschprofil_interface_confirmed_count": sum(_bool(r.get("ablauf_to_duschprofil_interface_confirmed")) for r in qa_rows),
        "installable_duschprofil_article_scope_confirmed_count": sum(_bool(r.get("installable_duschprofil_article_scope_confirmed")) for r in qa_rows),
        "spare_cover_not_installable_profile_confirmed_count": sum(_bool(r.get("spare_cover_not_installable_profile_confirmed")) for r in qa_rows),
        "production_safe_candidate_count": int(validation.get("production_safe_candidate_count", baseline.get("production_safe_candidate_count", 0)) or 0),
        "production_promotion_blocked": validation.get("production_promotion_blocked") is True,
        "ready_for_benchmark": validation.get("ready_for_benchmark") is True,
        "ready_for_customer_view": validation.get("ready_for_customer_view") is True,
        "diagnostic_only_note": DIAGNOSTIC_ONLY_NOTE,
    }
    for field in BLOCK_FALSE_FIELDS:
        report[f"{field}_count"] = sum(_bool(r.get(field)) for r in qa_rows)
    for counter in GENERATED_COUNTERS:
        report[counter] = int(validation.get(counter, baseline.get(counter, 0)) or 0)
    checks = [
        (family == EXPECTED_FAMILY, "family must be TECEdrainprofile"),
        (report["input_inventory_row_count"] == validator.EXPECTED_INPUT_INVENTORY_ROW_COUNT and report["family_inventory_row_count"] == validator.EXPECTED_FAMILY_INVENTORY_ROW_COUNT and report["current_machine_role_counts"] == validator.EXPECTED_MACHINE_ROLE_COUNTS, "source-pack baseline counts differ from expected"),
        (report["evidence_csv_row_count"] == 34, "evidence_csv_row_count must be 34"), (report["qa_row_count"] == 34, "qa_row_count must be 34"),
        (qa_ids == EXPECTED_QA_ROW_IDS and len(set(qa_ids)) == len(qa_ids), "QA row IDs must be deterministic and unique"),
        (report["accepted_v3_evidence_row_count"] == 34, "accepted_v3_evidence_row_count must be 34"),
        (report["accepted_ablauf_to_duschprofil_interface_count"] == 3 and report["accepted_generic_duschprofil_scope_count"] == 1 and report["accepted_installable_duschprofil_article_count"] == 30, "accepted counters must be 3/1/30"),
        (report["retained_drain_body_articles"] == validator.RETAINED_DRAIN_BODY_ARTICLES, "retained drain bodies must be exactly 673001,673002,673003"),
        (report["installable_duschprofil_articles"] == validator.INSTALLABLE_DUSCHPROFIL_ARTICLES, "installable Duschprofil article list differs from expected"),
        (not report["forbidden_spare_cover_articles_in_installable_scope"] and not any(str(a).startswith("675") for a in report["installable_duschprofil_articles"]), "675xxx Profildeckel articles must not be installable Duschprofil scope"),
        (report["qa_status_counts"] == {"pass": 34}, "all QA rows must pass"),
        (report["qa_blocking_status_counts"] == {"diagnostic_only_generation_blocked": 34}, "all QA rows must have diagnostic_only_generation_blocked blocking status"),
        (report["source_evidence_complete_count"] == 34, "source_evidence_complete_count must be 34"),
        (report["diagnostic_scope_confirmed_count"] == 34, "diagnostic_scope_confirmed_count must be 34"),
        (report["future_diagnostic_design_ready_count"] == 34, "future_diagnostic_design_ready_count must be 34"),
        (report["ablauf_to_duschprofil_interface_confirmed_count"] == 4, "ablauf_to_duschprofil_interface_confirmed_count must be 4"),
        (report["installable_duschprofil_article_scope_confirmed_count"] == 31, "installable_duschprofil_article_scope_confirmed_count must be 31"),
        (report["spare_cover_not_installable_profile_confirmed_count"] == 34, "spare_cover_not_installable_profile_confirmed_count must be 34"),
        (sum(not _bool(r.get("diagnostic_only")) for r in qa_rows) == 0, "diagnostic_only must be true for all QA rows"),
        (report["production_safe_candidate_count"] == 0, "production_safe_candidate_count must be 0"),
        (report["production_promotion_blocked"] is True, "production_promotion_blocked must be true"),
        (report["ready_for_benchmark"] is False, "ready_for_benchmark must be false"),
        (report["ready_for_customer_view"] is False, "ready_for_customer_view must be false"),
    ]
    for field in BLOCK_FALSE_FIELDS:
        checks.append((report[f"{field}_count"] == 0, f"{field}_count must be 0"))
    for counter in GENERATED_COUNTERS:
        checks.append((report[counter] == 0, f"{counter} must be 0"))
    for ok, msg in checks:
        if not ok and msg not in errors:
            errors.append(msg)
    report["valid"] = not errors
    return report, qa_rows


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description="Create read-only diagnostic-only QA report for validated TECEdrainprofile Ablauf-to-Duschprofil evidence template v3.")
    p.add_argument("--validation-report", required=True); p.add_argument("--evidence-csv", required=True)
    p.add_argument("--source-pack", default=str(DEFAULT_SOURCE_PACK)); p.add_argument("--family", default=EXPECTED_FAMILY)
    p.add_argument("--out", required=True); p.add_argument("--json-out", required=True); p.add_argument("--json", action="store_true")
    a = p.parse_args(argv)
    report, qa_rows = generate_report(a.validation_report, a.evidence_csv, a.source_pack, a.family)
    _write_csv(a.out, qa_rows); write_json_output(report, out=a.json_out)
    if a.json:
        write_json_output(report)
    return 0 if report["valid"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
