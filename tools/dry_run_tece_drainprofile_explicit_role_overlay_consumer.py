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
from tools.report_tece_source_inventory import load_source_pack  # noqa: E402
from tools.tece_report_output import write_json_output  # noqa: E402
from tools.validate_tece_drainprofile_drain_body_length_role_review_csv import EXPECTED_FAMILY  # noqa: E402

EXPECTED_INPUT_INVENTORY_ROW_COUNT = 436
EXPECTED_FAMILY_INVENTORY_ROW_COUNT = 76
EXPECTED_MACHINE_ROLE_COUNTS = {"accessory": 5, "complete_set": 4, "drain_body": 12, "profile_cover": 30, "unknown": 25}
EXPECTED_DRY_RUN_EFFECTIVE_ROLE = EXPECTED_CANONICAL_ROLE_CANDIDATE
EXPECTED_DRY_RUN_ROLE_STATUS = {
    article: ("retained_unresolved_length" if article in {"673001", "673002", "673003"} else "diagnostic_reclassified_from_unknown")
    for article in EXPECTED_ARTICLES
}
EXPECTED_DRY_RUN_ACTION = {
    "673001": "retain_diagnostic_role_without_length_or_pairing",
    "673002": "retain_diagnostic_role_without_length_or_pairing",
    "673003": "retain_diagnostic_role_without_length_or_pairing",
    "675004": "diagnostic_reclassify_to_accessory_spare_part",
    "675005": "diagnostic_reclassify_to_accessory_spare_part",
    "675006": "diagnostic_reclassify_to_accessory_water_trap",
    "675008": "diagnostic_reclassify_to_accessory_water_trap",
    "675009": "diagnostic_reclassify_to_accessory",
    "675016": "diagnostic_reclassify_to_accessory_spare_part",
    "675017": "diagnostic_reclassify_to_accessory_spare_part",
    "675018": "diagnostic_reclassify_to_accessory_water_trap",
    "675025": "diagnostic_reclassify_to_profile_cover",
}
CSV_COLUMNS = [
    "dry_run_row_id", "family", "article_number", "overlay_id", "current_source_pack_role", "reviewed_role_diagnostic",
    "canonical_role_candidate", "overlay_action", "dry_run_effective_role", "dry_run_role_status", "dry_run_action",
    "length_overlay_applied", "compatibility_pairing_generated", "source_pack_mutation_performed", "production_promotion_performed",
    "benchmark_ready_after_dry_run", "customer_view_ready_after_dry_run", "diagnostic_only", "production_safe",
    "production_promotion_blocked", "ready_for_benchmark", "ready_for_customer_view", "recommended_next_action", "production_status_note",
]
RECOMMENDED_NEXT_ACTION = "Use this dry-run output only as diagnostic evidence; do not apply overlay, mutate source-pack, generate compatibility pairs, or promote production/customer outputs."
PRODUCTION_STATUS_NOTE = "diagnostic-only TECEdrainprofile explicit role overlay dry-run consumer; no source-pack mutation; no extraction logic change; no length overlay; no compatibility pairing; no Products/BOM/Final_Assemblies/Final_Set_Details/customer-view promotion"
DIAGNOSTIC_ONLY_NOTE = "This dry-run consumer is read-only and diagnostic-only. It interprets a validated TECEdrainprofile explicit role overlay CSV for diagnostic dry-run reporting only and does not apply overlays, mutate source-pack files, change TECE logic, change ACO export, Products, Comparison, BOM_Options, Final_Assemblies, or Final_Set_Details. Production promotion remains blocked."
FALSE_FIELDS = ["length_overlay_applied", "compatibility_pairing_generated", "source_pack_mutation_performed", "production_promotion_performed", "benchmark_ready_after_dry_run", "customer_view_ready_after_dry_run", "production_safe", "ready_for_benchmark", "ready_for_customer_view"]


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


def _bool_text(value: bool) -> str:
    return "true" if value else "false"


def _row_ref(row: dict[str, str], index: int) -> dict[str, str]:
    return {"row_number": str(index), "dry_run_row_id": _clean(row.get("dry_run_row_id")), "overlay_id": _clean(row.get("overlay_id")), "article_number": _clean(row.get("article_number"))}


def _source_pack_baseline(source_pack: str | Path, errors: list[str]) -> tuple[int, int, dict[str, int], dict[str, str]]:
    try:
        report = load_source_pack(source_pack)
    except Exception as exc:
        errors.append(f"source-pack cannot be loaded: {exc}")
        return 0, 0, {}, {}
    rows = report.rows
    family_rows = [r for r in rows if r.tece_family_candidate == EXPECTED_FAMILY or r.product_family == EXPECTED_FAMILY]
    role_counts = _counts([_clean(r.tece_article_role_candidate) for r in family_rows])
    article_roles = {r.article_number: _clean(r.tece_article_role_candidate) for r in family_rows if r.article_number}
    return len(rows), len(family_rows), role_counts, article_roles


def _make_rows(overlay_rows: list[dict[str, str]], family: str) -> list[dict[str, str]]:
    dry_rows = []
    for index, row in enumerate(overlay_rows, start=1):
        article = _clean(row.get("article_number"))
        dry_rows.append({
            "dry_run_row_id": f"TECE-DP-EXPLICIT-ROLE-OVERLAY-DRYRUN-{index:06d}",
            "family": family,
            "article_number": article,
            "overlay_id": _clean(row.get("overlay_id")),
            "current_source_pack_role": _clean(row.get("current_source_pack_role")),
            "reviewed_role_diagnostic": _clean(row.get("reviewed_role_diagnostic")),
            "canonical_role_candidate": _clean(row.get("canonical_role_candidate")),
            "overlay_action": _clean(row.get("overlay_action")),
            "dry_run_effective_role": EXPECTED_DRY_RUN_EFFECTIVE_ROLE.get(article, ""),
            "dry_run_role_status": EXPECTED_DRY_RUN_ROLE_STATUS.get(article, ""),
            "dry_run_action": EXPECTED_DRY_RUN_ACTION.get(article, ""),
            **{field: "false" for field in FALSE_FIELDS},
            "diagnostic_only": "true",
            "production_promotion_blocked": "true",
            "recommended_next_action": RECOMMENDED_NEXT_ACTION,
            "production_status_note": PRODUCTION_STATUS_NOTE,
        })
    return dry_rows


def build_dry_run_report(overlay_csv: str | Path, overlay_validation_report: str | Path, design_report: str | Path, qa_report: str | Path, *, source_pack: str | Path = "local_source_packs/tece/pilot_001", family: str = EXPECTED_FAMILY, out: str | Path | None = None, dry_run_rows: list[dict[str, str]] | None = None) -> dict[str, Any]:
    errors: list[str] = []
    warnings: list[str] = []
    overlay_rows = _read_csv(overlay_csv, errors, "overlay CSV")
    overlay_report = _read_json(overlay_validation_report, errors, "overlay validation report")
    design = _read_json(design_report, errors, "design report")
    qa = _read_json(qa_report, errors, "QA report")
    inv_count, fam_count, role_counts, article_roles = _source_pack_baseline(source_pack, errors)

    overlay_valid = overlay_report.get("valid") is True
    design_valid = design.get("valid") is True
    qa_valid = qa.get("valid") is True
    rows = dry_run_rows if dry_run_rows is not None else _make_rows(overlay_rows, family)
    if out:
        Path(out).parent.mkdir(parents=True, exist_ok=True)
        with Path(out).open("w", encoding="utf-8-sig", newline="") as fh:
            writer = csv.DictWriter(fh, fieldnames=CSV_COLUMNS)
            writer.writeheader(); writer.writerows([{c: row.get(c, "") for c in CSV_COLUMNS} for row in rows])

    article_numbers = [_clean(r.get("article_number")) for r in rows]
    overlay_ids = [_clean(r.get("overlay_id")) for r in rows]
    dry_ids = [_clean(r.get("dry_run_row_id")) for r in rows]
    duplicate_dry = _duplicates(dry_ids); duplicate_overlay = _duplicates(overlay_ids); duplicate_articles = _duplicates(article_numbers)
    invalid_family_rows = []
    invalid_source_pack_role_rows = []
    invalid_overlay_mapping_rows = []
    invalid_effective_rows = []
    invalid_action_rows = []
    production_leakage_rows = []
    readiness_leakage_rows = []
    diagnostic_only_leakage_rows = []
    for index, row in enumerate(rows, start=1):
        ref = _row_ref(row, index); article = ref["article_number"]
        if _clean(row.get("family")) != EXPECTED_FAMILY: invalid_family_rows.append(ref)
        if _clean(row.get("current_source_pack_role")) != EXPECTED_CURRENT_SOURCE_PACK_ROLE.get(article) or (article_roles and article_roles.get(article) != EXPECTED_CURRENT_SOURCE_PACK_ROLE.get(article)):
            invalid_source_pack_role_rows.append(ref)
        if (_clean(row.get("reviewed_role_diagnostic")) != EXPECTED_REVIEWED_ROLE_DIAGNOSTIC.get(article) or _clean(row.get("canonical_role_candidate")) != EXPECTED_CANONICAL_ROLE_CANDIDATE.get(article) or _clean(row.get("overlay_action")) != EXPECTED_ROLE_OVERLAY_ACTION.get(article)):
            invalid_overlay_mapping_rows.append(ref)
        if _clean(row.get("dry_run_effective_role")) != EXPECTED_DRY_RUN_EFFECTIVE_ROLE.get(article) or _clean(row.get("dry_run_role_status")) != EXPECTED_DRY_RUN_ROLE_STATUS.get(article):
            invalid_effective_rows.append(ref)
        if _clean(row.get("dry_run_action")) != EXPECTED_DRY_RUN_ACTION.get(article): invalid_action_rows.append(ref)
        if _bool(row.get("production_safe")) or not _bool(row.get("production_promotion_blocked")): production_leakage_rows.append(ref)
        if _bool(row.get("ready_for_benchmark")) or _bool(row.get("ready_for_customer_view")) or _bool(row.get("benchmark_ready_after_dry_run")) or _bool(row.get("customer_view_ready_after_dry_run")): readiness_leakage_rows.append(ref)
        if not _bool(row.get("diagnostic_only")): diagnostic_only_leakage_rows.append(ref)

    counts = {field: sum(_bool(r.get(field)) for r in rows) for field in FALSE_FIELDS}
    checks = [
        (overlay_valid, "overlay validation report valid must be true"), (design_valid, "design report valid must be true"), (qa_valid, "QA report valid must be true"),
        (family == EXPECTED_FAMILY, "family must be TECEdrainprofile"), (inv_count == EXPECTED_INPUT_INVENTORY_ROW_COUNT, "input inventory row count differs from expected baseline"),
        (fam_count == EXPECTED_FAMILY_INVENTORY_ROW_COUNT, "family inventory row count differs from expected baseline"), (role_counts == EXPECTED_MACHINE_ROLE_COUNTS, "current machine role counts differ from expected baseline"),
        (len(overlay_rows) == 12, "overlay row count must be 12"), (len(rows) == 12, "dry-run row count must be 12"), (sorted(article_numbers) == EXPECTED_ARTICLES, "article set differs from expected 12 articles"),
        (overlay_report.get("overlay_row_count") in (None, 12), "overlay validation report overlay_row_count must be 12"), (not duplicate_dry, "duplicate dry_run_row_id detected"), (not duplicate_overlay, "duplicate overlay_id detected"), (not duplicate_articles, "duplicate article_number detected"),
        (not invalid_family_rows, "invalid family rows detected"), (not invalid_source_pack_role_rows, "current_source_pack_role mapping differs from expected"), (not invalid_overlay_mapping_rows, "overlay mapping differs from expected"),
        (not invalid_effective_rows, "dry_run_effective_role or status mapping differs from expected"), (not invalid_action_rows, "dry_run_action mapping differs from expected"),
        *[(counts[field] == 0, f"{field} must be false for every row") for field in FALSE_FIELDS], (not production_leakage_rows, "production leakage detected"), (not readiness_leakage_rows, "readiness leakage detected"), (not diagnostic_only_leakage_rows, "diagnostic_only must be true for every row"),
    ]
    for ok, message in checks:
        if not ok and message not in errors: errors.append(message)

    return {
        "valid": not errors, "errors": errors, "warnings": warnings, "family": family, "source_pack_path": str(source_pack), "overlay_csv_path": str(overlay_csv), "overlay_validation_report_path": str(overlay_validation_report), "design_report_path": str(design_report), "qa_report_path": str(qa_report),
        "overlay_validation_report_valid": overlay_valid, "design_report_valid": design_valid, "qa_report_valid": qa_valid, "input_inventory_row_count": inv_count, "family_inventory_row_count": fam_count, "current_machine_role_counts": role_counts,
        "overlay_row_count": len(overlay_rows), "dry_run_row_count": len(rows), "article_numbers": sorted(article_numbers), "current_source_pack_role_counts_for_overlay": _counts([_clean(r.get("current_source_pack_role")) for r in rows]),
        "reviewed_role_diagnostic_counts": _counts([_clean(r.get("reviewed_role_diagnostic")) for r in rows]), "canonical_role_candidate_counts": _counts([_clean(r.get("canonical_role_candidate")) for r in rows]), "overlay_action_counts": _counts([_clean(r.get("overlay_action")) for r in rows]),
        "dry_run_effective_role_counts": _counts([_clean(r.get("dry_run_effective_role")) for r in rows]), "dry_run_role_status_counts": _counts([_clean(r.get("dry_run_role_status")) for r in rows]), "dry_run_action_counts": _counts([_clean(r.get("dry_run_action")) for r in rows]),
        "duplicate_dry_run_row_ids": duplicate_dry, "duplicate_overlay_ids": duplicate_overlay, "duplicate_article_numbers": duplicate_articles, "invalid_family_rows": invalid_family_rows, "invalid_source_pack_role_rows": invalid_source_pack_role_rows, "invalid_overlay_mapping_rows": invalid_overlay_mapping_rows,
        "invalid_dry_run_effective_role_rows": invalid_effective_rows, "invalid_dry_run_action_rows": invalid_action_rows, "production_leakage_rows": production_leakage_rows, "readiness_leakage_rows": readiness_leakage_rows, "diagnostic_only_leakage_rows": diagnostic_only_leakage_rows,
        "length_overlay_applied_count": counts["length_overlay_applied"], "compatibility_pairing_generated_count": counts["compatibility_pairing_generated"], "source_pack_mutation_performed_count": counts["source_pack_mutation_performed"], "production_promotion_performed_count": counts["production_promotion_performed"],
        "benchmark_ready_after_dry_run_count": counts["benchmark_ready_after_dry_run"], "customer_view_ready_after_dry_run_count": counts["customer_view_ready_after_dry_run"], "proposed_source_pack_mutation_count": 0, "production_safe_candidate_count": counts["production_safe"],
        "production_promotion_blocked": not production_leakage_rows, "ready_for_benchmark": any(_bool(r.get("ready_for_benchmark")) for r in rows), "ready_for_customer_view": any(_bool(r.get("ready_for_customer_view")) for r in rows), "output_csv_path": str(out) if out else None, "diagnostic_only_note": DIAGNOSTIC_ONLY_NOTE,
    }


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description="Build read-only diagnostic dry-run report for validated TECEdrainprofile explicit role overlay CSV.")
    p.add_argument("--overlay-csv", required=True); p.add_argument("--overlay-validation-report", required=True); p.add_argument("--design-report", required=True); p.add_argument("--qa-report", required=True)
    p.add_argument("--source-pack", default="local_source_packs/tece/pilot_001"); p.add_argument("--family", default=EXPECTED_FAMILY); p.add_argument("--out", required=True); p.add_argument("--json-out", required=True); p.add_argument("--json", action="store_true")
    args = p.parse_args(argv)
    report = build_dry_run_report(args.overlay_csv, args.overlay_validation_report, args.design_report, args.qa_report, source_pack=args.source_pack, family=args.family, out=args.out)
    write_json_output(report, out=args.json_out)
    if args.json: write_json_output(report)
    return 0 if report["valid"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
