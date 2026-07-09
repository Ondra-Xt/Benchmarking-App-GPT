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
EXPECTED_MAPPING = {
    "673001": ("drain_body", "drain_body", "retained_unresolved_length", "retain_diagnostic_role_without_length_or_pairing"),
    "673002": ("drain_body", "drain_body", "retained_unresolved_length", "retain_diagnostic_role_without_length_or_pairing"),
    "673003": ("drain_body", "drain_body", "retained_unresolved_length", "retain_diagnostic_role_without_length_or_pairing"),
    "675004": ("unknown", "accessory", "diagnostic_reclassified_from_unknown", "diagnostic_reclassify_to_accessory_spare_part"),
    "675005": ("unknown", "accessory", "diagnostic_reclassified_from_unknown", "diagnostic_reclassify_to_accessory_spare_part"),
    "675006": ("unknown", "accessory", "diagnostic_reclassified_from_unknown", "diagnostic_reclassify_to_accessory_water_trap"),
    "675008": ("unknown", "accessory", "diagnostic_reclassified_from_unknown", "diagnostic_reclassify_to_accessory_water_trap"),
    "675009": ("unknown", "accessory", "diagnostic_reclassified_from_unknown", "diagnostic_reclassify_to_accessory"),
    "675016": ("unknown", "accessory", "diagnostic_reclassified_from_unknown", "diagnostic_reclassify_to_accessory_spare_part"),
    "675017": ("unknown", "accessory", "diagnostic_reclassified_from_unknown", "diagnostic_reclassify_to_accessory_spare_part"),
    "675018": ("unknown", "accessory", "diagnostic_reclassified_from_unknown", "diagnostic_reclassify_to_accessory_water_trap"),
    "675025": ("unknown", "profile_cover", "diagnostic_reclassified_from_unknown", "diagnostic_reclassify_to_profile_cover"),
}
EXPECTED_ARTICLES = sorted(EXPECTED_MAPPING)
CSV_COLUMNS = ["qa_row_id","family","article_number","dry_run_row_id","overlay_id","current_source_pack_role","dry_run_effective_role","dry_run_role_status","dry_run_action","expected_current_source_pack_role","expected_dry_run_effective_role","expected_dry_run_role_status","expected_dry_run_action","mapping_status","blocking_status","qa_status","length_overlay_applied","compatibility_pairing_generated","source_pack_mutation_performed","production_promotion_performed","benchmark_ready_after_dry_run","customer_view_ready_after_dry_run","diagnostic_only","production_safe","production_promotion_blocked","ready_for_benchmark","ready_for_customer_view","recommended_next_action","production_status_note"]
RECOMMENDED_NEXT_ACTION = "QA passed for diagnostic dry-run output; keep as evidence only and do not apply overlay, mutate source-pack, generate compatibility pairs, or promote production/customer outputs."
PRODUCTION_STATUS_NOTE = "diagnostic-only QA for TECEdrainprofile explicit role overlay dry-run output; no source-pack mutation; no extraction logic change; no length overlay; no compatibility pairing; no Products/BOM/Final_Assemblies/Final_Set_Details/customer-view promotion"
DIAGNOSTIC_ONLY_NOTE = "This QA report is read-only and diagnostic-only. It validates the TECEdrainprofile explicit role overlay dry-run output and confirms no overlay application, source-pack mutation, TECE logic change, ACO export change, Products, Comparison, BOM_Options, Final_Assemblies, or Final_Set_Details promotion. Production promotion remains blocked."


def _clean(v: Any) -> str:
    return "" if v is None else str(v).strip()


def _bool(v: Any) -> bool:
    if isinstance(v, bool):
        return v
    return _clean(v).lower() in {"1", "true", "yes", "y"}


def _counts(vals: list[str]) -> dict[str, int]:
    return dict(sorted(Counter(v or "" for v in vals).items()))


def _dups(vals: list[str]) -> list[str]:
    c = Counter(vals)
    return sorted(k for k, v in c.items() if k and v > 1)


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


def _field(row: dict[str, Any], *names: str) -> str:
    for name in names:
        value = _clean(row.get(name))
        if value:
            return value
    return ""


def _row_ref(row: dict[str, Any], idx: int) -> dict[str, str]:
    return {"row_number": str(idx), "qa_row_id": _clean(row.get("qa_row_id")), "dry_run_row_id": _clean(row.get("dry_run_row_id")), "overlay_id": _clean(row.get("overlay_id")), "article_number": _clean(row.get("article_number"))}


def _source_pack_counts(source_pack: str | Path, family: str, errors: list[str]) -> tuple[int, int, dict[str, int]]:
    try:
        report = load_source_pack(source_pack)
    except Exception as exc:
        errors.append(f"source-pack cannot be loaded: {exc}")
        return 0, 0, {}
    rows = list(getattr(report, "rows", []) or [])
    input_count = len(rows)
    family_rows = [r for r in rows if (_clean(getattr(r, "tece_family_candidate", "")) or _clean(getattr(r, "product_family", ""))) == family]
    role_counts = _counts([_clean(getattr(r, "tece_article_role_candidate", "")) or "unknown" for r in family_rows])
    return input_count, len(family_rows), role_counts


def _build_qa_rows(dry_rows: list[dict[str, str]], family: str) -> list[dict[str, str]]:
    rows = []
    for idx, dry in enumerate(dry_rows, start=1):
        article = _field(dry, "article_number", "article")
        exp = EXPECTED_MAPPING.get(article, ("", "", "", ""))
        current = _field(dry, "current_source_pack_role")
        effective = _field(dry, "dry_run_effective_role", "effective_role")
        status = _field(dry, "dry_run_role_status", "role_status")
        action = _field(dry, "dry_run_action", "action")
        mapping_pass = (current, effective, status, action) == exp
        row = {
            "qa_row_id": f"TECE-DP-EXPLICIT-ROLE-OVERLAY-DRYRUN-QA-{idx:06d}",
            "family": _field(dry, "family") or family,
            "article_number": article,
            "dry_run_row_id": _field(dry, "dry_run_row_id", "row_id") or f"TECE-DP-EXPLICIT-ROLE-OVERLAY-DRYRUN-{idx:06d}",
            "overlay_id": _field(dry, "overlay_id") or f"TECE-DP-EXPLICIT-ROLE-OVERLAY-{idx:06d}",
            "current_source_pack_role": current,
            "dry_run_effective_role": effective,
            "dry_run_role_status": status,
            "dry_run_action": action,
            "expected_current_source_pack_role": exp[0],
            "expected_dry_run_effective_role": exp[1],
            "expected_dry_run_role_status": exp[2],
            "expected_dry_run_action": exp[3],
            "mapping_status": "pass" if mapping_pass else "fail",
            "blocking_status": "pass",
            "qa_status": "pass" if mapping_pass else "fail",
            "length_overlay_applied": "false",
            "compatibility_pairing_generated": "false",
            "source_pack_mutation_performed": "false",
            "production_promotion_performed": "false",
            "benchmark_ready_after_dry_run": "false",
            "customer_view_ready_after_dry_run": "false",
            "diagnostic_only": "true",
            "production_safe": "false",
            "production_promotion_blocked": "true",
            "ready_for_benchmark": "false",
            "ready_for_customer_view": "false",
            "recommended_next_action": RECOMMENDED_NEXT_ACTION,
            "production_status_note": PRODUCTION_STATUS_NOTE,
        }
        rows.append(row)
    return rows


def build_qa_report(dry_run_csv: str | Path, dry_run_report: str | Path, overlay_validation_report: str | Path, source_pack: str | Path = DEFAULT_SOURCE_PACK, family: str = EXPECTED_FAMILY, out: str | Path | None = None, qa_rows: list[dict[str, str]] | None = None) -> tuple[list[dict[str, str]], dict[str, Any]]:
    errors: list[str] = []
    warnings: list[str] = []
    dry_rows = _read_csv(dry_run_csv, errors, "dry-run CSV")
    dry_report = _read_json(dry_run_report, errors, "dry-run report")
    overlay_report = _read_json(overlay_validation_report, errors, "overlay validation report")
    input_count, family_count, machine_counts = _source_pack_counts(source_pack, family, errors)
    rows = list(qa_rows) if qa_rows is not None else _build_qa_rows(dry_rows, family)

    dry_valid = dry_report.get("valid") is True
    overlay_valid = overlay_report.get("valid") is True
    article_numbers = sorted(_clean(r.get("article_number")) for r in rows if _clean(r.get("article_number")))
    dup_qa = _dups([_clean(r.get("qa_row_id")) for r in rows])
    dup_dry = _dups([_clean(r.get("dry_run_row_id")) for r in rows])
    dup_overlay = _dups([_clean(r.get("overlay_id")) for r in rows])
    dup_article = _dups([_clean(r.get("article_number")) for r in rows])
    invalid_family = [_row_ref(r, i) for i, r in enumerate(rows, 1) if _clean(r.get("family")) != EXPECTED_FAMILY]
    invalid_source = [_row_ref(r, i) for i, r in enumerate(rows, 1) if _clean(r.get("current_source_pack_role")) != EXPECTED_MAPPING.get(_clean(r.get("article_number")), ("", "", "", ""))[0]]
    invalid_effective = [_row_ref(r, i) for i, r in enumerate(rows, 1) if _clean(r.get("dry_run_effective_role")) != EXPECTED_MAPPING.get(_clean(r.get("article_number")), ("", "", "", ""))[1]]
    invalid_status = [_row_ref(r, i) for i, r in enumerate(rows, 1) if _clean(r.get("dry_run_role_status")) != EXPECTED_MAPPING.get(_clean(r.get("article_number")), ("", "", "", ""))[2]]
    invalid_action = [_row_ref(r, i) for i, r in enumerate(rows, 1) if _clean(r.get("dry_run_action")) != EXPECTED_MAPPING.get(_clean(r.get("article_number")), ("", "", "", ""))[3]]
    invalid_mapping = [_row_ref(r, i) for i, r in enumerate(rows, 1) if _clean(r.get("mapping_status")) != "pass"]
    invalid_blocking = [_row_ref(r, i) for i, r in enumerate(rows, 1) if _clean(r.get("blocking_status")) != "pass"]
    invalid_qa = [_row_ref(r, i) for i, r in enumerate(rows, 1) if _clean(r.get("qa_status")) != "pass"]
    production_leakage = [_row_ref(r, i) for i, r in enumerate(rows, 1) if any(_bool(r.get(k)) for k in ["length_overlay_applied","compatibility_pairing_generated","source_pack_mutation_performed","production_promotion_performed","production_safe"]) or not _bool(r.get("production_promotion_blocked"))]
    readiness_leakage = [_row_ref(r, i) for i, r in enumerate(rows, 1) if _bool(r.get("benchmark_ready_after_dry_run")) or _bool(r.get("customer_view_ready_after_dry_run")) or _bool(r.get("ready_for_benchmark")) or _bool(r.get("ready_for_customer_view"))]
    diagnostic_leakage = [_row_ref(r, i) for i, r in enumerate(rows, 1) if not _bool(r.get("diagnostic_only"))]
    bool_counts = {k: sum(_bool(r.get(k)) for r in rows) for k in ["length_overlay_applied","compatibility_pairing_generated","source_pack_mutation_performed","production_promotion_performed","benchmark_ready_after_dry_run","customer_view_ready_after_dry_run"]}
    production_safe_count = sum(_bool(r.get("production_safe")) for r in rows)
    promotion_blocked = all(_bool(r.get("production_promotion_blocked")) for r in rows) if rows else False
    ready_bench = any(_bool(r.get("ready_for_benchmark")) for r in rows)
    ready_customer = any(_bool(r.get("ready_for_customer_view")) for r in rows)

    checks = [
        (dry_valid, "dry-run report valid must be true"), (overlay_valid, "overlay validation report valid must be true"), (family == EXPECTED_FAMILY, "family must be TECEdrainprofile"),
        (input_count == EXPECTED_INPUT_INVENTORY_ROW_COUNT, "input_inventory_row_count must be 436"), (family_count == EXPECTED_FAMILY_INVENTORY_ROW_COUNT, "family_inventory_row_count must be 76"), (machine_counts == EXPECTED_MACHINE_ROLE_COUNTS, "current_machine_role_counts differ from expected baseline"),
        (len(dry_rows) == 12, "dry-run CSV row count must be 12"), (dry_report.get("dry_run_row_count") == 12, "dry-run report dry_run_row_count must be 12"), (dry_report.get("overlay_row_count") == 12, "dry-run report overlay_row_count must be 12"),
        (article_numbers == EXPECTED_ARTICLES, "article set differs from expected 12 articles"), (not dup_qa, "duplicate qa_row_id detected"), (not dup_dry, "duplicate dry_run_row_id detected"), (not dup_overlay, "duplicate overlay_id detected"), (not dup_article, "duplicate article_number detected"),
        (not invalid_family, "invalid family rows detected"), (not invalid_source, "current_source_pack_role mapping differs from expected"), (not invalid_effective, "dry_run_effective_role mapping differs from expected"), (not invalid_status, "dry_run_role_status mapping differs from expected"), (not invalid_action, "dry_run_action mapping differs from expected"),
        (not invalid_mapping, "mapping_status must be pass"), (not invalid_blocking, "blocking_status must be pass"), (not invalid_qa, "qa_status must be pass"), (not production_leakage, "production leakage detected"), (not readiness_leakage, "readiness leakage detected"), (not diagnostic_leakage, "diagnostic_only must be true"), (promotion_blocked, "production_promotion_blocked must be true"), (not ready_bench, "ready_for_benchmark must be false"), (not ready_customer, "ready_for_customer_view must be false"),
    ]
    for ok, msg in checks:
        if not ok and msg not in errors:
            errors.append(msg)
    if out:
        Path(out).parent.mkdir(parents=True, exist_ok=True)
        with Path(out).open("w", encoding="utf-8-sig", newline="") as fh:
            writer = csv.DictWriter(fh, fieldnames=CSV_COLUMNS); writer.writeheader(); writer.writerows(rows)
    report = {"valid": not errors,"errors": errors,"warnings": warnings,"family": family,"source_pack_path": str(source_pack),"dry_run_csv_path": str(dry_run_csv),"dry_run_report_path": str(dry_run_report),"overlay_validation_report_path": str(overlay_validation_report),"dry_run_report_valid": dry_valid,"overlay_validation_report_valid": overlay_valid,"input_inventory_row_count": input_count,"family_inventory_row_count": family_count,"current_machine_role_counts": machine_counts,"dry_run_csv_row_count": len(dry_rows),"dry_run_report_row_count": dry_report.get("dry_run_row_count"),"overlay_report_row_count": dry_report.get("overlay_row_count"),"qa_row_count": len(rows),"article_numbers": article_numbers,"duplicate_qa_row_ids": dup_qa,"duplicate_dry_run_row_ids": dup_dry,"duplicate_overlay_ids": dup_overlay,"duplicate_article_numbers": dup_article,"current_source_pack_role_counts": _counts([_clean(r.get("current_source_pack_role")) for r in rows]),"dry_run_effective_role_counts": _counts([_clean(r.get("dry_run_effective_role")) for r in rows]),"dry_run_role_status_counts": _counts([_clean(r.get("dry_run_role_status")) for r in rows]),"dry_run_action_counts": _counts([_clean(r.get("dry_run_action")) for r in rows]),"mapping_status_counts": _counts([_clean(r.get("mapping_status")) for r in rows]),"blocking_status_counts": _counts([_clean(r.get("blocking_status")) for r in rows]),"qa_status_counts": _counts([_clean(r.get("qa_status")) for r in rows]),"invalid_family_rows": invalid_family,"invalid_source_pack_role_rows": invalid_source,"invalid_dry_run_effective_role_rows": invalid_effective,"invalid_dry_run_role_status_rows": invalid_status,"invalid_dry_run_action_rows": invalid_action,"invalid_mapping_rows": invalid_mapping,"invalid_blocking_rows": invalid_blocking,"production_leakage_rows": production_leakage,"readiness_leakage_rows": readiness_leakage,"diagnostic_only_leakage_rows": diagnostic_leakage,**{f"{k}_count": v for k, v in bool_counts.items()},"proposed_source_pack_mutation_count": 0,"production_safe_candidate_count": production_safe_count,"production_promotion_blocked": promotion_blocked,"ready_for_benchmark": ready_bench,"ready_for_customer_view": ready_customer,"output_csv_path": str(out) if out else None,"diagnostic_only_note": DIAGNOSTIC_ONLY_NOTE}
    return rows, report


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description="Build read-only TECEdrainprofile explicit role overlay dry-run QA report.")
    p.add_argument("--dry-run-csv", required=True); p.add_argument("--dry-run-report", required=True); p.add_argument("--overlay-validation-report", required=True)
    p.add_argument("--source-pack", default=str(DEFAULT_SOURCE_PACK)); p.add_argument("--family", default=EXPECTED_FAMILY); p.add_argument("--out", required=True); p.add_argument("--json-out", required=True); p.add_argument("--json", action="store_true")
    args = p.parse_args(argv)
    _rows, report = build_qa_report(args.dry_run_csv, args.dry_run_report, args.overlay_validation_report, args.source_pack, args.family, args.out)
    write_json_output(report, out=args.json_out)
    if args.json:
        write_json_output(report)
    return 0 if report["valid"] else 1

if __name__ == "__main__":
    raise SystemExit(main())
