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
EXPECTED_QA_ROW_COUNT = 12
CSV_COLUMNS = ["audit_row_id","family","audit_area","audit_status","evidence_source","observed_value","expected_value","blocking_reason","compatibility_pair_generation_allowed","length_overlay_allowed","source_pack_mutation_allowed","production_promotion_allowed","benchmark_ready_allowed","customer_view_allowed","diagnostic_only","recommended_next_action","production_status_note"]
RECOMMENDED_NEXT_ACTION = "Collect explicit TECEdrainprofile article-level drain-body-to-profile-cover compatibility evidence before designing any diagnostic pairing workflow; do not generate pairs or promote production/customer outputs."
PRODUCTION_STATUS_NOTE = "diagnostic-only TECEdrainprofile post-overlay compatibility readiness audit; no source-pack mutation; no extraction logic change; no length overlay; no compatibility pair generation; no Products/BOM/Final_Assemblies/Final_Set_Details/customer-view promotion"
DIAGNOSTIC_ONLY_NOTE = "This readiness audit is read-only and diagnostic-only. It evaluates whether the TECEdrainprofile explicit role overlay dry-run evidence is sufficient for compatibility pairing and confirms that compatibility pair generation, length overlay, source-pack mutation, TECE logic change, ACO export change, Products, Comparison, BOM_Options, Final_Assemblies, or Final_Set_Details promotion remain blocked. Production promotion remains blocked."


def _clean(v: Any) -> str:
    return "" if v is None else str(v).strip()


def _bool(v: Any) -> bool:
    if isinstance(v, bool):
        return v
    return _clean(v).lower() in {"1", "true", "yes", "y"}


def _count(rows: list[dict[str, str]], field: str, value: str = "true") -> int:
    return sum(_clean(r.get(field)).lower() == value for r in rows)


def _counts(vals: list[str]) -> dict[str, int]:
    return dict(sorted(Counter(vals).items()))


def _dups(vals: list[str]) -> list[str]:
    c = Counter(vals)
    return sorted(k for k, v in c.items() if k and v > 1)


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
    return len(rows), len(family_rows), _counts([_clean(getattr(r, "tece_article_role_candidate", "")) or "unknown" for r in family_rows])


def _audit_rows(family: str) -> list[dict[str, str]]:
    specs = [
        ("role_overlay_chain_validity", "pass", "dry-run QA valid and dry-run report valid", "both valid true", ""),
        ("source_pack_baseline_stability", "pass", "436 inventory rows and 76 TECEdrainprofile rows", "436/76 baseline", ""),
        ("reviewed_effective_role_scope", "pass", "drain_body=3, accessory=8, profile_cover=1", "dry-run effective role counts from QA", ""),
        ("retained_drain_body_length_status", "blocked", "retained_unresolved_length=3", "no reviewed drain body length overlay available", "retained drain bodies have unresolved nominal length and cannot be used for length-governed pairing"),
        ("explicit_article_level_compatibility_evidence", "blocked", "explicit compatibility evidence count=0", "explicit article-level compatibility matrix required before pairing", "no explicit TECEdrainprofile article-level drain-body-to-profile-cover compatibility evidence has been validated"),
        ("compatibility_pair_generation", "blocked", "generated pairs=0", "no compatibility pairs generated", "diagnostic audit only; pair generation remains blocked"),
        ("production_promotion_readiness", "blocked", "production_safe_candidate_count=0", "no production-safe TECEdrainprofile candidates", "role overlay evidence is diagnostic-only and cannot promote benchmark/customer outputs"),
        ("recommended_evidence_collection", "required_next_step", "explicit compatibility evidence missing", "collect explicit TECEdrainprofile drain-body/profile-cover compatibility evidence before any pairing workflow", "explicit source evidence required"),
    ]
    rows = []
    for i, (area, status, observed, expected, reason) in enumerate(specs, 1):
        rows.append({"audit_row_id": f"TECE-DP-POST-OVERLAY-COMPAT-READINESS-{i:06d}", "family": family, "audit_area": area, "audit_status": status, "evidence_source": "dry-run QA report; dry-run QA CSV; dry-run report; source-pack inventory", "observed_value": observed, "expected_value": expected, "blocking_reason": reason, "compatibility_pair_generation_allowed": "false", "length_overlay_allowed": "false", "source_pack_mutation_allowed": "false", "production_promotion_allowed": "false", "benchmark_ready_allowed": "false", "customer_view_allowed": "false", "diagnostic_only": "true", "recommended_next_action": RECOMMENDED_NEXT_ACTION, "production_status_note": PRODUCTION_STATUS_NOTE})
    return rows


def build_readiness_audit(dry_run_qa_report: str | Path, dry_run_qa_csv: str | Path, dry_run_report: str | Path, source_pack: str | Path = DEFAULT_SOURCE_PACK, family: str = EXPECTED_FAMILY, out: str | Path | None = None, audit_rows: list[dict[str, str]] | None = None) -> tuple[list[dict[str, str]], dict[str, Any]]:
    errors: list[str] = []; warnings: list[str] = []
    qa_report = _read_json(dry_run_qa_report, errors, "dry-run QA report")
    qa_csv_rows = _read_csv(dry_run_qa_csv, errors, "dry-run QA CSV")
    dry_report = _read_json(dry_run_report, errors, "dry-run report")
    input_count, family_count, machine_counts = _source_pack_counts(source_pack, family, errors)
    rows = list(audit_rows) if audit_rows is not None else _audit_rows(family)

    dry_run_qa_report_valid = qa_report.get("valid") is True
    dry_run_report_valid = dry_report.get("valid") is True
    effective = qa_report.get("dry_run_effective_role_counts") or {}
    statuses = qa_report.get("dry_run_role_status_counts") or {}
    source_pack_baseline_valid = input_count == EXPECTED_INPUT_INVENTORY_ROW_COUNT and family_count == EXPECTED_FAMILY_INVENTORY_ROW_COUNT and machine_counts == EXPECTED_MACHINE_ROLE_COUNTS
    role_overlay_chain_valid = dry_run_qa_report_valid and dry_run_report_valid

    leakage_fields = ["compatibility_pair_generation_allowed","length_overlay_allowed","source_pack_mutation_allowed","production_promotion_allowed"]
    readiness_fields = ["benchmark_ready_allowed","customer_view_allowed"]
    production_leakage = [r.get("audit_row_id", str(i)) for i, r in enumerate(rows, 1) if any(_bool(r.get(f)) for f in leakage_fields)]
    readiness_leakage = [r.get("audit_row_id", str(i)) for i, r in enumerate(rows, 1) if any(_bool(r.get(f)) for f in readiness_fields)]
    diagnostic_leakage = [r.get("audit_row_id", str(i)) for i, r in enumerate(rows, 1) if not _bool(r.get("diagnostic_only"))]
    invalid_family = [r.get("audit_row_id", str(i)) for i, r in enumerate(rows, 1) if _clean(r.get("family")) != EXPECTED_FAMILY]
    invalid_status = [r.get("audit_row_id", str(i)) for i, r in enumerate(rows, 1) if _clean(r.get("audit_status")) not in {"pass", "blocked", "required_next_step"}]
    invalid_blocking = [r.get("audit_row_id", str(i)) for i, r in enumerate(rows, 1) if _clean(r.get("audit_status")) == "blocked" and not _clean(r.get("blocking_reason"))]

    generated_pairs = int(qa_report.get("compatibility_pairing_generated_count") or 0)
    length_overlay_applied = int(qa_report.get("length_overlay_applied_count") or 0)
    source_pack_mutations_performed = int(qa_report.get("source_pack_mutation_performed_count") or 0)
    production_promotions_performed = int(qa_report.get("production_promotion_performed_count") or 0)
    benchmark_after_dry_run = int(qa_report.get("benchmark_ready_after_dry_run_count") or 0)
    customer_after_dry_run = int(qa_report.get("customer_view_ready_after_dry_run_count") or 0)
    proposed_mutations = int(qa_report.get("proposed_source_pack_mutation_count") or 0)
    production_safe = int(qa_report.get("production_safe_candidate_count") or 0)
    checks = [
        (dry_run_qa_report_valid, "dry-run QA report valid must be true"), (dry_run_report_valid, "dry-run report valid must be true"), (family == EXPECTED_FAMILY, "family must be TECEdrainprofile"), (source_pack_baseline_valid, "source-pack baseline counts differ from expected"), (len(qa_csv_rows) == 12, "dry-run QA CSV row count must be 12"), (qa_report.get("qa_row_count") == 12, "dry-run QA report qa_row_count must be 12"), (dry_report.get("dry_run_row_count") == 12, "dry-run report dry_run_row_count must be 12"), ((qa_report.get("mapping_status_counts") or {}).get("pass") == 12, "mapping_status_counts.pass must be 12"), ((qa_report.get("blocking_status_counts") or {}).get("pass") == 12, "blocking_status_counts.pass must be 12"), ((qa_report.get("qa_status_counts") or {}).get("pass") == 12, "qa_status_counts.pass must be 12"),
        (_count(rows, "compatibility_pair_generation_allowed") == 0, "compatibility_pair_generation_allowed_count must be 0"), (_count(rows, "length_overlay_allowed") == 0, "length_overlay_allowed_count must be 0"), (_count(rows, "source_pack_mutation_allowed") == 0, "source_pack_mutation_allowed_count must be 0"), (_count(rows, "production_promotion_allowed") == 0, "production_promotion_allowed_count must be 0"), (_count(rows, "benchmark_ready_allowed") == 0, "benchmark_ready_allowed_count must be 0"), (_count(rows, "customer_view_allowed") == 0, "customer_view_allowed_count must be 0"), (generated_pairs == 0, "generated_compatibility_pair_count must be 0"), (length_overlay_applied == 0, "length_overlay_applied_count must be 0"), (source_pack_mutations_performed == 0, "source_pack_mutation_performed_count must be 0"), (production_promotions_performed == 0, "production_promotion_performed_count must be 0"), (benchmark_after_dry_run == 0, "benchmark_ready_after_dry_run_count must be 0"), (customer_after_dry_run == 0, "customer_view_ready_after_dry_run_count must be 0"), (proposed_mutations == 0, "proposed_source_pack_mutation_count must be 0"), (production_safe == 0, "production_safe_candidate_count must be 0"), (not production_leakage and not readiness_leakage and not diagnostic_leakage, "production/readiness/diagnostic leakage detected"), (qa_report.get("production_promotion_blocked") is True, "production_promotion_blocked must be true"), (qa_report.get("ready_for_benchmark") is False, "ready_for_benchmark must be false"), (qa_report.get("ready_for_customer_view") is False, "ready_for_customer_view must be false"),
    ]
    for ok, msg in checks:
        if not ok and msg not in errors: errors.append(msg)
    if out:
        Path(out).parent.mkdir(parents=True, exist_ok=True)
        with Path(out).open("w", encoding="utf-8-sig", newline="") as fh:
            w = csv.DictWriter(fh, fieldnames=CSV_COLUMNS); w.writeheader(); w.writerows(rows)
    report = {"valid": not errors,"errors": errors,"warnings": warnings,"family": family,"source_pack_path": str(source_pack),"dry_run_qa_report_path": str(dry_run_qa_report),"dry_run_qa_csv_path": str(dry_run_qa_csv),"dry_run_report_path": str(dry_run_report),"dry_run_qa_report_valid": dry_run_qa_report_valid,"dry_run_report_valid": dry_run_report_valid,"input_inventory_row_count": input_count,"family_inventory_row_count": family_count,"current_machine_role_counts": machine_counts,"dry_run_qa_csv_row_count": len(qa_csv_rows),"dry_run_qa_report_row_count": qa_report.get("qa_row_count"),"dry_run_report_row_count": dry_report.get("dry_run_row_count"),"audit_row_count": len(rows),"role_overlay_chain_valid": role_overlay_chain_valid,"source_pack_baseline_valid": source_pack_baseline_valid,"reviewed_effective_drain_body_count": int(effective.get("drain_body") or 0),"reviewed_effective_accessory_count": int(effective.get("accessory") or 0),"reviewed_effective_profile_cover_count": int(effective.get("profile_cover") or 0),"retained_unresolved_length_count": int(statuses.get("retained_unresolved_length") or 0),"diagnostic_reclassified_from_unknown_count": int(statuses.get("diagnostic_reclassified_from_unknown") or 0),"source_pack_profile_cover_count": int(machine_counts.get("profile_cover") or 0),"explicit_article_level_compatibility_evidence_count": 0,"compatibility_pair_generation_allowed_count": _count(rows, "compatibility_pair_generation_allowed"),"length_overlay_allowed_count": _count(rows, "length_overlay_allowed"),"source_pack_mutation_allowed_count": _count(rows, "source_pack_mutation_allowed"),"production_promotion_allowed_count": _count(rows, "production_promotion_allowed"),"benchmark_ready_allowed_count": _count(rows, "benchmark_ready_allowed"),"customer_view_allowed_count": _count(rows, "customer_view_allowed"),"generated_compatibility_pair_count": generated_pairs,"proposed_source_pack_mutation_count": proposed_mutations,"production_safe_candidate_count": production_safe,"audit_status_counts": _counts([_clean(r.get("audit_status")) for r in rows]),"duplicate_audit_row_ids": _dups([_clean(r.get("audit_row_id")) for r in rows]),"invalid_family_rows": invalid_family,"invalid_audit_status_rows": invalid_status,"invalid_blocking_rows": invalid_blocking,"production_leakage_rows": production_leakage,"readiness_leakage_rows": readiness_leakage,"diagnostic_only_leakage_rows": diagnostic_leakage,"production_promotion_blocked": qa_report.get("production_promotion_blocked"),"ready_for_benchmark": qa_report.get("ready_for_benchmark"),"ready_for_customer_view": qa_report.get("ready_for_customer_view"),"output_csv_path": str(out) if out else None,"diagnostic_only_note": DIAGNOSTIC_ONLY_NOTE}
    return rows, report


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description="Build read-only TECEdrainprofile post-overlay compatibility readiness audit.")
    p.add_argument("--dry-run-qa-report", required=True); p.add_argument("--dry-run-qa-csv", required=True); p.add_argument("--dry-run-report", required=True)
    p.add_argument("--source-pack", default=str(DEFAULT_SOURCE_PACK)); p.add_argument("--family", default=EXPECTED_FAMILY); p.add_argument("--out", required=True); p.add_argument("--json-out", required=True); p.add_argument("--json", action="store_true")
    args = p.parse_args(argv)
    _rows, report = build_readiness_audit(args.dry_run_qa_report, args.dry_run_qa_csv, args.dry_run_report, args.source_pack, args.family, args.out)
    write_json_output(report, out=args.json_out)
    if args.json: write_json_output(report)
    return 0 if report["valid"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
