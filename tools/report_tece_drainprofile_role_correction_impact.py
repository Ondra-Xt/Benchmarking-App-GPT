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

from tools.report_tece_source_inventory import load_source_pack
from tools.report_tece_unknown_role_contexts import _clean
from tools.tece_report_output import write_json_output
from tools.validate_tece_drainprofile_drain_body_length_role_review_csv import (
    EXPECTED_ARTICLES,
    EXPECTED_FAMILY,
    EXPECTED_REJECT_REASON_TAG_COUNTS,
    EXPECTED_REJECT_REASON_TAG_MAPPING,
)

EXPECTED_ROLE_COUNTS = {"drain_body": 12, "profile_cover": 30, "accessory": 5, "complete_set": 4, "unknown": 25}
CSV_COLUMNS = ["impact_row_id","family","article_number","current_machine_role","reviewer_decision","reject_reason_tag","reject_reason_detail","reviewer_notes","reviewed_nominal_length_mm","safe_to_apply_automatically","impact_category","proposed_corrected_role_diagnostic","length_overlay_allowed","compatibility_pairing_allowed","source_pack_mutation_allowed","diagnostic_only","production_safe","production_promotion_blocked","ready_for_benchmark","ready_for_customer_view","recommended_next_action","production_status_note"]
NOTE = "This report is read-only and diagnostic-only. It reports the impact of TECEdrainprofile manual role review on current machine-classified drain_body rows and does not mutate source-pack files, TECE logic, ACO export, Products, Comparison, BOM_Options, Final_Assemblies, or Final_Set_Details. Production promotion remains blocked."
NEXT = "use this diagnostic impact report to design a separate reviewed role overlay; do not mutate source-pack or generate compatibility pairs"
STATUS = "diagnostic-only TECEdrainprofile role-correction impact; no source-pack mutation; no Products/BOM/Final_Assemblies/Final_Set_Details/customer-view promotion"


def _read_csv(path: str | Path) -> list[dict[str, Any]]:
    with Path(path).open(encoding="utf-8-sig", newline="") as fh:
        return list(csv.DictReader(fh))


def _article(row: dict[str, Any]) -> str:
    for key in ("article_number", "article", "article_no", "article_id"):
        if _clean(row.get(key)):
            return _clean(row.get(key))
    return ""


def _b(value: Any) -> bool:
    return _clean(value).lower() == "true"


def _s(value: bool) -> str:
    return "true" if value else "false"


def _dupes(values: list[str]) -> list[str]:
    c = Counter(values)
    return sorted(k for k, v in c.items() if k and v > 1)


def _role(row: Any) -> str:
    return _clean(getattr(row, "tece_article_role_candidate", "")) or _clean(getattr(row, "article_role", "")) or "unknown"


def _family(row: Any) -> str:
    return _clean(getattr(row, "tece_family_candidate", "")) or _clean(getattr(row, "product_family", ""))


def build_impact_report(source_pack: str | Path, review_csv: str | Path, reject_reason_csv: str | Path, validation_report: str | Path, out: str | Path | None = None, family: str = EXPECTED_FAMILY) -> dict[str, Any]:
    errors: list[str] = []
    warnings: list[str] = []
    inventory = None
    try:
        inventory = load_source_pack(source_pack)
    except Exception as exc:
        errors.append(f"source-pack cannot be loaded: {exc}")
    review_rows = _read_csv(review_csv)
    reject_rows = _read_csv(reject_reason_csv)
    try:
        validation = json.loads(Path(validation_report).read_text(encoding="utf-8"))
    except Exception as exc:
        validation = {}
        errors.append(f"validation report is missing or cannot be loaded: {exc}")
    validation_valid = validation.get("valid") is True
    if not validation_valid:
        errors.append("validation report valid must be true")
    if family != EXPECTED_FAMILY:
        errors.append("family must be TECEdrainprofile")

    inv_rows = list(getattr(inventory, "rows", [])) if inventory else []
    input_count = int(getattr(inventory, "source_pack_candidate_count", len(inv_rows))) if inventory else 0
    fam_rows = [r for r in inv_rows if _family(r) == family]
    role_counts = dict(sorted(Counter(_role(r) for r in fam_rows).items()))
    by_article = {getattr(r, "article_number", ""): r for r in fam_rows}

    reject_by_article = {_article(r): r for r in reject_rows}
    impact_rows: list[dict[str, str]] = []
    for idx, row in enumerate(review_rows, start=1):
        article = _article(row)
        tag = _clean(row.get("reject_reason_tag")) or _clean(reject_by_article.get(article, {}).get("reject_reason_tag"))
        decision = _clean(row.get("reviewer_decision"))
        category = "retain_as_drain_or_ablauf_without_length" if decision == "drain_body_no_length_found" else "reject_machine_drain_body_role"
        proposed = "drain_body_unresolved_length" if tag == "no_nominal_length" else tag
        impact_rows.append({
            "impact_row_id": f"TECE-DP-ROLE-IMPACT-{idx:06d}", "family": _clean(row.get("family")) or family, "article_number": article,
            "current_machine_role": _clean(row.get("current_machine_role") or row.get("original_article_role") or row.get("article_role")),
            "reviewer_decision": decision, "reject_reason_tag": tag, "reject_reason_detail": _clean(row.get("reject_reason_detail") or reject_by_article.get(article, {}).get("reject_reason_detail")),
            "reviewer_notes": _clean(row.get("reviewer_notes")), "reviewed_nominal_length_mm": _clean(row.get("reviewed_nominal_length_mm")),
            "safe_to_apply_automatically": _s(_b(row.get("safe_to_apply_automatically"))), "impact_category": category, "proposed_corrected_role_diagnostic": proposed,
            "length_overlay_allowed": _clean(row.get("length_overlay_allowed")) or "false", "compatibility_pairing_allowed": _clean(row.get("compatibility_pairing_allowed")) or "false", "source_pack_mutation_allowed": _clean(row.get("source_pack_mutation_allowed")) or "false", "diagnostic_only": _clean(row.get("diagnostic_only")) or "true", "production_safe": _clean(row.get("production_safe")) or "false", "production_promotion_blocked": _clean(row.get("production_promotion_blocked")) or "true", "ready_for_benchmark": _clean(row.get("ready_for_benchmark")) or "false", "ready_for_customer_view": _clean(row.get("ready_for_customer_view")) or "false",
            "recommended_next_action": NEXT, "production_status_note": STATUS,
        })

    articles = [r["article_number"] for r in impact_rows]
    decision_counts = dict(sorted(Counter(r["reviewer_decision"] for r in impact_rows).items()))
    tag_counts = dict(sorted(Counter(r["reject_reason_tag"] for r in impact_rows).items()))
    invalid_family = [r for r in impact_rows if r["family"] != EXPECTED_FAMILY]
    invalid_role = [r for r in impact_rows if r["current_machine_role"] != "drain_body"]
    invalid_role.extend({"article_number": a, "current_machine_role": _role(by_article[a])} for a in EXPECTED_ARTICLES if a in by_article and _role(by_article[a]) != "drain_body")
    invalid_decision = [r for r in impact_rows if r["reviewer_decision"] not in {"drain_body_no_length_found", "not_drain_body"}]
    invalid_reject = [r for r in impact_rows if EXPECTED_REJECT_REASON_TAG_MAPPING.get(r["article_number"]) != r["reject_reason_tag"]]
    production_leakage = [r for r in impact_rows if r["production_safe"] == "true" or r["production_promotion_blocked"] != "true"]
    readiness_leakage = [r for r in impact_rows if r["ready_for_benchmark"] == "true" or r["ready_for_customer_view"] == "true"]
    diagnostic_leakage = [r for r in impact_rows if r["diagnostic_only"] != "true"]

    checks = [
        (len(review_rows) == 12, "review row count must be 12"), (len(impact_rows) == 12, "impact row count must be 12"),
        (sorted(articles) == EXPECTED_ARTICLES, "article set differs from expected 12 reviewed articles"),
        (all(a in by_article and _role(by_article[a]) == "drain_body" for a in EXPECTED_ARTICLES), "current source-pack inventory must contain expected articles as machine drain_body"),
        ({k: role_counts.get(k,0) for k in EXPECTED_ROLE_COUNTS} == EXPECTED_ROLE_COUNTS, "current_machine_role_counts do not match expected TECEdrainprofile baseline"),
        ({"drain_body_no_length_found": decision_counts.get("drain_body_no_length_found",0), "not_drain_body": decision_counts.get("not_drain_body",0)} == {"drain_body_no_length_found":3,"not_drain_body":9}, "decision counts do not match expected values"),
        ({k: tag_counts.get(k,0) for k in EXPECTED_REJECT_REASON_TAG_COUNTS} == EXPECTED_REJECT_REASON_TAG_COUNTS, "reject reason tag counts do not match expected values"),
    ]
    for ok, msg in checks:
        if not ok: errors.append(msg)
    if any(_b(r["safe_to_apply_automatically"]) for r in impact_rows): errors.append("safe_to_apply_true_count must be 0")
    if any(r["reviewed_nominal_length_mm"] for r in impact_rows): errors.append("reviewed_nominal_length_filled_count must be 0")
    for field in ("length_overlay_allowed","compatibility_pairing_allowed","source_pack_mutation_allowed"):
        if any(r[field] == "true" for r in impact_rows): errors.append(f"{field} must be false for every row")
    if production_leakage: errors.append("production leakage detected")
    if readiness_leakage: errors.append("readiness leakage detected")
    if diagnostic_leakage: errors.append("diagnostic_only must be true for every row")
    if invalid_family: errors.append("invalid family rows detected")
    if invalid_role: errors.append("invalid current machine role rows detected")
    if invalid_decision: errors.append("invalid decision rows detected")
    if invalid_reject: errors.append("invalid reject reason rows detected")

    if out:
        Path(out).parent.mkdir(parents=True, exist_ok=True)
        with Path(out).open("w", encoding="utf-8", newline="") as fh:
            w = csv.DictWriter(fh, fieldnames=CSV_COLUMNS); w.writeheader(); w.writerows(impact_rows)

    report = {"valid": not errors, "errors": errors, "warnings": warnings, "source_pack_path": str(source_pack), "family": family, "input_inventory_row_count": input_count, "family_inventory_row_count": len(fam_rows), "current_machine_role_counts": role_counts, "review_csv_path": str(review_csv), "reject_reason_csv_path": str(reject_reason_csv), "validation_report_path": str(validation_report), "validation_report_valid": validation_valid, "total_review_rows": len(review_rows), "impact_row_count": len(impact_rows), "reviewed_machine_drain_body_rows": sum(1 for r in impact_rows if r["current_machine_role"] == "drain_body"), "review_confirmed_drain_or_ablauf_rows": decision_counts.get("drain_body_no_length_found", 0), "review_rejected_not_drain_body_rows": decision_counts.get("not_drain_body", 0), "decision_counts": decision_counts, "reject_reason_tag_counts": tag_counts, "impact_category_counts": dict(sorted(Counter(r["impact_category"] for r in impact_rows).items())), "proposed_corrected_role_diagnostic_counts": dict(sorted(Counter(r["proposed_corrected_role_diagnostic"] for r in impact_rows).items())), "confirmed_drain_body_length_rows": decision_counts.get("confirmed_drain_body_length", 0), "safe_to_apply_true_count": sum(1 for r in impact_rows if _b(r["safe_to_apply_automatically"])), "reviewed_nominal_length_filled_count": sum(1 for r in impact_rows if r["reviewed_nominal_length_mm"]), "length_overlay_allowed_count": sum(1 for r in impact_rows if r["length_overlay_allowed"] == "true"), "compatibility_pairing_allowed_count": sum(1 for r in impact_rows if r["compatibility_pairing_allowed"] == "true"), "source_pack_mutation_allowed_count": sum(1 for r in impact_rows if r["source_pack_mutation_allowed"] == "true"), "duplicate_impact_row_ids": _dupes([r["impact_row_id"] for r in impact_rows]), "duplicate_article_numbers": _dupes(articles), "invalid_family_rows": invalid_family, "invalid_current_machine_role_rows": invalid_role, "invalid_decision_rows": invalid_decision, "invalid_reject_reason_rows": invalid_reject, "production_leakage_rows": production_leakage, "readiness_leakage_rows": readiness_leakage, "diagnostic_only_leakage_rows": diagnostic_leakage, "output_csv_path": str(out) if out else None, "proposed_source_pack_mutation_count": 0, "production_safe_candidate_count": 0, "production_promotion_blocked": not production_leakage, "ready_for_benchmark": False, "ready_for_customer_view": False, "diagnostic_only_note": NOTE}
    report["valid"] = not report["errors"] and not report["duplicate_impact_row_ids"] and not report["duplicate_article_numbers"]
    return report


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description="Build read-only TECEdrainprofile role-correction impact report.")
    p.add_argument("--source-pack", default="local_source_packs/tece/pilot_001")
    p.add_argument("--family", default=EXPECTED_FAMILY)
    p.add_argument("--review-csv", required=True); p.add_argument("--reject-reason-csv", required=True); p.add_argument("--validation-report", required=True)
    p.add_argument("--out", required=True); p.add_argument("--json-out", required=True); p.add_argument("--json", action="store_true")
    a = p.parse_args(argv)
    report = build_impact_report(a.source_pack, a.review_csv, a.reject_reason_csv, a.validation_report, out=a.out, family=a.family)
    write_json_output(report, out=a.json_out)
    if a.json: write_json_output(report)
    return 0 if report["valid"] else 1

if __name__ == "__main__":
    raise SystemExit(main())
