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
CSV_COLUMNS = [
    "impact_row_id", "family", "article_number", "review_csv_article_role", "current_source_pack_role",
    "current_machine_role", "reviewer_decision", "reject_reason_tag", "reject_reason_detail", "reviewer_notes",
    "reviewed_nominal_length_mm", "safe_to_apply_automatically", "impact_category",
    "proposed_corrected_role_diagnostic", "length_overlay_allowed", "compatibility_pairing_allowed",
    "source_pack_mutation_allowed", "diagnostic_only", "production_safe", "production_promotion_blocked",
    "ready_for_benchmark", "ready_for_customer_view", "recommended_next_action", "production_status_note",
]
NOTE = "This report is read-only and diagnostic-only. It reports the impact of TECEdrainprofile manual role review on current machine-classified drain_body rows and does not mutate source-pack files, TECE logic, ACO export, Products, Comparison, BOM_Options, Final_Assemblies, or Final_Set_Details. Production promotion remains blocked."
NEXT = "use this diagnostic impact report to design a separate reviewed role overlay; do not mutate source-pack or generate compatibility pairs"
STATUS = "diagnostic-only TECEdrainprofile role-correction impact; no source-pack mutation; no Products/BOM/Final_Assemblies/Final_Set_Details/customer-view promotion"


def _read_csv(path: str | Path) -> list[dict[str, Any]]:
    with Path(path).open(encoding="utf-8-sig", newline="") as fh:
        return list(csv.DictReader(fh))


def _article(row: dict[str, Any]) -> str:
    for key in ("article_number", "article", "article_no", "article_id"):
        value = _clean(row.get(key))
        if value:
            return value
    return ""


def _bool(value: Any) -> bool:
    return _clean(value).lower() == "true"


def _bool_text(value: bool) -> str:
    return "true" if value else "false"


def _duplicates(values: list[str]) -> list[str]:
    counts = Counter(values)
    return sorted(value for value, count in counts.items() if value and count > 1)


def _role(row: Any) -> str:
    return _clean(getattr(row, "tece_article_role_candidate", "")) or _clean(getattr(row, "article_role", "")) or "unknown"


def _family(row: Any) -> str:
    return _clean(getattr(row, "tece_family_candidate", "")) or _clean(getattr(row, "product_family", ""))


def _review_csv_role(row: dict[str, Any]) -> str:
    return _clean(row.get("review_csv_article_role") or row.get("original_article_role") or row.get("article_role"))


def _nonzero(counter: Counter[str] | dict[str, int]) -> dict[str, int]:
    return {key: value for key, value in sorted(dict(counter).items()) if value}


def build_impact_report(
    source_pack: str | Path,
    review_csv: str | Path,
    reject_reason_csv: str | Path,
    validation_report: str | Path,
    out: str | Path | None = None,
    family: str = EXPECTED_FAMILY,
) -> dict[str, Any]:
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

    inventory_rows = list(getattr(inventory, "rows", [])) if inventory else []
    input_count = int(getattr(inventory, "source_pack_candidate_count", len(inventory_rows))) if inventory else 0
    family_rows = [row for row in inventory_rows if _family(row) == family]
    role_counts = dict(sorted(Counter(_role(row) for row in family_rows).items()))
    inventory_by_article = {getattr(row, "article_number", ""): row for row in family_rows}
    reject_by_article = {_article(row): row for row in reject_rows}

    impact_rows: list[dict[str, str]] = []
    for index, row in enumerate(review_rows, start=1):
        article = _article(row)
        reject_row = reject_by_article.get(article, {})
        reject_tag = _clean(row.get("reject_reason_tag")) or _clean(reject_row.get("reject_reason_tag"))
        reviewer_decision = _clean(row.get("reviewer_decision"))
        source_pack_role = _role(inventory_by_article[article]) if article in inventory_by_article else "missing"
        review_role = _review_csv_role(row)
        impact_category = (
            "retain_as_drain_or_ablauf_without_length"
            if reviewer_decision == "drain_body_no_length_found"
            else "reject_machine_or_exported_drain_body_role"
        )
        proposed_role = "drain_body_unresolved_length" if reject_tag == "no_nominal_length" else reject_tag
        impact_rows.append({
            "impact_row_id": f"TECE-DP-ROLE-IMPACT-{index:06d}",
            "family": _clean(row.get("family")) or family,
            "article_number": article,
            "review_csv_article_role": review_role,
            "current_source_pack_role": source_pack_role,
            "current_machine_role": source_pack_role,
            "reviewer_decision": reviewer_decision,
            "reject_reason_tag": reject_tag,
            "reject_reason_detail": _clean(row.get("reject_reason_detail") or reject_row.get("reject_reason_detail")),
            "reviewer_notes": _clean(row.get("reviewer_notes")),
            "reviewed_nominal_length_mm": _clean(row.get("reviewed_nominal_length_mm")),
            "safe_to_apply_automatically": _bool_text(_bool(row.get("safe_to_apply_automatically"))),
            "impact_category": impact_category,
            "proposed_corrected_role_diagnostic": proposed_role,
            "length_overlay_allowed": _clean(row.get("length_overlay_allowed")) or "false",
            "compatibility_pairing_allowed": _clean(row.get("compatibility_pairing_allowed")) or "false",
            "source_pack_mutation_allowed": _clean(row.get("source_pack_mutation_allowed")) or "false",
            "diagnostic_only": _clean(row.get("diagnostic_only")) or "true",
            "production_safe": _clean(row.get("production_safe")) or "false",
            "production_promotion_blocked": _clean(row.get("production_promotion_blocked")) or "true",
            "ready_for_benchmark": _clean(row.get("ready_for_benchmark")) or "false",
            "ready_for_customer_view": _clean(row.get("ready_for_customer_view")) or "false",
            "recommended_next_action": NEXT,
            "production_status_note": STATUS,
        })

    article_numbers = [row["article_number"] for row in impact_rows]
    decision_counts = dict(sorted(Counter(row["reviewer_decision"] for row in impact_rows).items()))
    reject_tag_counts = dict(sorted(Counter(row["reject_reason_tag"] for row in impact_rows).items()))
    review_role_counts = dict(sorted(Counter(row["review_csv_article_role"] for row in impact_rows).items()))
    reviewed_source_role_counts = dict(sorted(Counter(row["current_source_pack_role"] for row in impact_rows).items()))
    source_pack_role_observation_rows = [
        {"article_number": row["article_number"], "review_csv_article_role": row["review_csv_article_role"], "current_source_pack_role": row["current_source_pack_role"]}
        for row in impact_rows
        if row["review_csv_article_role"] != row["current_source_pack_role"]
    ]

    invalid_family_rows = [row for row in impact_rows if row["family"] != EXPECTED_FAMILY]
    invalid_review_csv_role_rows = [row for row in impact_rows if row["review_csv_article_role"] != "drain_body"]
    invalid_decision_rows = [row for row in impact_rows if row["reviewer_decision"] not in {"drain_body_no_length_found", "not_drain_body"}]
    invalid_reject_reason_rows = [row for row in impact_rows if EXPECTED_REJECT_REASON_TAG_MAPPING.get(row["article_number"]) != row["reject_reason_tag"]]
    production_leakage_rows = [row for row in impact_rows if row["production_safe"] == "true" or row["production_promotion_blocked"] != "true"]
    readiness_leakage_rows = [row for row in impact_rows if row["ready_for_benchmark"] == "true" or row["ready_for_customer_view"] == "true"]
    diagnostic_only_leakage_rows = [row for row in impact_rows if row["diagnostic_only"] != "true"]

    checks = [
        (len(review_rows) == 12, "review row count must be 12"),
        (len(impact_rows) == 12, "impact row count must be 12"),
        (sorted(article_numbers) == EXPECTED_ARTICLES, "article set differs from expected 12 reviewed articles"),
        ({key: role_counts.get(key, 0) for key in EXPECTED_ROLE_COUNTS} == EXPECTED_ROLE_COUNTS, "current_machine_role_counts do not match expected TECEdrainprofile baseline"),
        ({"drain_body": review_role_counts.get("drain_body", 0)} == {"drain_body": 12}, "review CSV article_role must be drain_body for every row"),
        ({"drain_body_no_length_found": decision_counts.get("drain_body_no_length_found", 0), "not_drain_body": decision_counts.get("not_drain_body", 0)} == {"drain_body_no_length_found": 3, "not_drain_body": 9}, "decision counts do not match expected values"),
        ({key: reject_tag_counts.get(key, 0) for key in EXPECTED_REJECT_REASON_TAG_COUNTS} == EXPECTED_REJECT_REASON_TAG_COUNTS, "reject reason tag counts do not match expected values"),
    ]
    for ok, message in checks:
        if not ok:
            errors.append(message)
    if any(_bool(row["safe_to_apply_automatically"]) for row in impact_rows):
        errors.append("safe_to_apply_true_count must be 0")
    if any(row["reviewed_nominal_length_mm"] for row in impact_rows):
        errors.append("reviewed_nominal_length_filled_count must be 0")
    for field in ("length_overlay_allowed", "compatibility_pairing_allowed", "source_pack_mutation_allowed"):
        if any(row[field] == "true" for row in impact_rows):
            errors.append(f"{field} must be false for every row")
    if production_leakage_rows:
        errors.append("production leakage detected")
    if readiness_leakage_rows:
        errors.append("readiness leakage detected")
    if diagnostic_only_leakage_rows:
        errors.append("diagnostic_only must be true for every row")
    if invalid_family_rows:
        errors.append("invalid family rows detected")
    if invalid_review_csv_role_rows:
        errors.append("invalid review CSV article role rows detected")
    if invalid_decision_rows:
        errors.append("invalid decision rows detected")
    if invalid_reject_reason_rows:
        errors.append("invalid reject reason rows detected")

    duplicate_impact_row_ids = _duplicates([row["impact_row_id"] for row in impact_rows])
    duplicate_article_numbers = _duplicates(article_numbers)
    if duplicate_impact_row_ids:
        errors.append("duplicate impact row IDs detected")
    if duplicate_article_numbers:
        errors.append("duplicate article numbers detected")

    if out:
        Path(out).parent.mkdir(parents=True, exist_ok=True)
        with Path(out).open("w", encoding="utf-8", newline="") as fh:
            writer = csv.DictWriter(fh, fieldnames=CSV_COLUMNS)
            writer.writeheader()
            writer.writerows(impact_rows)

    report = {
        "valid": not errors,
        "errors": errors,
        "warnings": warnings,
        "source_pack_path": str(source_pack),
        "family": family,
        "input_inventory_row_count": input_count,
        "family_inventory_row_count": len(family_rows),
        "current_machine_role_counts": role_counts,
        "review_csv_path": str(review_csv),
        "reject_reason_csv_path": str(reject_reason_csv),
        "validation_report_path": str(validation_report),
        "validation_report_valid": validation_valid,
        "total_review_rows": len(review_rows),
        "impact_row_count": len(impact_rows),
        "reviewed_machine_drain_body_rows": review_role_counts.get("drain_body", 0),
        "review_confirmed_drain_or_ablauf_rows": decision_counts.get("drain_body_no_length_found", 0),
        "review_rejected_not_drain_body_rows": decision_counts.get("not_drain_body", 0),
        "decision_counts": decision_counts,
        "reject_reason_tag_counts": reject_tag_counts,
        "impact_category_counts": dict(sorted(Counter(row["impact_category"] for row in impact_rows).items())),
        "proposed_corrected_role_diagnostic_counts": dict(sorted(Counter(row["proposed_corrected_role_diagnostic"] for row in impact_rows).items())),
        "confirmed_drain_body_length_rows": decision_counts.get("confirmed_drain_body_length", 0),
        "safe_to_apply_true_count": sum(1 for row in impact_rows if _bool(row["safe_to_apply_automatically"])),
        "reviewed_nominal_length_filled_count": sum(1 for row in impact_rows if row["reviewed_nominal_length_mm"]),
        "length_overlay_allowed_count": sum(1 for row in impact_rows if row["length_overlay_allowed"] == "true"),
        "compatibility_pairing_allowed_count": sum(1 for row in impact_rows if row["compatibility_pairing_allowed"] == "true"),
        "source_pack_mutation_allowed_count": sum(1 for row in impact_rows if row["source_pack_mutation_allowed"] == "true"),
        "review_csv_article_role_counts": review_role_counts,
        "reviewed_article_source_pack_role_counts": reviewed_source_role_counts,
        "source_pack_role_observation_rows": source_pack_role_observation_rows,
        "duplicate_impact_row_ids": duplicate_impact_row_ids,
        "duplicate_article_numbers": duplicate_article_numbers,
        "invalid_family_rows": invalid_family_rows,
        "invalid_current_machine_role_rows": [],
        "invalid_review_csv_article_role_rows": invalid_review_csv_role_rows,
        "invalid_decision_rows": invalid_decision_rows,
        "invalid_reject_reason_rows": invalid_reject_reason_rows,
        "production_leakage_rows": production_leakage_rows,
        "readiness_leakage_rows": readiness_leakage_rows,
        "diagnostic_only_leakage_rows": diagnostic_only_leakage_rows,
        "output_csv_path": str(out) if out else None,
        "proposed_source_pack_mutation_count": 0,
        "production_safe_candidate_count": 0,
        "production_promotion_blocked": not production_leakage_rows,
        "ready_for_benchmark": False,
        "ready_for_customer_view": False,
        "diagnostic_only_note": NOTE,
    }
    return report


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Build read-only TECEdrainprofile role-correction impact report.")
    parser.add_argument("--source-pack", default="local_source_packs/tece/pilot_001")
    parser.add_argument("--family", default=EXPECTED_FAMILY)
    parser.add_argument("--review-csv", required=True)
    parser.add_argument("--reject-reason-csv", required=True)
    parser.add_argument("--validation-report", required=True)
    parser.add_argument("--out", required=True)
    parser.add_argument("--json-out", required=True)
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args(argv)
    report = build_impact_report(args.source_pack, args.review_csv, args.reject_reason_csv, args.validation_report, out=args.out, family=args.family)
    write_json_output(report, out=args.json_out)
    if args.json:
        write_json_output(report)
    return 0 if report["valid"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
