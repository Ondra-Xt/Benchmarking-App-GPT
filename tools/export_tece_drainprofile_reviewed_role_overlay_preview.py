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
from tools.validate_tece_drainprofile_drain_body_length_role_review_csv import EXPECTED_ARTICLES, EXPECTED_FAMILY

EXPECTED_BASELINE_ROLE_COUNTS = {"accessory": 5, "complete_set": 4, "drain_body": 12, "profile_cover": 30, "unknown": 25}
EXPECTED_REVIEWED_SOURCE_ROLE_COUNTS = {"drain_body": 3, "unknown": 9}
EXPECTED_REVIEWED_ROLE_DIAGNOSTIC = {
    "673001": "drain_body_unresolved_length", "673002": "drain_body_unresolved_length", "673003": "drain_body_unresolved_length",
    "675004": "spare_part", "675005": "spare_part", "675006": "water_trap", "675008": "water_trap",
    "675009": "accessory", "675016": "spare_part", "675017": "spare_part", "675018": "water_trap", "675025": "profile_cover",
}
EXPECTED_CANONICAL_ROLE_CANDIDATE = {
    article: ("drain_body" if role == "drain_body_unresolved_length" else "profile_cover" if role == "profile_cover" else "accessory")
    for article, role in EXPECTED_REVIEWED_ROLE_DIAGNOSTIC.items()
}
EXPECTED_ROLE_OVERLAY_ACTION = {
    article: (
        "retain_drain_body_but_block_length_and_pairing"
        if role == "drain_body_unresolved_length"
        else f"diagnostic_reclassify_exported_drain_body_to_{role}"
    )
    for article, role in EXPECTED_REVIEWED_ROLE_DIAGNOSTIC.items()
}
CSV_COLUMNS = [
    "overlay_preview_id", "family", "article_number", "review_csv_article_role", "current_source_pack_role",
    "reviewer_decision", "reject_reason_tag", "reject_reason_detail", "reviewer_notes", "reviewed_nominal_length_mm",
    "reviewed_role_diagnostic", "canonical_role_candidate", "role_overlay_action", "role_overlay_apply_allowed",
    "length_overlay_allowed", "compatibility_pairing_allowed", "source_pack_mutation_allowed", "diagnostic_only",
    "production_safe", "production_promotion_blocked", "ready_for_benchmark", "ready_for_customer_view",
    "recommended_next_action", "production_status_note",
]
NEXT = "review this diagnostic overlay preview before designing a separate explicit role overlay; do not mutate source-pack or generate compatibility pairs"
STATUS = "diagnostic-only TECEdrainprofile reviewed role overlay preview; no source-pack mutation; no length overlay; no compatibility pairing; no Products/BOM/Final_Assemblies/Final_Set_Details/customer-view promotion"
NOTE = "This preview is read-only and diagnostic-only. It previews TECEdrainprofile reviewed role overlay candidates from validated manual review and impact evidence and does not mutate source-pack files, TECE logic, ACO export, Products, Comparison, BOM_Options, Final_Assemblies, or Final_Set_Details. Production promotion remains blocked."


def _read_csv(path: str | Path) -> list[dict[str, Any]]:
    with Path(path).open(encoding="utf-8-sig", newline="") as fh:
        return list(csv.DictReader(fh))


def _read_json(path: str | Path, errors: list[str], label: str) -> dict[str, Any]:
    try:
        return json.loads(Path(path).read_text(encoding="utf-8"))
    except Exception as exc:
        errors.append(f"{label} missing or cannot be loaded: {exc}")
        return {}


def _article(row: dict[str, Any]) -> str:
    for key in ("article_number", "article", "article_no", "article_id"):
        value = _clean(row.get(key))
        if value:
            return value
    return ""


def _role(row: Any) -> str:
    return _clean(getattr(row, "tece_article_role_candidate", "")) or _clean(getattr(row, "article_role", "")) or "unknown"


def _family(row: Any) -> str:
    return _clean(getattr(row, "tece_family_candidate", "")) or _clean(getattr(row, "product_family", ""))


def _review_role(row: dict[str, Any]) -> str:
    return _clean(row.get("review_csv_article_role") or row.get("original_article_role") or row.get("article_role"))


def _bool_text(value: bool) -> str:
    return "true" if value else "false"


def _duplicates(values: list[str]) -> list[str]:
    counts = Counter(values)
    return sorted(value for value, count in counts.items() if value and count > 1)


def _nonzero(counter: Counter[str]) -> dict[str, int]:
    return {key: value for key, value in sorted(counter.items()) if value}


def build_overlay_preview(source_pack: str | Path, review_csv: str | Path, reject_reason_csv: str | Path, validation_report: str | Path, impact_report: str | Path, out: str | Path | None = None, family: str = EXPECTED_FAMILY) -> dict[str, Any]:
    errors: list[str] = []
    warnings: list[str] = []
    try:
        inventory = load_source_pack(source_pack)
    except Exception as exc:
        inventory = None
        errors.append(f"source-pack cannot be loaded: {exc}")
    review_rows = _read_csv(review_csv)
    reject_rows = _read_csv(reject_reason_csv)
    validation = _read_json(validation_report, errors, "validation report")
    impact = _read_json(impact_report, errors, "impact report")
    validation_valid = validation.get("valid") is True
    impact_valid = impact.get("valid") is True
    if not validation_valid:
        errors.append("validation report valid must be true")
    if not impact_valid:
        errors.append("impact report valid must be true")
    if family != EXPECTED_FAMILY:
        errors.append("family must be TECEdrainprofile")

    inventory_rows = list(getattr(inventory, "rows", [])) if inventory else []
    input_count = int(getattr(inventory, "source_pack_candidate_count", len(inventory_rows))) if inventory else 0
    family_rows = [row for row in inventory_rows if _family(row) == family]
    role_counts = dict(sorted(Counter(_role(row) for row in family_rows).items()))
    by_article = {getattr(row, "article_number", ""): row for row in family_rows}
    reject_by_article = {_article(row): row for row in reject_rows}

    overlay_rows: list[dict[str, str]] = []
    for index, row in enumerate(review_rows, start=1):
        article = _article(row)
        reject = reject_by_article.get(article, {})
        diagnostic = EXPECTED_REVIEWED_ROLE_DIAGNOSTIC.get(article, "")
        overlay_rows.append({
            "overlay_preview_id": f"TECE-DP-ROLE-OVERLAY-PREVIEW-{index:06d}",
            "family": _clean(row.get("family")) or family,
            "article_number": article,
            "review_csv_article_role": _review_role(row),
            "current_source_pack_role": _role(by_article[article]) if article in by_article else "missing",
            "reviewer_decision": _clean(row.get("reviewer_decision") or reject.get("reviewer_decision")),
            "reject_reason_tag": _clean(row.get("reject_reason_tag") or reject.get("reject_reason_tag")),
            "reject_reason_detail": _clean(row.get("reject_reason_detail") or reject.get("reject_reason_detail")),
            "reviewer_notes": _clean(row.get("reviewer_notes")),
            "reviewed_nominal_length_mm": _clean(row.get("reviewed_nominal_length_mm")),
            "reviewed_role_diagnostic": diagnostic,
            "canonical_role_candidate": EXPECTED_CANONICAL_ROLE_CANDIDATE.get(article, ""),
            "role_overlay_action": EXPECTED_ROLE_OVERLAY_ACTION.get(article, ""),
            "role_overlay_apply_allowed": _bool_text(False),
            "length_overlay_allowed": _bool_text(False),
            "compatibility_pairing_allowed": _bool_text(False),
            "source_pack_mutation_allowed": _bool_text(False),
            "diagnostic_only": _bool_text(True),
            "production_safe": _bool_text(False),
            "production_promotion_blocked": _bool_text(True),
            "ready_for_benchmark": _bool_text(False),
            "ready_for_customer_view": _bool_text(False),
            "recommended_next_action": NEXT,
            "production_status_note": STATUS,
        })

    article_numbers = [row["article_number"] for row in overlay_rows]
    review_role_counts = _nonzero(Counter(row["review_csv_article_role"] for row in overlay_rows))
    reviewed_source_role_counts = _nonzero(Counter(row["current_source_pack_role"] for row in overlay_rows))
    invalid_family_rows = [row for row in overlay_rows if row["family"] != EXPECTED_FAMILY]
    invalid_review_role_rows = [row for row in overlay_rows if row["review_csv_article_role"] != "drain_body"]
    invalid_current_role_rows = [row for row in overlay_rows if row["current_source_pack_role"] != ("drain_body" if row["article_number"] in {"673001", "673002", "673003"} else "unknown")]
    invalid_diagnostic_rows = [row for row in overlay_rows if row["reviewed_role_diagnostic"] != EXPECTED_REVIEWED_ROLE_DIAGNOSTIC.get(row["article_number"])]
    invalid_canonical_rows = [row for row in overlay_rows if row["canonical_role_candidate"] != EXPECTED_CANONICAL_ROLE_CANDIDATE.get(row["article_number"])]
    invalid_action_rows = [row for row in overlay_rows if row["role_overlay_action"] != EXPECTED_ROLE_OVERLAY_ACTION.get(row["article_number"])]
    production_leakage_rows = [row for row in overlay_rows if row["production_safe"] == "true" or row["production_promotion_blocked"] != "true"]
    readiness_leakage_rows = [row for row in overlay_rows if row["ready_for_benchmark"] == "true" or row["ready_for_customer_view"] == "true"]
    diagnostic_only_leakage_rows = [row for row in overlay_rows if row["diagnostic_only"] != "true"]
    duplicate_ids = _duplicates([row["overlay_preview_id"] for row in overlay_rows])
    duplicate_articles = _duplicates(article_numbers)

    checks = [
        (input_count == 436, "input_inventory_row_count must be 436"),
        (len(family_rows) == 76, "family_inventory_row_count must be 76"),
        ({k: role_counts.get(k, 0) for k in EXPECTED_BASELINE_ROLE_COUNTS} == EXPECTED_BASELINE_ROLE_COUNTS, "current source-pack baseline role counts differ from expected"),
        (len(review_rows) == 12, "review row count must be 12"),
        (len(overlay_rows) == 12, "overlay preview row count must be 12"),
        (sorted(article_numbers) == EXPECTED_ARTICLES, "article set differs from expected 12 articles"),
        (review_role_counts.get("drain_body", 0) == 12, "review_csv_article_role must be drain_body for all rows"),
        ({k: reviewed_source_role_counts.get(k, 0) for k in EXPECTED_REVIEWED_SOURCE_ROLE_COUNTS} == EXPECTED_REVIEWED_SOURCE_ROLE_COUNTS, "reviewed article current source-pack role counts differ from expected"),
    ]
    for ok, message in checks:
        if not ok:
            errors.append(message)
    for rows, message in [
        (invalid_family_rows, "invalid family rows detected"), (invalid_review_role_rows, "invalid review CSV article role rows detected"),
        (invalid_current_role_rows, "invalid current source-pack role rows detected"), (invalid_diagnostic_rows, "invalid reviewed role diagnostic rows detected"),
        (invalid_canonical_rows, "invalid canonical role candidate rows detected"), (invalid_action_rows, "invalid role overlay action rows detected"),
        (production_leakage_rows, "production leakage detected"), (readiness_leakage_rows, "readiness leakage detected"),
        (diagnostic_only_leakage_rows, "diagnostic_only must be true for every row"), (duplicate_ids, "duplicate overlay preview IDs detected"),
        (duplicate_articles, "duplicate article numbers detected"),
    ]:
        if rows:
            errors.append(message)
    for field in ("role_overlay_apply_allowed", "length_overlay_allowed", "compatibility_pairing_allowed", "source_pack_mutation_allowed"):
        if any(row[field] == "true" for row in overlay_rows):
            errors.append(f"{field} must be false for every row")

    if out:
        Path(out).parent.mkdir(parents=True, exist_ok=True)
        with Path(out).open("w", encoding="utf-8-sig", newline="") as fh:
            writer = csv.DictWriter(fh, fieldnames=CSV_COLUMNS)
            writer.writeheader(); writer.writerows(overlay_rows)

    return {
        "valid": not errors, "errors": errors, "warnings": warnings, "source_pack_path": str(source_pack), "family": family,
        "input_inventory_row_count": input_count, "family_inventory_row_count": len(family_rows), "current_machine_role_counts": role_counts,
        "review_csv_path": str(review_csv), "reject_reason_csv_path": str(reject_reason_csv), "validation_report_path": str(validation_report),
        "impact_report_path": str(impact_report), "validation_report_valid": validation_valid, "impact_report_valid": impact_valid,
        "total_review_rows": len(review_rows), "overlay_preview_row_count": len(overlay_rows), "article_numbers": sorted(article_numbers),
        "review_csv_article_role_counts": review_role_counts, "current_source_pack_role_counts_for_reviewed_articles": reviewed_source_role_counts,
        "reviewer_decision_counts": _nonzero(Counter(row["reviewer_decision"] for row in overlay_rows)),
        "reject_reason_tag_counts": _nonzero(Counter(row["reject_reason_tag"] for row in overlay_rows)),
        "reviewed_role_diagnostic_counts": _nonzero(Counter(row["reviewed_role_diagnostic"] for row in overlay_rows)),
        "canonical_role_candidate_counts": _nonzero(Counter(row["canonical_role_candidate"] for row in overlay_rows)),
        "role_overlay_action_counts": _nonzero(Counter(row["role_overlay_action"] for row in overlay_rows)),
        "role_overlay_apply_allowed_count": sum(row["role_overlay_apply_allowed"] == "true" for row in overlay_rows),
        "length_overlay_allowed_count": sum(row["length_overlay_allowed"] == "true" for row in overlay_rows),
        "compatibility_pairing_allowed_count": sum(row["compatibility_pairing_allowed"] == "true" for row in overlay_rows),
        "source_pack_mutation_allowed_count": sum(row["source_pack_mutation_allowed"] == "true" for row in overlay_rows),
        "duplicate_overlay_preview_ids": duplicate_ids, "duplicate_article_numbers": duplicate_articles,
        "invalid_family_rows": invalid_family_rows, "invalid_review_csv_article_role_rows": invalid_review_role_rows,
        "invalid_current_source_pack_role_rows": invalid_current_role_rows, "invalid_reviewed_role_diagnostic_rows": invalid_diagnostic_rows,
        "invalid_canonical_role_candidate_rows": invalid_canonical_rows, "invalid_role_overlay_action_rows": invalid_action_rows,
        "production_leakage_rows": production_leakage_rows, "readiness_leakage_rows": readiness_leakage_rows,
        "diagnostic_only_leakage_rows": diagnostic_only_leakage_rows, "output_csv_path": str(out) if out else None,
        "proposed_source_pack_mutation_count": 0, "production_safe_candidate_count": 0,
        "production_promotion_blocked": not production_leakage_rows, "ready_for_benchmark": False, "ready_for_customer_view": False,
        "diagnostic_only_note": NOTE,
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Export read-only TECEdrainprofile reviewed role overlay preview.")
    parser.add_argument("--source-pack", default="local_source_packs/tece/pilot_001")
    parser.add_argument("--family", default=EXPECTED_FAMILY)
    parser.add_argument("--review-csv", required=True)
    parser.add_argument("--reject-reason-csv", required=True)
    parser.add_argument("--validation-report", required=True)
    parser.add_argument("--impact-report", required=True)
    parser.add_argument("--out", required=True)
    parser.add_argument("--json-out", required=True)
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args(argv)
    report = build_overlay_preview(args.source_pack, args.review_csv, args.reject_reason_csv, args.validation_report, args.impact_report, out=args.out, family=args.family)
    write_json_output(report, out=args.json_out)
    if args.json:
        write_json_output(report)
    return 0 if report["valid"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
