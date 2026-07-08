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

from tools.report_tece_unknown_role_contexts import _clean
from tools.tece_report_output import write_json_output
from tools.validate_tece_drainprofile_drain_body_length_role_review_csv import EXPECTED_ARTICLES, EXPECTED_FAMILY

EXPECTED_CURRENT_SOURCE_PACK_ROLE = {
    article: ("drain_body" if article in {"673001", "673002", "673003"} else "unknown")
    for article in EXPECTED_ARTICLES
}
EXPECTED_REVIEWED_ROLE_DIAGNOSTIC = {
    "673001": "drain_body_unresolved_length", "673002": "drain_body_unresolved_length", "673003": "drain_body_unresolved_length",
    "675004": "spare_part", "675005": "spare_part", "675006": "water_trap", "675008": "water_trap",
    "675009": "accessory", "675016": "spare_part", "675017": "spare_part", "675018": "water_trap", "675025": "profile_cover",
}
EXPECTED_CANONICAL_ROLE_CANDIDATE = {
    article: ("drain_body" if diagnostic == "drain_body_unresolved_length" else "profile_cover" if diagnostic == "profile_cover" else "accessory")
    for article, diagnostic in EXPECTED_REVIEWED_ROLE_DIAGNOSTIC.items()
}
EXPECTED_ROLE_OVERLAY_ACTION = {
    article: (
        "retain_drain_body_but_block_length_and_pairing"
        if diagnostic == "drain_body_unresolved_length"
        else f"diagnostic_reclassify_exported_drain_body_to_{diagnostic}"
    )
    for article, diagnostic in EXPECTED_REVIEWED_ROLE_DIAGNOSTIC.items()
}
EXPECTED_PREVIEW_IDS = [f"TECE-DP-ROLE-OVERLAY-PREVIEW-{index:06d}" for index in range(1, 13)]
QA_COLUMNS = [
    "qa_row_id", "family", "article_number", "overlay_preview_id", "review_csv_article_role", "current_source_pack_role",
    "reviewed_role_diagnostic", "canonical_role_candidate", "role_overlay_action", "expected_current_source_pack_role",
    "expected_reviewed_role_diagnostic", "expected_canonical_role_candidate", "expected_role_overlay_action", "mapping_status",
    "blocking_status", "qa_status", "diagnostic_only", "production_safe", "production_promotion_blocked", "ready_for_benchmark",
    "ready_for_customer_view", "recommended_next_action", "production_status_note",
]
RECOMMENDED_NEXT_ACTION = "QA passed for diagnostic overlay preview; keep as evidence before designing an explicit non-mutating role overlay workflow"
PRODUCTION_STATUS_NOTE = "diagnostic-only QA for TECEdrainprofile reviewed role overlay preview; no source-pack mutation; no length overlay; no compatibility pairing; no Products/BOM/Final_Assemblies/Final_Set_Details/customer-view promotion"
DIAGNOSTIC_ONLY_NOTE = "This QA report is read-only and diagnostic-only. It validates the TECEdrainprofile reviewed role overlay preview mapping and blocking state and does not mutate source-pack files, TECE logic, ACO export, Products, Comparison, BOM_Options, Final_Assemblies, or Final_Set_Details. Production promotion remains blocked."


def _read_csv(path: str | Path) -> list[dict[str, str]]:
    with Path(path).open(encoding="utf-8-sig", newline="") as fh:
        return list(csv.DictReader(fh))


def _read_json(path: str | Path, errors: list[str]) -> dict[str, Any]:
    try:
        return json.loads(Path(path).read_text(encoding="utf-8"))
    except Exception as exc:
        errors.append(f"preview report missing or cannot be loaded: {exc}")
        return {}


def _bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    return _clean(value).lower() in {"true", "1", "yes", "y"}


def _bool_text(value: bool) -> str:
    return "true" if value else "false"


def _duplicates(values: list[str]) -> list[str]:
    counts = Counter(values)
    return sorted(value for value, count in counts.items() if value and count > 1)


def _counts(values: list[str]) -> dict[str, int]:
    return {key: count for key, count in sorted(Counter(values).items()) if key and count}


def build_qa_report(preview_csv: str | Path, preview_report: str | Path, out: str | Path | None = None, family: str = EXPECTED_FAMILY) -> dict[str, Any]:
    errors: list[str] = []
    warnings: list[str] = []
    rows = _read_csv(preview_csv)
    report = _read_json(preview_report, errors)
    preview_report_valid = report.get("valid") is True
    if not preview_report_valid:
        errors.append("preview report valid must be true")
    if family != EXPECTED_FAMILY:
        errors.append("family must be TECEdrainprofile")

    qa_rows: list[dict[str, str]] = []
    invalid_family_rows: list[dict[str, str]] = []
    invalid_overlay_preview_id_rows: list[dict[str, str]] = []
    invalid_article_mapping_rows: list[dict[str, str]] = []
    invalid_blocking_rows: list[dict[str, str]] = []
    production_leakage_rows: list[dict[str, str]] = []
    readiness_leakage_rows: list[dict[str, str]] = []
    diagnostic_only_leakage_rows: list[dict[str, str]] = []

    for index, row in enumerate(rows, start=1):
        article = _clean(row.get("article_number"))
        expected_current = EXPECTED_CURRENT_SOURCE_PACK_ROLE.get(article, "")
        expected_diagnostic = EXPECTED_REVIEWED_ROLE_DIAGNOSTIC.get(article, "")
        expected_canonical = EXPECTED_CANONICAL_ROLE_CANDIDATE.get(article, "")
        expected_action = EXPECTED_ROLE_OVERLAY_ACTION.get(article, "")
        actual_family = _clean(row.get("family"))
        overlay_id = _clean(row.get("overlay_preview_id"))
        expected_id = f"TECE-DP-ROLE-OVERLAY-PREVIEW-{index:06d}"
        mapping_ok = (
            _clean(row.get("review_csv_article_role")) == "drain_body"
            and _clean(row.get("current_source_pack_role")) == expected_current
            and _clean(row.get("reviewed_role_diagnostic")) == expected_diagnostic
            and _clean(row.get("canonical_role_candidate")) == expected_canonical
            and _clean(row.get("role_overlay_action")) == expected_action
        )
        blocking_ok = (
            not _bool(row.get("role_overlay_apply_allowed"))
            and not _bool(row.get("length_overlay_allowed"))
            and not _bool(row.get("compatibility_pairing_allowed"))
            and not _bool(row.get("source_pack_mutation_allowed"))
            and _bool(row.get("diagnostic_only"))
            and not _bool(row.get("production_safe"))
            and _bool(row.get("production_promotion_blocked"))
            and not _bool(row.get("ready_for_benchmark"))
            and not _bool(row.get("ready_for_customer_view"))
        )
        qa = {
            "qa_row_id": f"TECE-DP-ROLE-OVERLAY-QA-{index:06d}", "family": actual_family, "article_number": article,
            "overlay_preview_id": overlay_id, "review_csv_article_role": _clean(row.get("review_csv_article_role")),
            "current_source_pack_role": _clean(row.get("current_source_pack_role")), "reviewed_role_diagnostic": _clean(row.get("reviewed_role_diagnostic")),
            "canonical_role_candidate": _clean(row.get("canonical_role_candidate")), "role_overlay_action": _clean(row.get("role_overlay_action")),
            "expected_current_source_pack_role": expected_current, "expected_reviewed_role_diagnostic": expected_diagnostic,
            "expected_canonical_role_candidate": expected_canonical, "expected_role_overlay_action": expected_action,
            "mapping_status": "pass" if mapping_ok else "fail", "blocking_status": "pass" if blocking_ok else "fail",
            "qa_status": "pass" if mapping_ok and blocking_ok and actual_family == EXPECTED_FAMILY and overlay_id == expected_id else "fail",
            "diagnostic_only": _bool_text(_bool(row.get("diagnostic_only"))), "production_safe": _bool_text(_bool(row.get("production_safe"))),
            "production_promotion_blocked": _bool_text(_bool(row.get("production_promotion_blocked"))),
            "ready_for_benchmark": _bool_text(_bool(row.get("ready_for_benchmark"))), "ready_for_customer_view": _bool_text(_bool(row.get("ready_for_customer_view"))),
            "recommended_next_action": RECOMMENDED_NEXT_ACTION, "production_status_note": PRODUCTION_STATUS_NOTE,
        }
        qa_rows.append(qa)
        if actual_family != EXPECTED_FAMILY: invalid_family_rows.append(qa)
        if overlay_id != expected_id: invalid_overlay_preview_id_rows.append(qa)
        if not mapping_ok: invalid_article_mapping_rows.append(qa)
        if not blocking_ok: invalid_blocking_rows.append(qa)
        if _bool(row.get("production_safe")) or not _bool(row.get("production_promotion_blocked")): production_leakage_rows.append(qa)
        if _bool(row.get("ready_for_benchmark")) or _bool(row.get("ready_for_customer_view")): readiness_leakage_rows.append(qa)
        if not _bool(row.get("diagnostic_only")): diagnostic_only_leakage_rows.append(qa)

    article_numbers = [row["article_number"] for row in qa_rows]
    overlay_ids = [row["overlay_preview_id"] for row in qa_rows]
    duplicate_overlay_ids = _duplicates(overlay_ids)
    duplicate_articles = _duplicates(article_numbers)

    checks = [
        (len(rows) == 12, "preview row count must be 12"), (len(qa_rows) == 12, "QA row count must be 12"),
        (sorted(article_numbers) == EXPECTED_ARTICLES, "article set differs from expected 12 articles"),
        (overlay_ids == EXPECTED_PREVIEW_IDS, "overlay_preview_id sequence differs from expected deterministic IDs"),
        (not duplicate_overlay_ids, "duplicate overlay_preview_id detected"), (not duplicate_articles, "duplicate article_number detected"),
        (_counts([row["review_csv_article_role"] for row in qa_rows]).get("drain_body", 0) == 12, "review_csv_article_role must be drain_body for all rows"),
        (not invalid_family_rows, "invalid family rows detected"), (not invalid_article_mapping_rows, "invalid article mapping rows detected"),
        (not invalid_blocking_rows, "invalid blocking rows detected"), (not production_leakage_rows, "production leakage detected"),
        (not readiness_leakage_rows, "readiness leakage detected"), (not diagnostic_only_leakage_rows, "diagnostic_only must be true for every row"),
        (all(row["mapping_status"] == "pass" for row in qa_rows), "mapping_status must be pass for every row"),
        (all(row["blocking_status"] == "pass" for row in qa_rows), "blocking_status must be pass for every row"),
        (all(row["qa_status"] == "pass" for row in qa_rows), "qa_status must be pass for every row"),
    ]
    for ok, message in checks:
        if not ok and message not in errors:
            errors.append(message)

    if out:
        Path(out).parent.mkdir(parents=True, exist_ok=True)
        with Path(out).open("w", encoding="utf-8-sig", newline="") as fh:
            writer = csv.DictWriter(fh, fieldnames=QA_COLUMNS)
            writer.writeheader(); writer.writerows(qa_rows)

    return {
        "valid": not errors, "errors": errors, "warnings": warnings, "family": family, "preview_csv_path": str(preview_csv),
        "preview_report_path": str(preview_report), "preview_report_valid": preview_report_valid, "total_preview_rows": len(rows),
        "qa_row_count": len(qa_rows), "article_numbers": sorted(article_numbers), "overlay_preview_id_count": len(set(overlay_ids)),
        "duplicate_overlay_preview_ids": duplicate_overlay_ids, "duplicate_article_numbers": duplicate_articles,
        "review_csv_article_role_counts": _counts([row["review_csv_article_role"] for row in qa_rows]),
        "current_source_pack_role_counts": _counts([row["current_source_pack_role"] for row in qa_rows]),
        "reviewed_role_diagnostic_counts": _counts([row["reviewed_role_diagnostic"] for row in qa_rows]),
        "canonical_role_candidate_counts": _counts([row["canonical_role_candidate"] for row in qa_rows]),
        "role_overlay_action_counts": _counts([row["role_overlay_action"] for row in qa_rows]),
        "mapping_status_counts": _counts([row["mapping_status"] for row in qa_rows]), "blocking_status_counts": _counts([row["blocking_status"] for row in qa_rows]),
        "qa_status_counts": _counts([row["qa_status"] for row in qa_rows]), "invalid_family_rows": invalid_family_rows,
        "invalid_overlay_preview_id_rows": invalid_overlay_preview_id_rows, "invalid_article_mapping_rows": invalid_article_mapping_rows,
        "invalid_blocking_rows": invalid_blocking_rows, "production_leakage_rows": production_leakage_rows,
        "readiness_leakage_rows": readiness_leakage_rows, "diagnostic_only_leakage_rows": diagnostic_only_leakage_rows,
        "role_overlay_apply_allowed_count": sum(_bool(row.get("role_overlay_apply_allowed")) for row in rows),
        "length_overlay_allowed_count": sum(_bool(row.get("length_overlay_allowed")) for row in rows),
        "compatibility_pairing_allowed_count": sum(_bool(row.get("compatibility_pairing_allowed")) for row in rows),
        "source_pack_mutation_allowed_count": sum(_bool(row.get("source_pack_mutation_allowed")) for row in rows),
        "proposed_source_pack_mutation_count": sum(_bool(row.get("proposed_source_pack_mutation")) for row in rows),
        "production_safe_candidate_count": sum(_bool(row.get("production_safe")) for row in rows),
        "production_promotion_blocked": not production_leakage_rows, "ready_for_benchmark": any(_bool(row.get("ready_for_benchmark")) for row in rows),
        "ready_for_customer_view": any(_bool(row.get("ready_for_customer_view")) for row in rows), "output_csv_path": str(out) if out else None,
        "diagnostic_only_note": DIAGNOSTIC_ONLY_NOTE,
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Build read-only QA report for TECEdrainprofile reviewed role overlay preview CSV.")
    parser.add_argument("--preview-csv", required=True)
    parser.add_argument("--preview-report", required=True)
    parser.add_argument("--family", default=EXPECTED_FAMILY)
    parser.add_argument("--out", required=True)
    parser.add_argument("--json-out", required=True)
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args(argv)
    report = build_qa_report(args.preview_csv, args.preview_report, out=args.out, family=args.family)
    write_json_output(report, out=args.json_out)
    if args.json:
        write_json_output(report)
    return 0 if report["valid"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
