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
ROW_ID_PREFIX = "TECE-DP-ABLAUF-DUSCHPROFIL-EVIDENCE-V3"
EXPECTED_ROW_IDS = [f"{ROW_ID_PREFIX}-{i:06d}" for i in range(1, 35)]
RETAINED_DRAIN_BODY_ARTICLES = ["673001", "673002", "673003"]
INSTALLABLE_DUSCHPROFIL_ARTICLES = [
    "670800", "670810", "670900", "670910", "671000", "671010", "671200", "671210", "671600",
    "670821", "670921", "671021", "671221", "671621",
    "670801", "670802", "670812", "670803",
    "670901", "670902", "670912", "670903",
    "671001", "671002", "671012", "671003",
    "671201", "671202", "671212", "671203",
]
FORBIDDEN_SPARE_COVER_ARTICLES = ["675000", "675001", "675019", "675024", "675010", "675011", "675012", "675013", "675014", "675015", "675025"]
EXPECTED_ARTICLE_ORDER = RETAINED_DRAIN_BODY_ARTICLES + [""] + INSTALLABLE_DUSCHPROFIL_ARTICLES
EXPECTED_AREA_COUNTS = {
    "ablauf_to_duschprofil_interface_statement": 3,
    "duschprofil_installable_profile_scope_statement": 1,
    "installable_duschprofil_article_scope_and_length_evidence": 30,
}
EXPECTED_TARGET_TYPE_COUNTS = {
    "retained_drain_body_article": 3,
    "generic_duschprofil_scope": 1,
    "installable_duschprofil_article": 30,
}
ALLOWED_DECISIONS = {
    "ablauf_to_duschprofil_interface_statement": {"accepted_ablauf_to_duschprofil_interface"},
    "duschprofil_installable_profile_scope_statement": {"accepted_generic_duschprofil_scope"},
    "installable_duschprofil_article_scope_and_length_evidence": {"accepted_installable_duschprofil_article"},
}
ACCEPTED_DECISIONS = set().union(*ALLOWED_DECISIONS.values())
SOURCE_EVIDENCE_FIELDS = ["source_document_name", "source_document_version", "source_page_or_section", "source_url_or_path", "source_text_excerpt", "reviewed_evidence_summary", "reviewer_decision", "reviewer_notes"]
ALLOWED_TRUE_FIELDS = ["evidence_acceptance_allowed", "evidence_complete", "ready_for_future_diagnostic_design"]
BLOCK_FALSE_FIELDS = ["direct_pair_generation_allowed", "mediated_pair_generation_allowed", "candidate_matrix_generation_allowed", "source_pack_mutation_allowed", "extraction_logic_change_allowed", "role_overlay_allowed", "length_overlay_allowed", "production_promotion_allowed", "benchmark_ready_allowed", "customer_view_allowed"]
ZERO_COUNTER_FIELDS = ["generated_direct_pair_count", "generated_mediated_pair_count", "generated_candidate_matrix_row_count", "generated_length_overlay_row_count", "proposed_source_pack_mutation_count", "production_safe_candidate_count"]
REQUIRED_COLUMNS = ["template_v3_row_id", "family", "evidence_collection_area", "evidence_target_type", "article_number", *SOURCE_EVIDENCE_FIELDS, *ALLOWED_TRUE_FIELDS, *BLOCK_FALSE_FIELDS, "diagnostic_only"]
DIAGNOSTIC_ONLY_NOTE = "This TECEdrainprofile Ablauf-to-Duschprofil evidence template v3 validation is read-only and diagnostic-only. It validates reviewed official evidence for a corrected future diagnostic model in which TECEdrainprofile Ablauf articles 673001, 673002, and 673003 relate to installable TECEdrainprofile Duschprofil articles 670xxx/671xxx. It treats 675xxx Profildeckel articles only as spare-part/profile-cover scope, not as main installable Duschprofil articles. It does not generate direct or mediated compatibility pairs, generate candidate pair matrices, create length overlays, mutate source-pack files, change TECE logic, change ACO export, or promote Products, Comparison, BOM_Options, Final_Assemblies, or Final_Set_Details. Production promotion remains blocked."


def _clean(v: Any) -> str:
    return "" if v is None else str(v).strip()


def _bool(v: Any) -> bool:
    return v is True or _clean(v).lower() in {"1", "true", "yes", "y"}


def _counts(vals) -> dict[str, int]:
    return dict(sorted(Counter(vals).items()))


def _dups(vals):
    c = Counter(vals)
    return sorted(k for k, n in c.items() if k and n > 1)


def _read_csv(path: str | Path, errors: list[str]) -> tuple[list[dict[str, str]], list[str]]:
    try:
        with Path(path).open(encoding="utf-8-sig", newline="") as fh:
            reader = csv.DictReader(fh)
            return list(reader), list(reader.fieldnames or [])
    except Exception as exc:
        errors.append(f"evidence CSV missing or cannot be loaded: {exc}")
        return [], []


def _source_pack(source_pack, family, errors):
    try:
        rows = list(getattr(load_source_pack(source_pack), "rows", []) or [])
    except Exception as exc:
        errors.append(f"source-pack cannot be loaded: {exc}")
        return [], [], {}
    fam = [r for r in rows if (_clean(getattr(r, "tece_family_candidate", "")) or _clean(getattr(r, "product_family", ""))) == family]
    return rows, fam, _counts(_clean(getattr(r, "tece_article_role_candidate", "")) or "unknown" for r in fam)


def _row_label(row: dict[str, Any], idx: int) -> str:
    return _clean(row.get("template_v3_row_id")) or str(idx + 1)


def validate(evidence_csv, source_pack=DEFAULT_SOURCE_PACK, family=EXPECTED_FAMILY):
    errors: list[str] = []
    warnings: list[str] = []
    rows, columns = _read_csv(evidence_csv, errors)
    all_rows, fam_rows, machine_counts = _source_pack(source_pack, family, errors)
    ids = [_clean(r.get("template_v3_row_id")) for r in rows]
    article_numbers = [_clean(r.get("article_number")) for r in rows]
    missing_cols = [c for c in REQUIRED_COLUMNS if c not in columns]

    invalid_family = [_row_label(r, i) for i, r in enumerate(rows) if _clean(r.get("family")) != EXPECTED_FAMILY]
    invalid_ids = [_row_label(r, i) for i, r in enumerate(rows) if _clean(r.get("template_v3_row_id")) not in EXPECTED_ROW_IDS]
    invalid_decision: list[str] = []
    invalid_evidence: list[str] = []
    diagnostic_leak: list[str] = []
    block_leak: list[str] = []
    allowed_true_missing: list[str] = []
    forbidden_in_installable = sorted({a for r, a in zip(rows, article_numbers) if a in FORBIDDEN_SPARE_COVER_ARTICLES and _clean(r.get("evidence_target_type")) == "installable_duschprofil_article"})

    for i, r in enumerate(rows):
        label = _row_label(r, i)
        area = _clean(r.get("evidence_collection_area"))
        decision = _clean(r.get("reviewer_decision"))
        if decision not in ALLOWED_DECISIONS.get(area, set()):
            invalid_decision.append(label)
        if decision in ACCEPTED_DECISIONS and any(not _clean(r.get(f)) for f in SOURCE_EVIDENCE_FIELDS):
            invalid_evidence.append(label)
        if not _bool(r.get("diagnostic_only")):
            diagnostic_leak.append(label)
        if any(_bool(r.get(f)) for f in BLOCK_FALSE_FIELDS):
            block_leak.append(label)
        if any(not _bool(r.get(f)) for f in ALLOWED_TRUE_FIELDS):
            allowed_true_missing.append(label)

    report = {
        "valid": False,
        "errors": errors,
        "warnings": warnings,
        "family": family,
        "source_pack_path": str(source_pack),
        "evidence_csv_path": str(evidence_csv),
        "input_inventory_row_count": len(all_rows),
        "family_inventory_row_count": len(fam_rows),
        "current_machine_role_counts": machine_counts,
        "evidence_csv_row_count": len(rows),
        "evidence_collection_area_counts": _counts(_clean(r.get("evidence_collection_area")) for r in rows),
        "evidence_target_type_counts": _counts(_clean(r.get("evidence_target_type")) for r in rows),
        "retained_drain_body_articles": RETAINED_DRAIN_BODY_ARTICLES,
        "installable_duschprofil_articles": INSTALLABLE_DUSCHPROFIL_ARTICLES,
        "forbidden_spare_cover_articles_in_installable_scope": forbidden_in_installable,
        "accepted_ablauf_to_duschprofil_interface_count": sum(_clean(r.get("reviewer_decision")) == "accepted_ablauf_to_duschprofil_interface" for r in rows),
        "accepted_generic_duschprofil_scope_count": sum(_clean(r.get("reviewer_decision")) == "accepted_generic_duschprofil_scope" for r in rows),
        "accepted_installable_duschprofil_article_count": sum(_clean(r.get("reviewer_decision")) == "accepted_installable_duschprofil_article" for r in rows),
        "accepted_v3_evidence_row_count": sum(_clean(r.get("reviewer_decision")) in ACCEPTED_DECISIONS for r in rows),
        "reviewed_evidence_complete_row_count": sum(_bool(r.get("evidence_complete")) for r in rows),
        "ready_for_future_diagnostic_design_count": sum(_bool(r.get("ready_for_future_diagnostic_design")) for r in rows),
        "production_promotion_blocked": True,
        "ready_for_benchmark": False,
        "ready_for_customer_view": False,
        "diagnostic_only_note": DIAGNOSTIC_ONLY_NOTE,
        "required_columns_missing": missing_cols,
        "duplicate_template_v3_row_ids": _dups(ids),
        "duplicate_article_rows": _dups(article_numbers),
        "invalid_family_rows": invalid_family,
        "invalid_template_v3_rows": invalid_ids,
        "invalid_manual_decision_rows": invalid_decision,
        "invalid_manual_evidence_rows": invalid_evidence,
        "diagnostic_only_leakage_rows": diagnostic_leak,
        "blocking_flag_leakage_rows": sorted(set(block_leak)),
        "allowed_diagnostic_acceptance_missing_rows": sorted(set(allowed_true_missing)),
    }
    for f in BLOCK_FALSE_FIELDS:
        report[f"{f}_count"] = sum(_bool(r.get(f)) for r in rows)
    for f in ZERO_COUNTER_FIELDS:
        report[f] = 0

    checks = [
        (family == EXPECTED_FAMILY, "family must be TECEdrainprofile"),
        (len(all_rows) == EXPECTED_INPUT_INVENTORY_ROW_COUNT and len(fam_rows) == EXPECTED_FAMILY_INVENTORY_ROW_COUNT and machine_counts == EXPECTED_MACHINE_ROLE_COUNTS, "source-pack baseline counts differ from expected"),
        (len(rows) == 34, "evidence CSV row count must be 34"),
        (not missing_cols, "required columns are missing"),
        (ids == EXPECTED_ROW_IDS, "required template_v3_row_ids are missing or out of order"),
        (not report["duplicate_template_v3_row_ids"], "duplicate template_v3 row IDs exist"),
        (not report["duplicate_article_rows"], "duplicate nonblank article rows exist"),
        (report["evidence_collection_area_counts"] == EXPECTED_AREA_COUNTS, "evidence_collection_area counts differ from expected"),
        (report["evidence_target_type_counts"] == EXPECTED_TARGET_TYPE_COUNTS, "evidence_target_type counts differ from expected"),
        (article_numbers == EXPECTED_ARTICLE_ORDER, "article list/order differs from expected"),
        (not forbidden_in_installable, "675xxx Profildeckel articles must not be installable Duschprofil scope"),
        (not invalid_family and not invalid_ids and not invalid_decision and not invalid_evidence, "row identity, family, decision, or evidence validation failed"),
        (not diagnostic_leak and not block_leak and not allowed_true_missing, "diagnostic-only flag policy failed"),
        (report["accepted_ablauf_to_duschprofil_interface_count"] == 3, "accepted interface count must be 3"),
        (report["accepted_generic_duschprofil_scope_count"] == 1, "accepted generic scope count must be 1"),
        (report["accepted_installable_duschprofil_article_count"] == 30, "accepted installable article count must be 30"),
        (report["accepted_v3_evidence_row_count"] == 34, "accepted v3 evidence row count must be 34"),
        (report["reviewed_evidence_complete_row_count"] == 34, "evidence_complete count must be 34"),
        (report["ready_for_future_diagnostic_design_count"] == 34, "ready_for_future_diagnostic_design count must be 34"),
    ]
    for f in BLOCK_FALSE_FIELDS:
        checks.append((report[f"{f}_count"] == 0, f"{f}_count must be 0"))
    for f in ZERO_COUNTER_FIELDS:
        checks.append((report[f] == 0, f"{f} must be 0"))
    checks += [(report["production_promotion_blocked"] is True, "production_promotion_blocked must be true"), (report["ready_for_benchmark"] is False, "ready_for_benchmark must be false"), (report["ready_for_customer_view"] is False, "ready_for_customer_view must be false")]
    for ok, msg in checks:
        if not ok and msg not in errors:
            errors.append(msg)
    report["valid"] = not errors
    return report


def main(argv=None):
    p = argparse.ArgumentParser(description="Validate reviewed TECEdrainprofile Ablauf-to-Duschprofil evidence template v3 CSV.")
    p.add_argument("--evidence-csv", required=True)
    p.add_argument("--source-pack", default=str(DEFAULT_SOURCE_PACK))
    p.add_argument("--family", default=EXPECTED_FAMILY)
    p.add_argument("--json-out")
    p.add_argument("--json", action="store_true")
    a = p.parse_args(argv)
    report = validate(a.evidence_csv, a.source_pack, a.family)
    if a.json_out:
        write_json_output(report, out=a.json_out)
    if a.json or not a.json_out:
        write_json_output(report)
    return 0 if report["valid"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
