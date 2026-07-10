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

from tools.export_tece_drainprofile_explicit_compatibility_evidence_collection_template import (  # noqa:E402
    BLOCKING_FIELDS,
    DEFAULT_SOURCE_PACK,
    EXPECTED_FAMILY,
    EXPECTED_FAMILY_INVENTORY_ROW_COUNT,
    EXPECTED_INPUT_INVENTORY_ROW_COUNT,
    EXPECTED_MACHINE_ROLE_COUNTS,
    REQUIRED_TEMPLATE_IDS,
    RETAINED_DRAIN_BODY_ARTICLE_NUMBERS,
    _bool,
    _clean,
    _counts,
    _dups,
    _int,
)
from tools.report_tece_source_inventory import load_source_pack  # noqa:E402
from tools.tece_report_output import write_json_output  # noqa:E402

CSV_COLUMNS = [
    "source_aid_id", "family", "aid_area", "aid_target_type", "related_template_row_id",
    "related_requirement_id", "article_number", "article_scope", "source_pack_article_role",
    "source_pack_row_found", "source_pack_product_family", "source_pack_role_candidate",
    "source_pack_source_file", "source_pack_source_page_or_section", "source_pack_source_text_or_excerpt",
    "source_pack_debug_context", "source_aid_status", "reviewer_use_instruction",
    "evidence_decision_allowed", "evidence_complete", "ready_for_future_diagnostic_design",
    "compatibility_pair_generation_allowed", "candidate_pair_matrix_allowed", "length_overlay_allowed",
    "source_pack_mutation_allowed", "production_promotion_allowed", "benchmark_ready_allowed",
    "customer_view_allowed", "diagnostic_only", "recommended_next_action", "production_status_note",
]
REQUIRED_SOURCE_AID_IDS = [f"TECE-DP-COMPAT-EVIDENCE-SOURCE-AID-{i:06d}" for i in range(1, 6)]
RECOMMENDED_NEXT_ACTION = "Use this diagnostic source-aid export only to help manually locate official TECE source evidence; copy verified evidence into the evidence collection template and validate it separately before any future diagnostic design."
PRODUCTION_STATUS_NOTE = "diagnostic-only TECEdrainprofile explicit compatibility evidence source-aid export; no evidence acceptance; no source-pack mutation; no extraction logic change; no length overlay; no compatibility pair generation; no candidate pair matrix; no Products/BOM/Final_Assemblies/Final_Set_Details/customer-view promotion"
DIAGNOSTIC_ONLY_NOTE = "This source-aid export is read-only and diagnostic-only. It provides TECEdrainprofile source-pack context and search pointers to help a human reviewer manually collect explicit evidence, but it does not accept evidence, generate compatibility pairs, generate candidate pair matrices, create length overlays, mutate source-pack files, change TECE logic, change ACO export, or promote Products, Comparison, BOM_Options, Final_Assemblies, or Final_Set_Details. Production promotion remains blocked."
VALIDATION_ZERO_FIELDS = ["generated_compatibility_pair_count", "generated_candidate_pair_matrix_row_count", "generated_length_overlay_row_count", "proposed_source_pack_mutation_count", "production_safe_candidate_count"]
VALIDATION_COUNT_FIELDS = ["evidence_csv_row_count", "template_csv_row_count", "template_report_row_count", "retained_drain_body_row_count", "compatibility_matrix_row_count", "profile_cover_scope_row_count", "evidence_complete_row_count", "evidence_incomplete_row_count", "missing_manual_evidence_row_count"]
EXPECTED_VALIDATION_COUNTS = {"evidence_csv_row_count": 5, "template_csv_row_count": 5, "template_report_row_count": 5, "retained_drain_body_row_count": 3, "compatibility_matrix_row_count": 1, "profile_cover_scope_row_count": 1, "evidence_complete_row_count": 0, "evidence_incomplete_row_count": 5, "missing_manual_evidence_row_count": 5}


def _read_csv(path: str | Path, errors: list[str]) -> list[dict[str, str]]:
    try:
        with Path(path).open(encoding="utf-8-sig", newline="") as fh:
            return list(csv.DictReader(fh))
    except Exception as exc:
        errors.append(f"evidence template CSV missing or cannot be loaded: {exc}")
        return []


def _read_json(path: str | Path, errors: list[str]) -> dict[str, Any]:
    try:
        return json.loads(Path(path).read_text(encoding="utf-8"))
    except Exception as exc:
        errors.append(f"evidence validation report missing or cannot be loaded: {exc}")
        return {}


def _row_ref(row: dict[str, str], idx: int) -> str:
    return _clean(row.get("source_aid_id")) or str(idx)


def _page_label(row: Any) -> str:
    return _clean(getattr(row, "page_range_label", "")) or _clean(getattr(row, "catalogue_page_label", "")) or _clean(getattr(row, "source_page_start", ""))


def _source_pack(source_pack: str | Path, family: str, errors: list[str]) -> tuple[list[Any], list[Any], dict[str, int]]:
    try:
        rows = list(load_source_pack(source_pack).rows or [])
    except Exception as exc:
        errors.append(f"source-pack cannot be loaded: {exc}")
        return [], [], {}
    fam = [r for r in rows if (_clean(getattr(r, "tece_family_candidate", "")) or _clean(getattr(r, "product_family", ""))) == family]
    roles = [_clean(getattr(r, "tece_article_role_candidate", "")) or _clean(getattr(r, "article_role", "")) or "unknown" for r in fam]
    return rows, fam, _counts(roles)


def _base(source_aid_id: str, family: str, area: str, target: str, template_id: str, req_id: str, article: str, scope: str) -> dict[str, str]:
    row = {c: "" for c in CSV_COLUMNS}
    row.update({"source_aid_id": source_aid_id, "family": family, "aid_area": area, "aid_target_type": target, "related_template_row_id": template_id, "related_requirement_id": req_id, "article_number": article, "article_scope": scope, "evidence_decision_allowed": "false", "evidence_complete": "false", "ready_for_future_diagnostic_design": "false", "compatibility_pair_generation_allowed": "false", "candidate_pair_matrix_allowed": "false", "length_overlay_allowed": "false", "source_pack_mutation_allowed": "false", "production_promotion_allowed": "false", "benchmark_ready_allowed": "false", "customer_view_allowed": "false", "diagnostic_only": "true", "recommended_next_action": RECOMMENDED_NEXT_ACTION, "production_status_note": PRODUCTION_STATUS_NOTE})
    return row


def build_source_aid(evidence_template_csv: str | Path, evidence_validation_report: str | Path, source_pack: str | Path = DEFAULT_SOURCE_PACK, family: str = EXPECTED_FAMILY, out: str | Path | None = None, rows_override: list[dict[str, str]] | None = None) -> tuple[list[dict[str, str]], dict[str, Any]]:
    errors: list[str] = []
    warnings: list[str] = []
    template_rows = _read_csv(evidence_template_csv, errors)
    validation = _read_json(evidence_validation_report, errors)
    inv_rows, fam_rows, role_counts = _source_pack(source_pack, family, errors)
    profile_rows = [r for r in fam_rows if (_clean(getattr(r, "tece_article_role_candidate", "")) or "unknown") == "profile_cover"]
    by_article = { _clean(getattr(r, "article_number", "")): r for r in fam_rows if _clean(getattr(r, "article_number", "")) }

    if rows_override is None:
        rows: list[dict[str, str]] = []
        instruction = "Use this source-pack context only as a pointer. The reviewer must verify explicit nominal length evidence or explicit length-independence evidence in official TECE source text before marking the evidence template as accepted."
        for idx, article in enumerate(RETAINED_DRAIN_BODY_ARTICLE_NUMBERS, 1):
            r = _base(REQUIRED_SOURCE_AID_IDS[idx - 1], family, "retained_drain_body_nominal_length_evidence", "retained_drain_body_article_lookup", f"TECE-DP-COMPAT-EVIDENCE-TEMPLATE-{idx:06d}", "TECE-DP-COMPAT-EVIDENCE-REQ-000001", article, article)
            src = by_article.get(article)
            found = src is not None
            r.update({"source_pack_row_found": str(found).lower(), "source_pack_article_role": _clean(getattr(src, "tece_article_role_candidate", "")) if found else "", "source_pack_product_family": _clean(getattr(src, "product_family", "")) if found else "", "source_pack_role_candidate": _clean(getattr(src, "tece_article_role_candidate", "")) if found else "", "source_pack_source_file": _clean(getattr(src, "source_file", "")) if found else "", "source_pack_source_page_or_section": _page_label(src) if found else "", "source_pack_source_text_or_excerpt": _clean(getattr(src, "evidence_text", ""))[:500] if found else "", "source_pack_debug_context": _clean(getattr(src, "classification_reason", "")) if found else "", "source_aid_status": "source_pack_article_context_found" if found else "source_pack_article_context_missing", "reviewer_use_instruction": instruction})
            rows.append(r)
        r = _base(REQUIRED_SOURCE_AID_IDS[3], family, "article_level_drain_body_to_profile_cover_compatibility_matrix", "compatibility_matrix_source_search_pointer", "TECE-DP-COMPAT-EVIDENCE-TEMPLATE-000004", "TECE-DP-COMPAT-EVIDENCE-REQ-000002", "", "retained_drain_bodies_673001_673002_673003_to_tecedrainprofile_profile_covers")
        r.update({"source_pack_row_found": "false", "source_aid_status": "explicit_compatibility_matrix_not_found_in_current_source_pack", "reviewer_use_instruction": "Search official TECE catalog, datasheets, article pages, or compatibility tables for an explicit article-level drain-body-to-profile-cover compatibility matrix or statement. Do not infer compatibility from same family, same length, section proximity, or generic profile-cover compatibility."})
        rows.append(r)
        covers = sorted(_clean(getattr(x, "article_number", "")) for x in profile_rows if _clean(getattr(x, "article_number", "")))
        r = _base(REQUIRED_SOURCE_AID_IDS[4], family, "profile_cover_scope_confirmation", "profile_cover_scope_summary", "TECE-DP-COMPAT-EVIDENCE-TEMPLATE-000005", "TECE-DP-COMPAT-EVIDENCE-REQ-000003", "", "30_tecedrainprofile_profile_cover_rows")
        r.update({"source_pack_row_found": str(len(profile_rows) == 30).lower(), "source_pack_source_text_or_excerpt": f"profile_cover_count={len(profile_rows)}; articles=" + ";".join(covers[:30]), "source_aid_status": "source_pack_profile_cover_scope_summary_available" if len(profile_rows) == 30 else "source_pack_profile_cover_scope_summary_mismatch", "reviewer_use_instruction": "Use this profile-cover scope summary only as a pointer. The reviewer must confirm the 30 profile_cover rows against official TECE catalog/profile-cover source scope before marking scope evidence accepted."})
        rows.append(r)
    else:
        rows = rows_override

    ids = [_clean(r.get("source_aid_id")) for r in rows]
    dup_ids = _dups(ids)
    drain_articles = [_clean(r.get("article_number")) for r in rows if _clean(r.get("aid_target_type")) == "retained_drain_body_article_lookup"]
    dup_article = _dups(drain_articles)
    invalid_family = [_row_ref(r, i) for i, r in enumerate(rows, 1) if _clean(r.get("family")) != EXPECTED_FAMILY]
    invalid_template = [_row_ref(r, i) for i, r in enumerate(rows, 1) if _clean(r.get("related_template_row_id")) not in REQUIRED_TEMPLATE_IDS]
    invalid_source = [_row_ref(r, i) for i, r in enumerate(rows, 1) if _clean(r.get("source_aid_id")) not in REQUIRED_SOURCE_AID_IDS]
    invalid_blocking = [_row_ref(r, i) for i, r in enumerate(rows, 1) if any(_bool(r.get(f)) for f in ["evidence_decision_allowed", "evidence_complete", "ready_for_future_diagnostic_design"] + BLOCKING_FIELDS)]
    production_leak = [_row_ref(r, i) for i, r in enumerate(rows, 1) if any(_bool(r.get(f)) for f in ["compatibility_pair_generation_allowed", "candidate_pair_matrix_allowed", "length_overlay_allowed", "source_pack_mutation_allowed", "production_promotion_allowed"])]
    readiness_leak = [_row_ref(r, i) for i, r in enumerate(rows, 1) if _bool(r.get("ready_for_future_diagnostic_design")) or _bool(r.get("benchmark_ready_allowed")) or _bool(r.get("customer_view_allowed"))]
    diag_leak = [_row_ref(r, i) for i, r in enumerate(rows, 1) if not _bool(r.get("diagnostic_only"))]
    counts = {f: sum(_bool(r.get(f)) for r in rows) for f in ["evidence_decision_allowed", "evidence_complete", "ready_for_future_diagnostic_design"] + BLOCKING_FIELDS}
    generated = {f: _int(validation.get(f)) for f in VALIDATION_ZERO_FIELDS}

    checks = [
        (validation.get("valid") is True, "evidence validation report valid must be true"),
        (family == EXPECTED_FAMILY and validation.get("family") == EXPECTED_FAMILY, "family must be TECEdrainprofile"),
        (len(inv_rows) == EXPECTED_INPUT_INVENTORY_ROW_COUNT and len(fam_rows) == EXPECTED_FAMILY_INVENTORY_ROW_COUNT and role_counts == EXPECTED_MACHINE_ROLE_COUNTS and validation.get("input_inventory_row_count") == EXPECTED_INPUT_INVENTORY_ROW_COUNT and validation.get("family_inventory_row_count") == EXPECTED_FAMILY_INVENTORY_ROW_COUNT and validation.get("current_machine_role_counts") == EXPECTED_MACHINE_ROLE_COUNTS, "source-pack baseline counts differ from expected"),
        (len(template_rows) == 5, "evidence template CSV must contain exactly 5 rows"),
        (set(REQUIRED_TEMPLATE_IDS).issubset({_clean(r.get("template_row_id")) for r in template_rows}), "required template IDs are missing"),
        (all(_int(validation.get(k)) == v for k, v in EXPECTED_VALIDATION_COUNTS.items()), "validation report row counts differ from expected"),
        (validation.get("retained_drain_body_article_numbers") == RETAINED_DRAIN_BODY_ARTICLE_NUMBERS, "retained drain body article numbers differ"),
        (validation.get("ready_for_future_diagnostic_design") is False and validation.get("production_promotion_blocked") is True and validation.get("ready_for_benchmark") is False and validation.get("ready_for_customer_view") is False, "production/readiness report gates differ"),
        (all(v == 0 for v in generated.values()), "generation/mutation/promotion counters must be 0"),
        (len(rows) == 5, "source_aid_row_count must be 5"),
        (set(REQUIRED_SOURCE_AID_IDS).issubset(ids), "required source_aid_ids are missing"),
        (not dup_ids and not dup_article, "duplicate source aid IDs or retained drain body article rows detected"),
        (not invalid_family and not invalid_template and not invalid_source, "invalid source-aid rows detected"),
        (not invalid_blocking and not production_leak and not readiness_leak and not diag_leak, "production/readiness/diagnostic leakage detected"),
    ]
    for ok, msg in checks:
        if not ok and msg not in errors:
            errors.append(msg)

    report = {"valid": not errors, "errors": errors, "warnings": warnings, "family": family, "source_pack_path": str(source_pack), "evidence_template_csv_path": str(evidence_template_csv), "evidence_validation_report_path": str(evidence_validation_report), "evidence_validation_report_valid": validation.get("valid") is True, "input_inventory_row_count": len(inv_rows), "family_inventory_row_count": len(fam_rows), "current_machine_role_counts": role_counts, "evidence_template_row_count": len(template_rows), "validation_evidence_csv_row_count": _int(validation.get("evidence_csv_row_count")), "validation_template_csv_row_count": _int(validation.get("template_csv_row_count")), "validation_template_report_row_count": _int(validation.get("template_report_row_count")), "source_aid_row_count": len(rows), "source_aid_ids": ids, "aid_area_counts": _counts([_clean(r.get("aid_area")) for r in rows]), "aid_target_type_counts": _counts([_clean(r.get("aid_target_type")) for r in rows]), "source_aid_status_counts": _counts([_clean(r.get("source_aid_status")) for r in rows]), "retained_drain_body_article_numbers": RETAINED_DRAIN_BODY_ARTICLE_NUMBERS, "retained_drain_body_source_aid_row_count": sum(_clean(r.get("aid_target_type")) == "retained_drain_body_article_lookup" for r in rows), "retained_drain_body_source_pack_found_count": sum(_clean(r.get("aid_target_type")) == "retained_drain_body_article_lookup" and _bool(r.get("source_pack_row_found")) for r in rows), "compatibility_matrix_source_aid_row_count": sum(_clean(r.get("aid_target_type")) == "compatibility_matrix_source_search_pointer" for r in rows), "explicit_compatibility_matrix_found_count": 0, "profile_cover_scope_source_aid_row_count": sum(_clean(r.get("aid_target_type")) == "profile_cover_scope_summary" for r in rows), "source_pack_profile_cover_count": len(profile_rows), **generated, **{f"{k}_count": v for k, v in counts.items()}, "proposed_source_pack_mutation_count": generated["proposed_source_pack_mutation_count"], "production_safe_candidate_count": generated["production_safe_candidate_count"], "duplicate_source_aid_ids": dup_ids, "duplicate_article_source_aid_rows": dup_article, "invalid_family_rows": invalid_family, "invalid_template_rows": invalid_template, "invalid_source_aid_rows": invalid_source, "invalid_blocking_rows": invalid_blocking, "production_leakage_rows": production_leak, "readiness_leakage_rows": readiness_leak, "diagnostic_only_leakage_rows": diag_leak, "production_promotion_blocked": True if validation.get("production_promotion_blocked") is True else validation.get("production_promotion_blocked"), "ready_for_benchmark": validation.get("ready_for_benchmark"), "ready_for_customer_view": validation.get("ready_for_customer_view"), "output_csv_path": str(out) if out else None, "diagnostic_only_note": DIAGNOSTIC_ONLY_NOTE}

    if out:
        Path(out).parent.mkdir(parents=True, exist_ok=True)
        with Path(out).open("w", encoding="utf-8", newline="") as fh:
            writer = csv.DictWriter(fh, fieldnames=CSV_COLUMNS, extrasaction="ignore")
            writer.writeheader(); writer.writerows(rows)
    return rows, report


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description="Export read-only TECEdrainprofile explicit compatibility evidence source-aid CSV.")
    p.add_argument("--evidence-template-csv", required=True)
    p.add_argument("--evidence-validation-report", required=True)
    p.add_argument("--source-pack", default=str(DEFAULT_SOURCE_PACK))
    p.add_argument("--family", default=EXPECTED_FAMILY)
    p.add_argument("--out", required=True)
    p.add_argument("--json-out", required=True)
    p.add_argument("--json", action="store_true")
    a = p.parse_args(argv)
    _rows, report = build_source_aid(a.evidence_template_csv, a.evidence_validation_report, a.source_pack, a.family, a.out)
    write_json_output(report, out=a.json_out)
    if a.json:
        write_json_output(report)
    return 0 if report["valid"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
