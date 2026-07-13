from __future__ import annotations

import argparse, csv, json, re, sys
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
ROW_ID_PREFIX = "TECE-DP-MEDIATED-EVIDENCE-TEMPLATE-V2"
EXPECTED_ROW_IDS = [f"{ROW_ID_PREFIX}-{i:06d}" for i in range(1, 16)]
RETAINED_DRAIN_BODY_ARTICLES = ["673001", "673002", "673003"]
PROPOSED_PROFILE_COVER_ARTICLES = ["675000", "675001", "675010", "675011", "675012", "675013", "675014", "675015", "675019", "675024", "675025"]
EXPECTED_ARTICLE_ORDER = RETAINED_DRAIN_BODY_ARTICLES + [""] + PROPOSED_PROFILE_COVER_ARTICLES
EXPECTED_AREA_COUNTS = {"drain_body_to_duschprofil_system_relationship": 3, "duschprofil_profile_cover_scope_confirmation": 1, "proposed_profile_cover_article_scope": 11}
ALLOWED_DECISIONS = {
    "drain_body_to_duschprofil_system_relationship": {"accepted_explicit_drain_body_for_tecedrainprofile_duschprofil_statement"},
    "duschprofil_profile_cover_scope_confirmation": {"accepted_explicit_duschprofil_includes_profile_cover_statement"},
    "proposed_profile_cover_article_scope": {"accepted_explicit_profile_cover_article_scope"},
}
ACCEPTED_DECISIONS = set().union(*ALLOWED_DECISIONS.values())
SOURCE_EVIDENCE_FIELDS = ["source_document_name", "source_document_version", "source_page_or_section", "source_url_or_path", "source_text_excerpt", "reviewed_evidence_summary", "reviewer_decision", "reviewer_notes", "safe_to_use_for_future_diagnostic_design"]
BLOCK_FALSE_FIELDS = ["direct_pair_generation_allowed", "mediated_pair_generation_allowed", "candidate_matrix_generation_allowed", "evidence_acceptance_allowed", "evidence_complete", "ready_for_future_diagnostic_design", "source_pack_mutation_allowed", "extraction_logic_change_allowed", "role_overlay_allowed", "length_overlay_allowed", "production_promotion_allowed", "benchmark_ready_allowed", "customer_view_allowed"]
ZERO_COUNTER_FIELDS = ["generated_direct_pair_count", "generated_mediated_pair_count", "generated_candidate_matrix_row_count", "generated_length_overlay_row_count", "proposed_source_pack_mutation_count", "production_safe_candidate_count"]
REQUIRED_COLUMNS = ["template_v2_row_id", "family", "evidence_collection_area", "evidence_target_type", "article_number", *SOURCE_EVIDENCE_FIELDS, *BLOCK_FALSE_FIELDS, "diagnostic_only"]
DIAGNOSTIC_ONLY_NOTE = "This reviewed TECEdrainprofile mediated relationship evidence template v2 validation is read-only and diagnostic-only. It validates that explicit official evidence has been collected for the mediated diagnostic evidence scope: drain-body-to-Duschprofil relationship, Duschprofil/profile-cover scope confirmation, and proposed profile-cover article scope. It does not generate direct or mediated compatibility pairs, generate candidate pair matrices, create length overlays, mutate source-pack files, change TECE logic, change ACO export, or promote Products, Comparison, BOM_Options, Final_Assemblies, or Final_Set_Details. Production promotion remains blocked."


def _clean(v: Any) -> str: return "" if v is None else str(v).strip()
def _bool(v: Any) -> bool: return v is True or _clean(v).lower() in {"1", "true", "yes", "y"}
def _counts(vals): return dict(sorted(Counter(vals).items()))
def _dups(vals):
    c = Counter(vals); return sorted(k for k, n in c.items() if k and n > 1)
def _ids(rows): return [_clean(r.get("template_v2_row_id")) for r in rows]
def _row_label(row, idx): return _clean(row.get("template_v2_row_id")) or str(idx + 1)
def _read_csv(path: str | Path, errors: list[str]) -> tuple[list[dict[str, str]], list[str]]:
    try:
        with Path(path).open(encoding="utf-8-sig", newline="") as fh:
            reader = csv.DictReader(fh)
            return list(reader), list(reader.fieldnames or [])
    except Exception as exc:
        errors.append(f"evidence CSV missing or cannot be loaded: {exc}"); return [], []
def _read_json(path: str | Path, errors: list[str]) -> dict[str, Any]:
    try: return json.loads(Path(path).read_text(encoding="utf-8"))
    except Exception as exc: errors.append(f"template v2 report missing or cannot be loaded: {exc}"); return {}
def _source_pack(source_pack, family, errors):
    try: rows = list(getattr(load_source_pack(source_pack), "rows", []) or [])
    except Exception as exc: errors.append(f"source-pack cannot be loaded: {exc}"); return [], [], {}
    fam = [r for r in rows if (_clean(getattr(r, "tece_family_candidate", "")) or _clean(getattr(r, "product_family", ""))) == family]
    return rows, fam, _counts([_clean(getattr(r, "tece_article_role_candidate", "")) or "unknown" for r in fam])
def _articles(rows, wanted):
    have = {_clean(getattr(r, "article_number", "")) for r in rows}
    return [a for a in wanted if a in have], [a for a in wanted if a not in have]
def _date_ok(article: str, text: str) -> bool:
    low = text.lower()
    if article in {"675019", "675010"}: return bool(re.search(r"(bis|until|through|to)\s*0?6\s*/\s*2023|06\s*/\s*2023", low))
    if article in {"675024", "675025"}: return bool(re.search(r"(ab|from|since)\s*0?7\s*/\s*2023|07\s*/\s*2023", low))
    return True

def validate(evidence_csv, template_v2_report, source_pack=DEFAULT_SOURCE_PACK, family=EXPECTED_FAMILY):
    errors: list[str] = []; warnings: list[str] = []
    rows, columns = _read_csv(evidence_csv, errors)
    tmpl = _read_json(template_v2_report, errors)
    all_rows, fam_rows, machine_counts = _source_pack(source_pack, family, errors)
    ids = _ids(rows)
    missing_cols = [c for c in REQUIRED_COLUMNS if c not in columns]
    drain_found, drain_missing = _articles(fam_rows, RETAINED_DRAIN_BODY_ARTICLES)
    cover_found, cover_missing = _articles(fam_rows, PROPOSED_PROFILE_COVER_ARTICLES)

    invalid_family = [_row_label(r, i) for i, r in enumerate(rows) if _clean(r.get("family")) != EXPECTED_FAMILY]
    invalid_template = [_row_label(r, i) for i, r in enumerate(rows) if _clean(r.get("template_v2_row_id")) not in EXPECTED_ROW_IDS]
    invalid_decision = [] ; invalid_evidence = [] ; invalid_safe = [] ; invalid_date = [] ; direct_claim = [] ; production_leak = [] ; readiness_leak = [] ; diagnostic_leak = []
    for i, r in enumerate(rows):
        label = _row_label(r, i); area = _clean(r.get("evidence_collection_area")); dec = _clean(r.get("reviewer_decision")); safe = _bool(r.get("safe_to_use_for_future_diagnostic_design"))
        if dec not in ALLOWED_DECISIONS.get(area, set()): invalid_decision.append(label)
        if dec in ACCEPTED_DECISIONS and any(not _clean(r.get(f)) for f in SOURCE_EVIDENCE_FIELDS): invalid_evidence.append(label)
        if (safe and dec not in ACCEPTED_DECISIONS) or not safe: invalid_safe.append(label)
        if not _date_ok(_clean(r.get("article_number")), _clean(r.get("reviewed_evidence_summary")) + " " + _clean(r.get("reviewer_notes"))): invalid_date.append(label)
        claim_text = (_clean(r.get("source_text_excerpt")) + " " + _clean(r.get("reviewer_notes"))).lower()
        if re.search(r"direct .*drain.*body.*(cover|profildeckel).*(matrix|pair)|matrix.*drain.*body.*(cover|profildeckel)", claim_text): direct_claim.append(label)
        if any(_bool(r.get(f)) for f in ["direct_pair_generation_allowed","mediated_pair_generation_allowed","candidate_matrix_generation_allowed","evidence_acceptance_allowed","source_pack_mutation_allowed","extraction_logic_change_allowed","role_overlay_allowed","length_overlay_allowed","production_promotion_allowed"]): production_leak.append(label)
        if any(_bool(r.get(f)) for f in ["evidence_complete","ready_for_future_diagnostic_design","benchmark_ready_allowed","customer_view_allowed"]): readiness_leak.append(label)
        if re.search(r"production[- ]ready|customer[- ]view ready|benchmark[- ]ready|production promotion allowed|customer readiness", claim_text): readiness_leak.append(label)
        if not _bool(r.get("diagnostic_only")): diagnostic_leak.append(label)
    readiness_leak = sorted(set(readiness_leak))

    report = {"valid": False, "errors": errors, "warnings": warnings, "family": family, "source_pack_path": str(source_pack), "evidence_csv_path": str(evidence_csv), "template_v2_report_path": str(template_v2_report), "template_v2_report_valid": tmpl.get("valid") is True, "input_inventory_row_count": len(all_rows), "family_inventory_row_count": len(fam_rows), "current_machine_role_counts": machine_counts, "evidence_csv_row_count": len(rows), "template_v2_row_count": len(ids), "template_v2_row_ids": ids, "evidence_collection_area_counts": _counts(_clean(r.get("evidence_collection_area")) for r in rows), "evidence_target_type_counts": _counts(_clean(r.get("evidence_target_type")) for r in rows), "reviewer_decision_counts": _counts(_clean(r.get("reviewer_decision")) for r in rows), "row_status_counts": _counts(_clean(r.get("evidence_status")) for r in rows), "retained_drain_body_articles": RETAINED_DRAIN_BODY_ARTICLES, "proposed_profile_cover_articles": PROPOSED_PROFILE_COVER_ARTICLES, "source_pack_retained_drain_body_articles_found": drain_found, "source_pack_retained_drain_body_articles_missing": drain_missing, "source_pack_proposed_profile_cover_articles_found": cover_found, "source_pack_proposed_profile_cover_articles_missing": cover_missing, "duplicate_template_v2_row_ids": _dups(ids), "duplicate_article_rows": _dups(_clean(r.get("article_number")) for r in rows), "required_columns_missing": missing_cols, "invalid_family_rows": invalid_family, "invalid_template_v2_rows": invalid_template, "invalid_manual_decision_rows": invalid_decision, "invalid_manual_evidence_rows": invalid_evidence, "invalid_safe_to_use_rows": invalid_safe, "invalid_date_scope_rows": invalid_date, "invalid_direct_pairing_claim_rows": direct_claim, "invalid_blocking_rows": [], "production_leakage_rows": sorted(set(production_leak)), "readiness_leakage_rows": readiness_leak, "diagnostic_only_leakage_rows": diagnostic_leak, "diagnostic_only_note": DIAGNOSTIC_ONLY_NOTE}
    for k,v in {"accepted_drain_body_to_duschprofil_evidence_count":"accepted_explicit_drain_body_for_tecedrainprofile_duschprofil_statement","accepted_duschprofil_profile_cover_scope_count":"accepted_explicit_duschprofil_includes_profile_cover_statement","accepted_profile_cover_article_scope_count":"accepted_explicit_profile_cover_article_scope"}.items(): report[k] = report["reviewer_decision_counts"].get(v,0)
    report["accepted_mediated_evidence_row_count"] = sum(1 for r in rows if _clean(r.get("reviewer_decision")) in ACCEPTED_DECISIONS)
    report["rejected_or_ambiguous_row_count"] = len(rows) - report["accepted_mediated_evidence_row_count"]
    report["safe_to_use_for_future_diagnostic_design_true_count"] = sum(_bool(r.get("safe_to_use_for_future_diagnostic_design")) for r in rows)
    report["reviewed_evidence_complete_row_count"] = sum(1 for r in rows if _clean(r.get("reviewer_decision")) in ACCEPTED_DECISIONS and _bool(r.get("safe_to_use_for_future_diagnostic_design")))
    report["reviewed_evidence_incomplete_row_count"] = len(rows) - report["reviewed_evidence_complete_row_count"]
    report["ready_for_future_diagnostic_design"] = report["reviewed_evidence_complete_row_count"] == 15 and not invalid_decision and not invalid_evidence and not invalid_safe
    for f in BLOCK_FALSE_FIELDS: report[f + ("_flag_count" if f in {"evidence_complete","ready_for_future_diagnostic_design"} else "_count")] = sum(_bool(r.get(f)) for r in rows)
    for f in ZERO_COUNTER_FIELDS: report[f] = int(tmpl.get(f, 0) or 0)
    report.update({"production_promotion_blocked": True, "ready_for_benchmark": False, "ready_for_customer_view": False})

    checks = [(tmpl.get("valid") is True, "template v2 report valid must be true"), (family == EXPECTED_FAMILY, "family must be TECEdrainprofile"), (len(all_rows)==436 and len(fam_rows)==76 and machine_counts==EXPECTED_MACHINE_ROLE_COUNTS, "source-pack baseline counts differ from expected"), (len(rows)==15, "evidence CSV row count must be 15"), (not missing_cols, "required columns are missing"), (ids == EXPECTED_ROW_IDS, "required template_v2_row_ids are missing or out of order"), (not report["duplicate_template_v2_row_ids"], "duplicate template_v2 row IDs exist"), (not report["duplicate_article_rows"], "duplicate nonblank article rows exist"), (report["evidence_collection_area_counts"] == EXPECTED_AREA_COUNTS, "evidence_collection_area counts differ from expected"), ([_clean(r.get("article_number")) for r in rows] == EXPECTED_ARTICLE_ORDER, "article list/order differs from expected"), (not invalid_family, "family rows must be TECEdrainprofile"), (not invalid_decision, "reviewer decision is not allowed for row area"), (not invalid_evidence, "accepted decision has blank source/manual evidence field"), (report["safe_to_use_for_future_diagnostic_design_true_count"] == 15 and not invalid_safe, "safe_to_use_for_future_diagnostic_design must be true only with accepted decisions for all rows"), (not invalid_date, "date-limited articles are missing date-scope notes"), (not direct_claim, "direct drain-body-to-cover matrix or pairing claim detected"), (not production_leak and not readiness_leak and not diagnostic_leak, "production/readiness/diagnostic leakage detected"), (not drain_missing, "retained drain-body articles missing from source-pack"), (not cover_missing, "proposed profile-cover articles missing from source-pack")]
    for f in BLOCK_FALSE_FIELDS:
        key = f + ("_flag_count" if f in {"evidence_complete","ready_for_future_diagnostic_design"} else "_count")
        checks.append((report[key] == 0, f"{key} must be 0"))
    for f in ZERO_COUNTER_FIELDS: checks.append((report[f] == 0, f"{f} must be 0"))
    checks += [(report["production_promotion_blocked"] is True, "production_promotion_blocked must be true"), (report["ready_for_benchmark"] is False, "ready_for_benchmark must be false"), (report["ready_for_customer_view"] is False, "ready_for_customer_view must be false")]
    for ok, msg in checks:
        if not ok and msg not in errors: errors.append(msg)
    report["valid"] = not errors
    return report

def main(argv=None):
    p=argparse.ArgumentParser(description="Validate reviewed TECEdrainprofile mediated relationship evidence template v2 CSV.")
    p.add_argument("--evidence-csv", required=True); p.add_argument("--template-v2-report", required=True); p.add_argument("--source-pack", default=str(DEFAULT_SOURCE_PACK)); p.add_argument("--family", default=EXPECTED_FAMILY); p.add_argument("--json-out"); p.add_argument("--json", action="store_true")
    a=p.parse_args(argv); report=validate(a.evidence_csv, a.template_v2_report, a.source_pack, a.family)
    if a.json_out: write_json_output(report, out=a.json_out)
    if a.json or not a.json_out: write_json_output(report)
    return 0 if report["valid"] else 1
if __name__ == "__main__": raise SystemExit(main())
