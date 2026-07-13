from __future__ import annotations

import argparse, csv, json, sys
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
RETAINED_DRAIN_BODY_ARTICLES = ["673001", "673002", "673003"]
PROPOSED_PROFILE_COVER_ARTICLES = ["675000", "675001", "675010", "675011", "675012", "675013", "675014", "675015", "675019", "675024", "675025"]
ROW_ID_PREFIX = "TECE-DP-MEDIATED-EVIDENCE-TEMPLATE-V2"
RECOMMENDED_NEXT_ACTION = "Collect explicit official TECE evidence for the mediated TECEdrainprofile system relationship before any future diagnostic pair design is considered."
PRODUCTION_STATUS_NOTE = "diagnostic-only TECEdrainprofile mediated relationship evidence template v2; no evidence acceptance; no direct or mediated pair generation; no candidate matrix; no source-pack mutation; no extraction logic change; no role overlay; no length overlay; no Products/BOM/Final_Assemblies/Final_Set_Details/customer-view promotion"
DIAGNOSTIC_ONLY_NOTE = "This mediated relationship evidence template v2 export is read-only and diagnostic-only. It creates blank manual evidence rows for TECEdrainprofile drain-body-to-Duschprofil system relationship evidence, Duschprofil/profile-cover scope confirmation, and proposed profile-cover article scope evidence. It does not accept evidence, generate direct or mediated compatibility pairs, generate candidate pair matrices, create length overlays, mutate source-pack files, change TECE logic, change ACO export, or promote Products, Comparison, BOM_Options, Final_Assemblies, or Final_Set_Details. Production promotion remains blocked."
CSV_COLUMNS = ["template_v2_row_id","family","evidence_collection_area","evidence_target_type","article_number","article_scope","evidence_status","evidence_required","required_evidence_type","accepted_source_examples","rejected_inference_methods","source_document_name","source_document_version","source_page_or_section","source_url_or_path","source_text_excerpt","reviewed_evidence_summary","reviewer_decision","reviewer_notes","safe_to_use_for_future_diagnostic_design","direct_pair_generation_allowed","mediated_pair_generation_allowed","candidate_matrix_generation_allowed","evidence_acceptance_allowed","evidence_complete","ready_for_future_diagnostic_design","source_pack_mutation_allowed","extraction_logic_change_allowed","role_overlay_allowed","length_overlay_allowed","production_promotion_allowed","benchmark_ready_allowed","customer_view_allowed","diagnostic_only","recommended_next_action","production_status_note"]
MANUAL_EVIDENCE_FIELDS = ["source_document_name","source_document_version","source_page_or_section","source_url_or_path","source_text_excerpt","reviewed_evidence_summary","reviewer_decision","reviewer_notes","safe_to_use_for_future_diagnostic_design"]
BLOCK_FALSE_FIELDS = ["direct_pair_generation_allowed","mediated_pair_generation_allowed","candidate_matrix_generation_allowed","evidence_acceptance_allowed","evidence_complete","ready_for_future_diagnostic_design","source_pack_mutation_allowed","extraction_logic_change_allowed","role_overlay_allowed","length_overlay_allowed","production_promotion_allowed","benchmark_ready_allowed","customer_view_allowed"]
ZERO_COUNTER_FIELDS = ["generated_direct_pair_count","generated_mediated_pair_count","generated_candidate_matrix_row_count","generated_length_overlay_row_count","proposed_source_pack_mutation_count","production_safe_candidate_count"]
AUDIT_ZERO_FIELDS = ZERO_COUNTER_FIELDS + ["evidence_acceptance_count","source_pack_mutation_count","production_promotion_count"]
REJECTED_INFERENCE_METHODS = "|".join(["same_family_assumption","nearby_catalog_row_assumption","same_length_assumption","direct_drain_body_to_cover_pairing_without_explicit_evidence","generated_matrix_without_official_source","inferred_from_installation_layering_only","inferred_from_article_number_pattern_only"])
ACCEPTED_SOURCE_EXAMPLES = "official TECE catalogue|official TECE technical datasheet|official TECE installation manual"


def _clean(v: Any) -> str: return "" if v is None else str(v).strip()
def _bool(v: Any) -> bool: return v is True or _clean(v).lower() in {"1", "true", "yes", "y"}
def _counts(vals): return dict(sorted(Counter(vals).items()))
def _dups(vals):
    c = Counter(vals); return sorted(k for k, v in c.items() if k and v > 1)
def _count_true(rows, field): return sum(_bool(r.get(field)) for r in rows)
def _read_json(path, errors, label):
    try: return json.loads(Path(path).read_text(encoding="utf-8"))
    except Exception as exc: errors.append(f"{label} missing or cannot be loaded: {exc}"); return {}
def _source_pack(source_pack, family, errors):
    try: rows = list(getattr(load_source_pack(source_pack), "rows", []) or [])
    except Exception as exc: errors.append(f"source-pack cannot be loaded: {exc}"); return [], [], {}
    fam = [r for r in rows if (_clean(getattr(r, "tece_family_candidate", "")) or _clean(getattr(r, "product_family", ""))) == family]
    return rows, fam, _counts([_clean(getattr(r, "tece_article_role_candidate", "")) or "unknown" for r in fam])
def _articles(rows, wanted):
    have = {_clean(getattr(r, "article_number", "")) for r in rows}
    return [a for a in wanted if a in have], [a for a in wanted if a not in have]

def template_rows(family=EXPECTED_FAMILY):
    rows = []
    def base(i, area, target, article, scope, req):
        row = {c: "" for c in CSV_COLUMNS}
        row.update({"template_v2_row_id": f"{ROW_ID_PREFIX}-{i:06d}", "family": family, "evidence_collection_area": area, "evidence_target_type": target, "article_number": article, "article_scope": scope, "evidence_status": "needs_manual_evidence", "evidence_required": "true", "required_evidence_type": req, "accepted_source_examples": ACCEPTED_SOURCE_EXAMPLES, "rejected_inference_methods": REJECTED_INFERENCE_METHODS, "diagnostic_only": "true", "recommended_next_action": RECOMMENDED_NEXT_ACTION, "production_status_note": PRODUCTION_STATUS_NOTE})
        for f in BLOCK_FALSE_FIELDS: row[f] = "false"
        return row
    req1 = "explicit official TECE evidence that the retained drain body is intended for / connects to TECEdrainprofile Duschprofil"
    for i, article in enumerate(RETAINED_DRAIN_BODY_ARTICLES, 1):
        rows.append(base(i, "drain_body_to_duschprofil_system_relationship", "retained_drain_body_article", article, "retained TECEdrainprofile Ablauf / drain body", req1))
    rows.append(base(4, "duschprofil_profile_cover_scope_confirmation", "duschprofil_profile_cover_scope", "", "TECEdrainprofile Duschprofil / Profildeckel scope", "explicit official TECE evidence that TECEdrainprofile Duschprofil includes, uses, or is completed by Profildeckel / profile cover scope"))
    req3 = "explicit official TECE evidence that this article is a TECEdrainprofile Profildeckel / profile-cover article"
    for offset, article in enumerate(PROPOSED_PROFILE_COVER_ARTICLES, 5):
        rows.append(base(offset, "proposed_profile_cover_article_scope", "proposed_profile_cover_article", article, "proposed TECEdrainprofile Profildeckel / profile-cover article scope", req3))
    return rows

def build_template(mediated_audit_report, source_pack=DEFAULT_SOURCE_PACK, family=EXPECTED_FAMILY, out=None, rows=None):
    errors=[]; warnings=[]
    audit = _read_json(mediated_audit_report, errors, "mediated audit report")
    all_rows, fam_rows, machine_counts = _source_pack(source_pack, family, errors)
    drain_found, drain_missing = _articles(fam_rows, RETAINED_DRAIN_BODY_ARTICLES)
    cover_found, cover_missing = _articles(fam_rows, PROPOSED_PROFILE_COVER_ARTICLES)
    rows = list(rows) if rows is not None else template_rows(family)
    ids=[_clean(r.get("template_v2_row_id")) for r in rows]
    manual_prefilled=[ids[i] or str(i+1) for i,r in enumerate(rows) if any(_clean(r.get(f)) for f in MANUAL_EVIDENCE_FIELDS)]
    duplicate_articles=_dups([_clean(r.get("article_number")) for r in rows])
    production_leak=[ids[i] or str(i+1) for i,r in enumerate(rows) if any(_bool(r.get(f)) for f in ["direct_pair_generation_allowed","mediated_pair_generation_allowed","candidate_matrix_generation_allowed","evidence_acceptance_allowed","source_pack_mutation_allowed","extraction_logic_change_allowed","role_overlay_allowed","length_overlay_allowed","production_promotion_allowed"])]
    readiness_leak=[ids[i] or str(i+1) for i,r in enumerate(rows) if any(_bool(r.get(f)) for f in ["benchmark_ready_allowed","customer_view_allowed","ready_for_future_diagnostic_design","evidence_complete"])]
    diagnostic_leak=[ids[i] or str(i+1) for i,r in enumerate(rows) if not _bool(r.get("diagnostic_only"))]
    invalid_family=[ids[i] or str(i+1) for i,r in enumerate(rows) if _clean(r.get("family")) != EXPECTED_FAMILY]
    valid_ids=[f"{ROW_ID_PREFIX}-{i:06d}" for i in range(1,16)]
    invalid_template=[ids[i] or str(i+1) for i,r in enumerate(rows) if _clean(r.get("template_v2_row_id")) not in valid_ids or _clean(r.get("evidence_status")) != "needs_manual_evidence" or not _bool(r.get("evidence_required"))]
    report={"valid":False,"errors":errors,"warnings":warnings,"family":family,"source_pack_path":str(source_pack),"mediated_audit_report_path":str(mediated_audit_report),"mediated_audit_report_valid":audit.get("valid") is True,"input_inventory_row_count":len(all_rows),"family_inventory_row_count":len(fam_rows),"current_machine_role_counts":machine_counts,"template_v2_row_count":len(rows),"template_v2_row_ids":ids,"evidence_collection_area_counts":_counts([_clean(r.get("evidence_collection_area")) for r in rows]),"evidence_target_type_counts":_counts([_clean(r.get("evidence_target_type")) for r in rows]),"retained_drain_body_articles":RETAINED_DRAIN_BODY_ARTICLES,"proposed_profile_cover_articles":PROPOSED_PROFILE_COVER_ARTICLES,"source_pack_retained_drain_body_articles_found":drain_found,"source_pack_retained_drain_body_articles_missing":drain_missing,"source_pack_proposed_profile_cover_articles_found":cover_found,"source_pack_proposed_profile_cover_articles_missing":cover_missing,"manual_evidence_fields_blank_count":len(rows)*len(MANUAL_EVIDENCE_FIELDS)-sum(1 for r in rows for f in MANUAL_EVIDENCE_FIELDS if _clean(r.get(f))),"manual_evidence_fields_prefilled_rows":manual_prefilled,"duplicate_template_v2_row_ids":_dups(ids),"duplicate_article_rows":duplicate_articles,"invalid_family_rows":invalid_family,"invalid_template_v2_rows":invalid_template,"invalid_blocking_rows":[],"production_leakage_rows":production_leak,"readiness_leakage_rows":readiness_leak,"diagnostic_only_leakage_rows":diagnostic_leak,"production_promotion_blocked":audit.get("production_promotion_blocked"),"ready_for_benchmark":audit.get("ready_for_benchmark"),"ready_for_customer_view":audit.get("ready_for_customer_view"),"output_csv_path":str(out) if out else None,"diagnostic_only_note":DIAGNOSTIC_ONLY_NOTE}
    for f in BLOCK_FALSE_FIELDS: report[f+"_count"]=_count_true(rows,f)
    for f in ZERO_COUNTER_FIELDS: report[f]=int(audit.get(f,0) or 0)
    checks=[(audit.get("valid") is True,"mediated audit report valid must be true"),(family==EXPECTED_FAMILY,"family must be TECEdrainprofile"),(len(all_rows)==EXPECTED_INPUT_INVENTORY_ROW_COUNT and len(fam_rows)==EXPECTED_FAMILY_INVENTORY_ROW_COUNT and machine_counts==EXPECTED_MACHINE_ROLE_COUNTS,"source-pack baseline counts differ from expected"),(audit.get("family")==EXPECTED_FAMILY,"mediated audit report family must be TECEdrainprofile"),(audit.get("input_inventory_row_count")==436 and audit.get("family_inventory_row_count")==76 and audit.get("current_machine_role_counts")==EXPECTED_MACHINE_ROLE_COUNTS,"mediated audit source-pack baseline counts differ from expected"),(audit.get("audit_row_count")==5,"audit_row_count must be 5"),(audit.get("retained_drain_body_articles")==RETAINED_DRAIN_BODY_ARTICLES,"retained drain-body article list differs from expected"),(audit.get("proposed_profile_cover_articles")==PROPOSED_PROFILE_COVER_ARTICLES,"proposed profile-cover article list differs from expected"),(audit.get("source_pack_retained_drain_body_articles_missing")==[],"audit retained drain-body missing list must be empty"),(audit.get("source_pack_proposed_profile_cover_articles_missing")==[],"audit proposed profile-cover missing list must be empty"),(int(audit.get("explicit_direct_compatibility_matrix_found_count",0) or 0)==0,"explicit_direct_compatibility_matrix_found_count must be 0"),(audit.get("legacy_direct_pairing_requirement_supported") is False,"legacy_direct_pairing_requirement_supported must be false"),(audit.get("mediated_relationship_model_recommended") is True,"mediated_relationship_model_recommended must be true"),(audit.get("future_template_v2_recommended") is True,"future_template_v2_recommended must be true"),(drain_missing==[],"retained drain-body article missing from source-pack"),(cover_missing==[],"proposed profile-cover article missing from source-pack"),(len(rows)==15,"template_v2_row_count must be 15"),(ids==valid_ids,"required template_v2_row_ids are missing"),(not report["duplicate_template_v2_row_ids"],"duplicate template_v2 row IDs exist"),(not duplicate_articles,"duplicate article rows exist"),(not manual_prefilled,"manual evidence fields are prefilled"),(audit.get("production_promotion_blocked") is True,"production_promotion_blocked must be true"),(audit.get("ready_for_benchmark") is False,"ready_for_benchmark must be false"),(audit.get("ready_for_customer_view") is False,"ready_for_customer_view must be false"),(not production_leak and not readiness_leak and not diagnostic_leak,"production/readiness/diagnostic leakage detected")]
    for f in AUDIT_ZERO_FIELDS: checks.append((int(audit.get(f,0) or 0)==0, f"{f} must be 0"))
    for f in [k for k in report if k.endswith("_allowed_count") or k in {"evidence_complete_count","ready_for_future_diagnostic_design_count"}]: checks.append((report[f]==0, f"{f} must be 0"))
    for f in ZERO_COUNTER_FIELDS: checks.append((report[f]==0, f"{f} must be 0"))
    for ok,msg in checks:
        if not ok and msg not in errors: errors.append(msg)
    report["valid"] = not errors
    if out:
        Path(out).parent.mkdir(parents=True, exist_ok=True)
        with Path(out).open("w", encoding="utf-8-sig", newline="") as fh:
            w=csv.DictWriter(fh, fieldnames=CSV_COLUMNS, extrasaction="ignore"); w.writeheader(); w.writerows(rows)
    return rows, report

def main(argv=None):
    p=argparse.ArgumentParser(description="Export read-only TECEdrainprofile mediated relationship evidence template v2.")
    p.add_argument("--mediated-audit-report", required=True); p.add_argument("--source-pack", default=str(DEFAULT_SOURCE_PACK)); p.add_argument("--family", default=EXPECTED_FAMILY); p.add_argument("--out", required=True); p.add_argument("--json-out", required=True); p.add_argument("--json", action="store_true")
    a=p.parse_args(argv); _rows, report=build_template(a.mediated_audit_report, a.source_pack, a.family, a.out)
    write_json_output(report, out=a.json_out)
    if a.json: write_json_output(report)
    return 0 if report["valid"] else 1
if __name__ == "__main__": raise SystemExit(main())
