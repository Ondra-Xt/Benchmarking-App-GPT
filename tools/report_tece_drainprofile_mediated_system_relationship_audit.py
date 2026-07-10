from __future__ import annotations

import argparse, csv, json, sys
from collections import Counter
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path: sys.path.insert(0, str(REPO_ROOT))
from tools.report_tece_source_inventory import load_source_pack  # noqa: E402
from tools.tece_report_output import write_json_output  # noqa: E402

DEFAULT_SOURCE_PACK = Path("local_source_packs/tece/pilot_001")
EXPECTED_FAMILY = "TECEdrainprofile"
EXPECTED_INPUT_INVENTORY_ROW_COUNT = 436
EXPECTED_FAMILY_INVENTORY_ROW_COUNT = 76
EXPECTED_MACHINE_ROLE_COUNTS = {"accessory":5,"complete_set":4,"drain_body":12,"profile_cover":30,"unknown":25}
RETAINED_DRAIN_BODY_ARTICLES = ["673001","673002","673003"]
PROPOSED_PROFILE_COVER_ARTICLES = ["675000","675001","675010","675011","675012","675013","675014","675015","675019","675024","675025"]
REQUIRED_AUDIT_AREAS = ["legacy_direct_drain_body_to_profile_cover_requirement","drain_body_to_duschprofil_mediated_relationship","duschprofil_to_profile_cover_scope","proposed_profile_cover_article_scope_crosscheck","mediated_relationship_template_v2_recommendation"]
REQUIRED_AUDIT_STATUSES = ["not_supported_by_current_explicit_matrix","future_evidence_requirement_needed","future_evidence_requirement_needed","diagnostic_crosscheck_only","recommended_for_next_diagnostic_step"]
RECOMMENDATIONS = ["retire_direct_pairing_requirement_for_tecedrainprofile_diagnostic_design","collect_explicit_drain_body_to_duschprofil_interface_evidence","collect_explicit_duschprofil_profile_cover_scope_evidence","verify_proposed_profile_cover_articles_in_future_template","create_mediated_relationship_evidence_template_v2"]
RECOMMENDED_REQUIREMENT_MODEL = "drain_body_to_duschprofil_system_relationship plus duschprofil_profile_cover_scope_confirmation"
RECOMMENDED_NEXT_ACTION = "Create a new diagnostic-only TECEdrainprofile mediated relationship evidence template v2 that collects explicit official evidence for drain_body_to_duschprofil interface, Duschprofil profile-cover scope, and proposed profile-cover article scope before any compatibility generation is considered."
PRODUCTION_STATUS_NOTE = "diagnostic-only TECEdrainprofile mediated system relationship audit; no evidence acceptance; no source-pack mutation; no extraction logic change; no role overlay; no length overlay; no compatibility pair generation; no candidate pair matrix; no Products/BOM/Final_Assemblies/Final_Set_Details/customer-view promotion"
DIAGNOSTIC_ONLY_NOTE = "This mediated system relationship audit is read-only and diagnostic-only. It records that the current direct TECEdrainprofile drain-body-to-profile-cover compatibility requirement is not supported by an explicit matrix in the current source pack and recommends a future evidence-template-v2 model based on drain-body-to-Duschprofil system relationship plus Duschprofil/profile-cover scope confirmation. It does not accept evidence, generate direct or mediated compatibility pairs, generate candidate pair matrices, create length overlays, mutate source-pack files, change TECE logic, change ACO export, or promote Products, Comparison, BOM_Options, Final_Assemblies, or Final_Set_Details. Production promotion remains blocked."
CSV_COLUMNS = ["audit_row_id","family","audit_area","audit_status","legacy_requirement","recommended_requirement_model","retained_drain_body_articles","proposed_profile_cover_articles","source_pack_retained_drain_body_articles_found","source_pack_proposed_profile_cover_articles_found","source_pack_proposed_profile_cover_articles_missing","explicit_direct_compatibility_matrix_found","direct_pair_generation_allowed","mediated_pair_generation_allowed","candidate_matrix_generation_allowed","future_template_v2_recommended","evidence_acceptance_allowed","evidence_complete","ready_for_future_diagnostic_design","source_pack_mutation_allowed","extraction_logic_change_allowed","role_overlay_allowed","length_overlay_allowed","production_promotion_allowed","benchmark_ready_allowed","customer_view_allowed","diagnostic_only","recommended_next_action","production_status_note"]
ZERO_COUNTER_FIELDS = ["generated_direct_pair_count","generated_mediated_pair_count","generated_candidate_matrix_row_count","generated_length_overlay_row_count","proposed_source_pack_mutation_count","production_safe_candidate_count"]
MANUAL_ZERO_FIELDS = ["generated_direct_pair_count","generated_mediated_pair_count","generated_candidate_matrix_row_count","generated_length_overlay_row_count","proposed_source_pack_mutation_count","production_safe_candidate_count","evidence_acceptance_count","source_pack_mutation_count","production_promotion_count"]
QA_ZERO_FIELDS = ["generated_direct_pair_count","generated_mediated_pair_count","generated_candidate_matrix_row_count","generated_length_overlay_row_count","proposed_source_pack_mutation_count","production_safe_candidate_count","evidence_acceptance_count","source_pack_mutation_count","production_promotion_count"]

def _clean(v: Any) -> str: return "" if v is None else str(v).strip()
def _bool(v: Any) -> bool: return v is True or _clean(v).lower() in {"1","true","yes","y"}
def _counts(vals): return dict(sorted(Counter(vals).items()))
def _dups(vals):
    c=Counter(vals); return sorted(k for k,v in c.items() if k and v>1)
def _read_json(path, errors, label):
    try: return json.loads(Path(path).read_text(encoding="utf-8"))
    except Exception as exc: errors.append(f"{label} missing or cannot be loaded: {exc}"); return {}
def _read_csv(path, errors, label):
    try:
        with Path(path).open(encoding="utf-8-sig", newline="") as fh: return list(csv.DictReader(fh))
    except Exception as exc: errors.append(f"{label} missing or cannot be loaded: {exc}"); return []
def _articles(rows, wanted):
    have={_clean(getattr(r,"article_number", "")) for r in rows}; return [a for a in wanted if a in have], [a for a in wanted if a not in have]
def _source_pack(source_pack, family, errors):
    try: rows=list(getattr(load_source_pack(source_pack),"rows",[]) or [])
    except Exception as exc: errors.append(f"source-pack cannot be loaded: {exc}"); return [],[],{}
    fam=[r for r in rows if (_clean(getattr(r,"tece_family_candidate", "")) or _clean(getattr(r,"product_family", ""))) == family]
    return rows, fam, _counts([_clean(getattr(r,"tece_article_role_candidate", "")) or "unknown" for r in fam])
def _count_true(rows, field): return sum(_bool(r.get(field)) for r in rows)

def _audit_rows(family, drain_found=None, cover_found=None, cover_missing=None):
    drain_found = drain_found if drain_found is not None else RETAINED_DRAIN_BODY_ARTICLES
    cover_found = cover_found if cover_found is not None else []
    cover_missing = cover_missing if cover_missing is not None else []
    rows=[]
    for i,(area,status,rec) in enumerate(zip(REQUIRED_AUDIT_AREAS, REQUIRED_AUDIT_STATUSES, RECOMMENDATIONS),1):
        row={c:"" for c in CSV_COLUMNS}
        row.update({"audit_row_id":f"TECE-DP-MEDIATED-SYSTEM-RELATIONSHIP-AUDIT-{i:06d}","family":family,"audit_area":area,"audit_status":status,"legacy_requirement":"direct article-level drain_body_to_profile_cover compatibility matrix","recommended_requirement_model":RECOMMENDED_REQUIREMENT_MODEL,"retained_drain_body_articles":"|".join(RETAINED_DRAIN_BODY_ARTICLES),"proposed_profile_cover_articles":"|".join(PROPOSED_PROFILE_COVER_ARTICLES),"source_pack_retained_drain_body_articles_found":"|".join(drain_found),"source_pack_proposed_profile_cover_articles_found":"|".join(cover_found),"source_pack_proposed_profile_cover_articles_missing":"|".join(cover_missing),"explicit_direct_compatibility_matrix_found":"false","future_template_v2_recommended":"true" if i==5 else "false","recommended_next_action":RECOMMENDED_NEXT_ACTION,"production_status_note":PRODUCTION_STATUS_NOTE})
        for f in ["direct_pair_generation_allowed","mediated_pair_generation_allowed","candidate_matrix_generation_allowed","evidence_acceptance_allowed","evidence_complete","ready_for_future_diagnostic_design","source_pack_mutation_allowed","extraction_logic_change_allowed","role_overlay_allowed","length_overlay_allowed","production_promotion_allowed","benchmark_ready_allowed","customer_view_allowed"]: row[f]="false"
        row["diagnostic_only"]="true"; row["recommendation"] = rec
        rows.append(row)
    return rows

def build_audit(manual_workpack_report, source_aid_csv, source_aid_report, source_aid_qa_report, source_pack=DEFAULT_SOURCE_PACK, family=EXPECTED_FAMILY, out=None, audit_rows=None):
    errors=[]; warnings=[]
    manual=_read_json(manual_workpack_report, errors, "manual workpack report"); aid_csv=_read_csv(source_aid_csv, errors, "source-aid CSV"); aid=_read_json(source_aid_report, errors, "source-aid report"); qa=_read_json(source_aid_qa_report, errors, "source-aid QA report")
    all_rows, fam_rows, machine_counts = _source_pack(source_pack, family, errors)
    drain_found, drain_missing = _articles(fam_rows, RETAINED_DRAIN_BODY_ARTICLES); cover_found, cover_missing = _articles(fam_rows, PROPOSED_PROFILE_COVER_ARTICLES)
    rows=list(audit_rows) if audit_rows is not None else _audit_rows(family, drain_found, cover_found, cover_missing)
    ids=[_clean(r.get("audit_row_id")) for r in rows]
    production_fields=["direct_pair_generation_allowed","mediated_pair_generation_allowed","candidate_matrix_generation_allowed","evidence_acceptance_allowed","source_pack_mutation_allowed","extraction_logic_change_allowed","role_overlay_allowed","length_overlay_allowed","production_promotion_allowed"]
    readiness_fields=["benchmark_ready_allowed","customer_view_allowed","ready_for_future_diagnostic_design"]
    production_leak=[ids[i] or str(i+1) for i,r in enumerate(rows) if any(_bool(r.get(f)) for f in production_fields)]
    readiness_leak=[ids[i] or str(i+1) for i,r in enumerate(rows) if any(_bool(r.get(f)) for f in readiness_fields)]
    diagnostic_leak=[ids[i] or str(i+1) for i,r in enumerate(rows) if not _bool(r.get("diagnostic_only"))]
    invalid_family=[ids[i] or str(i+1) for i,r in enumerate(rows) if _clean(r.get("family"))!=EXPECTED_FAMILY]
    invalid_audit=[ids[i] or str(i+1) for i,r in enumerate(rows) if _clean(r.get("audit_area")) not in REQUIRED_AUDIT_AREAS or _clean(r.get("audit_status")) not in REQUIRED_AUDIT_STATUSES]
    invalid_block=[ids[i] or str(i+1) for i,r in enumerate(rows) if _clean(r.get("recommended_requirement_model"))!=RECOMMENDED_REQUIREMENT_MODEL]
    explicit=int(qa.get("explicit_direct_compatibility_matrix_found_count", qa.get("explicit_compatibility_matrix_found_count",0)) or 0)
    report={"valid":False,"errors":errors,"warnings":warnings,"family":family,"source_pack_path":str(source_pack),"manual_workpack_report_path":str(manual_workpack_report),"source_aid_csv_path":str(source_aid_csv),"source_aid_report_path":str(source_aid_report),"source_aid_qa_report_path":str(source_aid_qa_report),"manual_workpack_report_valid":manual.get("valid") is True,"source_aid_report_valid":aid.get("valid") is True,"source_aid_qa_report_valid":qa.get("valid") is True,"input_inventory_row_count":len(all_rows),"family_inventory_row_count":len(fam_rows),"current_machine_role_counts":machine_counts,"audit_row_count":len(rows),"audit_row_ids":ids,"audit_area_counts":_counts([_clean(r.get("audit_area")) for r in rows]),"audit_status_counts":_counts([_clean(r.get("audit_status")) for r in rows]),"retained_drain_body_articles":RETAINED_DRAIN_BODY_ARTICLES,"proposed_profile_cover_articles":PROPOSED_PROFILE_COVER_ARTICLES,"source_pack_retained_drain_body_articles_found":drain_found,"source_pack_retained_drain_body_articles_missing":drain_missing,"source_pack_proposed_profile_cover_articles_found":cover_found,"source_pack_proposed_profile_cover_articles_missing":cover_missing,"source_pack_profile_cover_count":int(machine_counts.get("profile_cover") or 0),"explicit_direct_compatibility_matrix_found_count":explicit,"legacy_direct_pairing_requirement_supported":False,"mediated_relationship_model_recommended":True,"future_template_v2_recommended":any(_bool(r.get("future_template_v2_recommended")) for r in rows),"duplicate_audit_row_ids":_dups(ids),"invalid_family_rows":invalid_family,"invalid_audit_rows":invalid_audit,"invalid_blocking_rows":invalid_block,"production_leakage_rows":production_leak,"readiness_leakage_rows":readiness_leak,"diagnostic_only_leakage_rows":diagnostic_leak,"production_promotion_blocked":qa.get("production_promotion_blocked"),"ready_for_benchmark":qa.get("ready_for_benchmark"),"ready_for_customer_view":qa.get("ready_for_customer_view"),"output_csv_path":str(out) if out else None,"diagnostic_only_note":DIAGNOSTIC_ONLY_NOTE}
    for f in ["direct_pair_generation_allowed","mediated_pair_generation_allowed","candidate_matrix_generation_allowed","evidence_acceptance_allowed","evidence_complete","ready_for_future_diagnostic_design","source_pack_mutation_allowed","extraction_logic_change_allowed","role_overlay_allowed","length_overlay_allowed","production_promotion_allowed","benchmark_ready_allowed","customer_view_allowed"]: report[f+"_count"]=_count_true(rows,f)
    for f in ZERO_COUNTER_FIELDS: report[f]=int(qa.get(f, manual.get(f,0)) or 0)
    checks=[(manual.get("valid") is True,"manual workpack report valid must be true"),(aid.get("valid") is True,"source-aid report valid must be true"),(qa.get("valid") is True,"source-aid QA report valid must be true"),(family==EXPECTED_FAMILY,"family must be TECEdrainprofile"),(len(all_rows)==436 and len(fam_rows)==76 and machine_counts==EXPECTED_MACHINE_ROLE_COUNTS,"source-pack baseline counts differ from expected"),(manual.get("workpack_row_count")==5 and manual.get("evidence_draft_row_count")==5,"manual workpack report row counts differ from expected"),(manual.get("retained_drain_body_article_numbers")==RETAINED_DRAIN_BODY_ARTICLES,"retained drain-body article list differs from expected"),(manual.get("retained_drain_body_workpack_row_count")==3 and manual.get("compatibility_matrix_workpack_row_count")==1 and manual.get("profile_cover_scope_workpack_row_count")==1,"manual workpack section counts differ from expected"),(manual.get("manual_evidence_fields_prefilled_workpack_rows")==[] and manual.get("manual_evidence_fields_prefilled_draft_rows")==[],"manual evidence prefilled rows must be empty"),(qa.get("source_aid_csv_row_count")==5 and qa.get("source_aid_report_row_count")==5 and qa.get("qa_row_count")==5,"source-aid QA row counts differ from expected"),((qa.get("mapping_status_counts") or {}).get("pass")==5 and (qa.get("blocking_status_counts") or {}).get("pass")==5 and (qa.get("qa_status_counts") or {}).get("pass")==5,"source-aid QA status counts differ from expected"),(explicit==0,"explicit_direct_compatibility_matrix_found_count must be 0"),(len(rows)==5,"audit row count must be 5"),(ids==[f"TECE-DP-MEDIATED-SYSTEM-RELATIONSHIP-AUDIT-{i:06d}" for i in range(1,6)],"required audit_row_ids are missing"),(not report["duplicate_audit_row_ids"],"duplicate audit row IDs exist"),(report["retained_drain_body_articles"]==RETAINED_DRAIN_BODY_ARTICLES,"retained drain-body article list differs from expected"),(report["proposed_profile_cover_articles"]==PROPOSED_PROFILE_COVER_ARTICLES,"proposed profile-cover article list differs from expected"),(report["legacy_direct_pairing_requirement_supported"] is False,"legacy_direct_pairing_requirement_supported must be false"),(report["mediated_relationship_model_recommended"] is True,"mediated_relationship_model_recommended must be true"),(report["future_template_v2_recommended"] is True,"future_template_v2_recommended must be true"),(qa.get("production_promotion_blocked") is True,"production_promotion_blocked must be true"),(qa.get("ready_for_benchmark") is False,"ready_for_benchmark must be false"),(qa.get("ready_for_customer_view") is False,"ready_for_customer_view must be false"),(not production_leak and not readiness_leak and not diagnostic_leak,"production/readiness/diagnostic leakage detected")]
    for f in MANUAL_ZERO_FIELDS:
        checks.append((int(manual.get(f,0) or 0)==0, f"manual {f} must be 0"))
    for f in QA_ZERO_FIELDS:
        checks.append((int(qa.get(f,0) or 0)==0, f"source-aid QA {f} must be 0"))
    for f in ZERO_COUNTER_FIELDS: checks.append((report[f]==0, f"{f} must be 0"))
    for f in [k for k in report if k.endswith("_allowed_count") or k in {"evidence_complete_count","ready_for_future_diagnostic_design_count"}]: checks.append((report[f]==0, f"{f} must be 0"))
    for ok,msg in checks:
        if not ok and msg not in errors: errors.append(msg)
    report["valid"] = not errors
    if out:
        Path(out).parent.mkdir(parents=True, exist_ok=True)
        with Path(out).open("w", encoding="utf-8-sig", newline="") as fh:
            w=csv.DictWriter(fh, fieldnames=CSV_COLUMNS, extrasaction="ignore"); w.writeheader(); w.writerows(rows)
    return rows, report

def main(argv=None):
    p=argparse.ArgumentParser(description="Build read-only TECEdrainprofile mediated system relationship audit.")
    p.add_argument("--manual-workpack-report", required=True); p.add_argument("--source-aid-csv", required=True); p.add_argument("--source-aid-report", required=True); p.add_argument("--source-aid-qa-report", required=True)
    p.add_argument("--source-pack", default=str(DEFAULT_SOURCE_PACK)); p.add_argument("--family", default=EXPECTED_FAMILY); p.add_argument("--out", required=True); p.add_argument("--json-out", required=True); p.add_argument("--json", action="store_true")
    a=p.parse_args(argv); _rows, report=build_audit(a.manual_workpack_report,a.source_aid_csv,a.source_aid_report,a.source_aid_qa_report,a.source_pack,a.family,a.out)
    write_json_output(report, out=a.json_out)
    if a.json: write_json_output(report)
    return 0 if report["valid"] else 1
if __name__ == "__main__": raise SystemExit(main())
