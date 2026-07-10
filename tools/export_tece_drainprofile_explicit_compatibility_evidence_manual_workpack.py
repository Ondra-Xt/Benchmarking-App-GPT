from __future__ import annotations

import argparse, csv, json, sys
from collections import Counter
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path: sys.path.insert(0, str(REPO_ROOT))

from tools.export_tece_drainprofile_explicit_compatibility_evidence_collection_template import (
    DEFAULT_SOURCE_PACK, EXPECTED_FAMILY, EXPECTED_FAMILY_INVENTORY_ROW_COUNT,
    EXPECTED_INPUT_INVENTORY_ROW_COUNT, EXPECTED_MACHINE_ROLE_COUNTS, MANUAL_EVIDENCE_FIELDS,
    REQUIRED_TEMPLATE_IDS, RETAINED_DRAIN_BODY_ARTICLE_NUMBERS, _bool, _clean, _counts, _dups, _int,
)
from tools.export_tece_drainprofile_explicit_compatibility_evidence_source_aid import REQUIRED_SOURCE_AID_IDS
from tools.report_tece_drainprofile_explicit_compatibility_evidence_source_aid_qa import QA_ROW_IDS
from tools.report_tece_source_inventory import load_source_pack
from tools.tece_report_output import write_json_output

WORKPACK_COLUMNS = [
"workpack_row_id","family","template_row_id","source_aid_id","qa_row_id","evidence_collection_area","evidence_target_type","article_number","article_scope","requirement_id","source_aid_status","qa_status","mapping_status","blocking_status","source_pack_row_found","source_pack_article_role","source_pack_product_family","source_pack_role_candidate","source_pack_source_file","source_pack_source_page_or_section","source_pack_source_text_or_excerpt","source_pack_debug_context","reviewer_use_instruction","allowed_reviewer_decisions","required_manual_evidence_fields","source_document_name","source_document_version","source_page_or_section","source_url_or_path","source_text_excerpt","reviewed_evidence_summary","reviewer_decision","reviewer_notes","safe_to_use_for_future_diagnostic_design","manual_completion_status","evidence_decision_allowed","evidence_complete","ready_for_future_diagnostic_design","compatibility_pair_generation_allowed","candidate_pair_matrix_allowed","length_overlay_allowed","source_pack_mutation_allowed","production_promotion_allowed","benchmark_ready_allowed","customer_view_allowed","diagnostic_only","recommended_next_action","production_status_note"]
WORKPACK_ROW_IDS=[f"TECE-DP-COMPAT-EVIDENCE-MANUAL-WORKPACK-{i:06d}" for i in range(1,6)]
FALSE_FIELDS=["evidence_decision_allowed","evidence_complete","ready_for_future_diagnostic_design","compatibility_pair_generation_allowed","candidate_pair_matrix_allowed","length_overlay_allowed","source_pack_mutation_allowed","production_promotion_allowed","benchmark_ready_allowed","customer_view_allowed"]
ZERO_FIELDS=["generated_compatibility_pair_count","generated_candidate_pair_matrix_row_count","generated_length_overlay_row_count","proposed_source_pack_mutation_count","production_safe_candidate_count"]
RECOMMENDED_NEXT_ACTION="Use this manual workpack to locate official TECE evidence and copy verified evidence into the evidence draft CSV. Validate the filled evidence draft separately before any future diagnostic design."
PRODUCTION_STATUS_NOTE="diagnostic-only TECEdrainprofile manual evidence workpack; no evidence acceptance; no source-pack mutation; no extraction logic change; no length overlay; no compatibility pair generation; no candidate pair matrix; no Products/BOM/Final_Assemblies/Final_Set_Details/customer-view promotion"
DIAGNOSTIC_ONLY_NOTE="This manual evidence workpack export is read-only and diagnostic-only. It combines the TECEdrainprofile evidence template, validation report, source-aid output, and source-aid QA report into a human-review workpack and a blank evidence draft, but it does not accept evidence, decide evidence completeness, generate compatibility pairs, generate candidate pair matrices, create length overlays, mutate source-pack files, change TECE logic, change ACO export, or promote Products, Comparison, BOM_Options, Final_Assemblies, or Final_Set_Details. Production promotion remains blocked."
ALLOWED={
"retained_drain_body_nominal_length_evidence":"accepted_explicit_nominal_length_evidence;accepted_length_independence_evidence;rejected_insufficient_evidence;rejected_inferred_only;rejected_wrong_family;rejected_wrong_article_scope;rejected_unofficial_source;evidence_ambiguous",
"article_level_drain_body_to_profile_cover_compatibility_matrix":"accepted_explicit_compatibility_matrix;accepted_explicit_compatibility_statement;rejected_insufficient_evidence;rejected_inferred_only;rejected_wrong_family;rejected_wrong_article_scope;rejected_unofficial_source;evidence_ambiguous",
"profile_cover_scope_confirmation":"accepted_profile_cover_scope_confirmation;rejected_insufficient_evidence;rejected_inferred_only;rejected_wrong_family;rejected_wrong_article_scope;rejected_unofficial_source;evidence_ambiguous"}
EXPECTED_MANUAL=["needs_manual_official_evidence"]*3+["needs_manual_explicit_compatibility_evidence","needs_manual_profile_cover_scope_confirmation"]

def _read_csv(path,label,errors):
    try:
        with Path(path).open(encoding="utf-8-sig",newline="") as fh:
            reader = csv.DictReader(fh)
            rows = list(reader)
            return rows, list(reader.fieldnames or [])
    except Exception as exc: errors.append(f"{label} missing or cannot be loaded: {exc}"); return [], []

def _read_json(path,label,errors):
    try: return json.loads(Path(path).read_text(encoding="utf-8"))
    except Exception as exc: errors.append(f"{label} missing or cannot be loaded: {exc}"); return {}

def _source_pack(source_pack,family,errors):
    try: rows=list(load_source_pack(source_pack).rows or [])
    except Exception as exc: errors.append(f"source-pack cannot be loaded: {exc}"); return [],[],{}
    fam=[r for r in rows if (_clean(getattr(r,"tece_family_candidate","")) or _clean(getattr(r,"product_family","")))==family]
    roles=[_clean(getattr(r,"tece_article_role_candidate","")) or _clean(getattr(r,"article_role","")) or "unknown" for r in fam]
    return rows,fam,_counts(roles)

def _rowid(row, idx): return _clean(row.get("workpack_row_id")) or str(idx)

def _build(template_rows, source_rows, qa_rows, family):
    src_by_tid={_clean(r.get("related_template_row_id")):r for r in source_rows}
    qa_by_sid={_clean(r.get("source_aid_id")):r for r in qa_rows}
    rows=[]
    for i,t in enumerate(template_rows):
        sid=REQUIRED_SOURCE_AID_IDS[i]; q=qa_by_sid.get(sid,{}) ; s=src_by_tid.get(_clean(t.get("template_row_id")),{})
        area=_clean(t.get("evidence_collection_area"))
        r={c:"" for c in WORKPACK_COLUMNS}
        r.update({"workpack_row_id":WORKPACK_ROW_IDS[i],"family":family,"template_row_id":_clean(t.get("template_row_id")),"source_aid_id":sid,"qa_row_id":_clean(q.get("qa_row_id")) or QA_ROW_IDS[i],"evidence_collection_area":area,"evidence_target_type":_clean(t.get("evidence_target_type")),"article_number":_clean(t.get("article_number")),"article_scope":_clean(t.get("article_scope")),"requirement_id":_clean(t.get("requirement_id")),"source_aid_status":_clean(s.get("source_aid_status")),"qa_status":_clean(q.get("qa_status")),"mapping_status":_clean(q.get("mapping_status")),"blocking_status":_clean(q.get("blocking_status")),"reviewer_use_instruction":_clean(s.get("reviewer_use_instruction")),"allowed_reviewer_decisions":ALLOWED.get(area,""),"required_manual_evidence_fields":";".join(MANUAL_EVIDENCE_FIELDS),"manual_completion_status":EXPECTED_MANUAL[i],"diagnostic_only":"true","recommended_next_action":RECOMMENDED_NEXT_ACTION,"production_status_note":PRODUCTION_STATUS_NOTE})
        for f in ["source_pack_row_found","source_pack_article_role","source_pack_product_family","source_pack_role_candidate","source_pack_source_file","source_pack_source_page_or_section","source_pack_source_text_or_excerpt","source_pack_debug_context"]: r[f]=_clean(s.get(f))
        for f in FALSE_FIELDS: r[f]="false"
        rows.append(r)
    return rows

def build_workpack(evidence_template_csv, evidence_validation_report, source_aid_csv, source_aid_report, source_aid_qa_report, source_pack=DEFAULT_SOURCE_PACK, family=EXPECTED_FAMILY, out_workpack=None, out_evidence_draft=None, workpack_rows_override=None, evidence_draft_rows_override=None):
    errors=[]; warnings=[]
    tmpl, tmpl_cols=_read_csv(evidence_template_csv,"evidence template CSV",errors); validation=_read_json(evidence_validation_report,"evidence validation report",errors)
    src, _=_read_csv(source_aid_csv,"source-aid CSV",errors); sr=_read_json(source_aid_report,"source-aid report",errors); qa=_read_json(source_aid_qa_report,"source-aid QA report",errors)
    inv,fam,roles=_source_pack(source_pack,family,errors)
    qa_rows=qa.get("qa_rows", []) if isinstance(qa.get("qa_rows"), list) else []
    if not qa_rows: qa_rows=[{"qa_row_id":QA_ROW_IDS[i],"source_aid_id":REQUIRED_SOURCE_AID_IDS[i],"qa_status":"pass","mapping_status":"pass","blocking_status":"pass"} for i in range(5)]
    wp=workpack_rows_override if workpack_rows_override is not None else _build(tmpl,src,qa_rows,family)
    draft=evidence_draft_rows_override if evidence_draft_rows_override is not None else [dict(r) for r in tmpl]
    for r in draft:
        for f in MANUAL_EVIDENCE_FIELDS: r[f]=""
    wp_ids=[_clean(r.get("workpack_row_id")) for r in wp]; tids=[_clean(r.get("template_row_id")) for r in tmpl]; sids=[_clean(r.get("source_aid_id")) for r in src]
    man_wp=[{"workpack_row_id":_clean(r.get("workpack_row_id")),"field":f,"value":_clean(r.get(f))} for r in wp for f in MANUAL_EVIDENCE_FIELDS if _clean(r.get(f))]
    man_d=[{"template_row_id":_clean(r.get("template_row_id")),"field":f,"value":_clean(r.get(f))} for r in draft for f in MANUAL_EVIDENCE_FIELDS if _clean(r.get(f))]
    dup_wp=_dups(wp_ids); dup_tid=_dups(tids); dup_sid=_dups(sids); dup_art=_dups([_clean(r.get("article_number")) for r in wp if _clean(r.get("evidence_collection_area"))=="retained_drain_body_nominal_length_evidence"])
    invalid_map=[]
    for i,r in enumerate(wp):
        exp_art=RETAINED_DRAIN_BODY_ARTICLE_NUMBERS[i] if i<3 else ""
        if i>=5 or _clean(r.get("workpack_row_id"))!=WORKPACK_ROW_IDS[i] or _clean(r.get("template_row_id"))!=REQUIRED_TEMPLATE_IDS[i] or _clean(r.get("source_aid_id"))!=REQUIRED_SOURCE_AID_IDS[i] or _clean(r.get("article_number"))!=exp_art or _clean(r.get("manual_completion_status"))!=EXPECTED_MANUAL[i]: invalid_map.append(_rowid(r,i+1))
    counts={f:sum(_bool(r.get(f)) for r in wp) for f in FALSE_FIELDS}; gen={f:_int(qa.get(f, sr.get(f, validation.get(f)))) for f in ZERO_FIELDS}
    invalid_block=[_rowid(r,i+1) for i,r in enumerate(wp) if any(_bool(r.get(f)) for f in FALSE_FIELDS)]
    prod=[_rowid(r,i+1) for i,r in enumerate(wp) if any(_bool(r.get(f)) for f in ["compatibility_pair_generation_allowed","candidate_pair_matrix_allowed","length_overlay_allowed","source_pack_mutation_allowed","production_promotion_allowed"])]
    ready=[_rowid(r,i+1) for i,r in enumerate(wp) if any(_bool(r.get(f)) for f in ["ready_for_future_diagnostic_design","benchmark_ready_allowed","customer_view_allowed"])]
    diag=[_rowid(r,i+1) for i,r in enumerate(wp) if not _bool(r.get("diagnostic_only"))]
    checks=[(validation.get("valid") is True,"evidence validation report valid must be true"),(sr.get("valid") is True,"source-aid report valid must be true"),(qa.get("valid") is True,"source-aid QA report valid must be true"),(family==EXPECTED_FAMILY and validation.get("family",family)==EXPECTED_FAMILY and sr.get("family",family)==EXPECTED_FAMILY and qa.get("family",family)==EXPECTED_FAMILY,"family must be TECEdrainprofile"),(len(inv)==EXPECTED_INPUT_INVENTORY_ROW_COUNT and len(fam)==EXPECTED_FAMILY_INVENTORY_ROW_COUNT and roles==EXPECTED_MACHINE_ROLE_COUNTS,"source-pack baseline counts differ from expected"),(len(tmpl)==5,"evidence template CSV must contain exactly 5 rows"),(len(src)==5,"source-aid CSV must contain exactly 5 rows"),(_int(sr.get("source_aid_row_count"))==5,"source-aid report row count must be 5"),(_int(qa.get("qa_row_count"))==5,"source-aid QA report qa_row_count must be 5"),(len(wp)==5,"workpack row count must be 5"),(len(draft)==5,"evidence draft row count must be 5"),(tids==REQUIRED_TEMPLATE_IDS,"required template IDs are missing or out of order"),(sids==REQUIRED_SOURCE_AID_IDS,"required source_aid IDs are missing or out of order"),(wp_ids==WORKPACK_ROW_IDS,"required workpack row IDs are missing or out of order"),(not dup_wp and not dup_tid and not dup_sid and not dup_art,"duplicate IDs or article workpack rows detected"),(not invalid_map,"workpack row mapping differs from expected"),(not man_wp,"manual evidence fields must be blank in workpack"),(not man_d,"manual evidence fields must be blank in evidence draft"),(tmpl_cols==list(draft[0].keys()) if draft else False,"evidence draft column order differs from template"),(all(v==0 for v in counts.values()),"workpack allowed flags must all be false"),(all(v==0 for v in gen.values()),"generation/mutation/promotion counters must be 0"),(qa.get("production_promotion_blocked") is True and qa.get("ready_for_benchmark") is False and qa.get("ready_for_customer_view") is False,"production/readiness gates differ"),(not prod and not ready and not diag,"production/readiness/diagnostic leakage detected")]
    # report-specific expected values
    for k,v in {"evidence_csv_row_count":5,"template_csv_row_count":5,"template_report_row_count":5,"evidence_complete_row_count":0,"evidence_incomplete_row_count":5,"missing_manual_evidence_row_count":5}.items(): checks.append((_int(validation.get(k))==v,f"evidence validation report {k} differs from expected"))
    checks += [(validation.get("retained_drain_body_article_numbers")==RETAINED_DRAIN_BODY_ARTICLE_NUMBERS,"retained drain body article numbers differ"),(validation.get("ready_for_future_diagnostic_design") is False and validation.get("production_promotion_blocked") is True and validation.get("ready_for_benchmark") is False and validation.get("ready_for_customer_view") is False,"evidence validation production gates differ"),(_int(validation.get("input_inventory_row_count", EXPECTED_INPUT_INVENTORY_ROW_COUNT))==EXPECTED_INPUT_INVENTORY_ROW_COUNT and _int(validation.get("family_inventory_row_count", EXPECTED_FAMILY_INVENTORY_ROW_COUNT))==EXPECTED_FAMILY_INVENTORY_ROW_COUNT and validation.get("current_machine_role_counts", EXPECTED_MACHINE_ROLE_COUNTS)==EXPECTED_MACHINE_ROLE_COUNTS,"evidence validation source-pack baseline counts differ"),(_int(qa.get("input_inventory_row_count", EXPECTED_INPUT_INVENTORY_ROW_COUNT))==EXPECTED_INPUT_INVENTORY_ROW_COUNT and _int(qa.get("family_inventory_row_count", EXPECTED_FAMILY_INVENTORY_ROW_COUNT))==EXPECTED_FAMILY_INVENTORY_ROW_COUNT and qa.get("current_machine_role_counts", EXPECTED_MACHINE_ROLE_COUNTS)==EXPECTED_MACHINE_ROLE_COUNTS,"source-aid QA source-pack baseline counts differ"),(_int(qa.get("source_aid_csv_row_count",5))==5 and _int(qa.get("source_aid_report_row_count",5))==5,"source-aid QA input row counts differ"),(qa.get("mapping_status_counts")=={"pass":5} and qa.get("blocking_status_counts")=={"pass":5} and qa.get("qa_status_counts")=={"pass":5},"source-aid QA status counts differ"),(_int(qa.get("explicit_compatibility_matrix_found_count"))==0,"explicit compatibility matrix count must be 0")]
    for ok,msg in checks:
        if not ok and msg not in errors: errors.append(msg)
    report={"valid":not errors,"errors":errors,"warnings":warnings,"family":family,"source_pack_path":str(source_pack),"evidence_template_csv_path":str(evidence_template_csv),"evidence_validation_report_path":str(evidence_validation_report),"source_aid_csv_path":str(source_aid_csv),"source_aid_report_path":str(source_aid_report),"source_aid_qa_report_path":str(source_aid_qa_report),"evidence_validation_report_valid":validation.get("valid") is True,"source_aid_report_valid":sr.get("valid") is True,"source_aid_qa_report_valid":qa.get("valid") is True,"input_inventory_row_count":len(inv),"family_inventory_row_count":len(fam),"current_machine_role_counts":roles,"evidence_template_row_count":len(tmpl),"evidence_draft_row_count":len(draft),"source_aid_csv_row_count":len(src),"source_aid_report_row_count":_int(sr.get("source_aid_row_count")),"source_aid_qa_row_count":_int(qa.get("qa_row_count")),"workpack_row_count":len(wp),"workpack_row_ids":wp_ids,"template_row_ids":tids,"source_aid_ids":sids,"retained_drain_body_article_numbers":RETAINED_DRAIN_BODY_ARTICLE_NUMBERS,"retained_drain_body_workpack_row_count":sum(_clean(r.get("evidence_collection_area"))=="retained_drain_body_nominal_length_evidence" for r in wp),"compatibility_matrix_workpack_row_count":sum(_clean(r.get("evidence_collection_area"))=="article_level_drain_body_to_profile_cover_compatibility_matrix" for r in wp),"profile_cover_scope_workpack_row_count":sum(_clean(r.get("evidence_collection_area"))=="profile_cover_scope_confirmation" for r in wp),"manual_completion_status_counts":_counts([_clean(r.get("manual_completion_status")) for r in wp]),"qa_status_counts":qa.get("qa_status_counts",{}),"mapping_status_counts":qa.get("mapping_status_counts",{}),"blocking_status_counts":qa.get("blocking_status_counts",{}),"manual_evidence_fields_prefilled_workpack_rows":man_wp,"manual_evidence_fields_prefilled_draft_rows":man_d,**{f"{f}_count":v for f,v in counts.items()},**gen,"duplicate_workpack_row_ids":dup_wp,"duplicate_template_row_ids":dup_tid,"duplicate_source_aid_ids":dup_sid,"duplicate_article_workpack_rows":dup_art,"invalid_family_rows":[_rowid(r,i+1) for i,r in enumerate(wp) if _clean(r.get("family"))!=EXPECTED_FAMILY],"invalid_workpack_rows":invalid_map,"invalid_draft_rows":[] if tmpl_cols==(list(draft[0].keys()) if draft else []) else ["column_order"],"invalid_mapping_rows":[] if not invalid_map else invalid_map,"invalid_blocking_rows":invalid_block,"production_leakage_rows":prod,"readiness_leakage_rows":ready,"diagnostic_only_leakage_rows":diag,"production_promotion_blocked":qa.get("production_promotion_blocked"),"ready_for_benchmark":qa.get("ready_for_benchmark"),"ready_for_customer_view":qa.get("ready_for_customer_view"),"output_workpack_csv_path":str(out_workpack) if out_workpack else None,"output_evidence_draft_csv_path":str(out_evidence_draft) if out_evidence_draft else None,"diagnostic_only_note":DIAGNOSTIC_ONLY_NOTE}
    if out_workpack:
        Path(out_workpack).parent.mkdir(parents=True,exist_ok=True); w=csv.DictWriter(Path(out_workpack).open("w",encoding="utf-8",newline=""),fieldnames=WORKPACK_COLUMNS,extrasaction="ignore"); w.writeheader(); w.writerows(wp)
    if out_evidence_draft:
        Path(out_evidence_draft).parent.mkdir(parents=True,exist_ok=True); w=csv.DictWriter(Path(out_evidence_draft).open("w",encoding="utf-8",newline=""),fieldnames=tmpl_cols,extrasaction="ignore"); w.writeheader(); w.writerows(draft)
    return wp,draft,report

def main(argv=None):
    p=argparse.ArgumentParser(description="Export read-only TECEdrainprofile explicit compatibility evidence manual workpack.")
    for a in ["evidence-template-csv","evidence-validation-report","source-aid-csv","source-aid-report","source-aid-qa-report","out-workpack","out-evidence-draft","json-out"]: p.add_argument(f"--{a}", required=True)
    p.add_argument("--source-pack", default=str(DEFAULT_SOURCE_PACK)); p.add_argument("--family", default=EXPECTED_FAMILY); p.add_argument("--json", action="store_true")
    a=p.parse_args(argv); _w,_d,r=build_workpack(a.evidence_template_csv,a.evidence_validation_report,a.source_aid_csv,a.source_aid_report,a.source_aid_qa_report,a.source_pack,a.family,a.out_workpack,a.out_evidence_draft)
    write_json_output(r,out=a.json_out)
    if a.json: write_json_output(r)
    return 0 if r["valid"] else 1
if __name__=="__main__": raise SystemExit(main())
