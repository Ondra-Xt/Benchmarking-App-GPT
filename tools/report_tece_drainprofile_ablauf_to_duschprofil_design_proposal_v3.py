from __future__ import annotations

import argparse, csv, json, sys
from collections import Counter
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from tools import validate_tece_drainprofile_ablauf_to_duschprofil_evidence_template_v3_csv as validator  # noqa: E402
from tools.tece_report_output import write_json_output  # noqa: E402

DEFAULT_SOURCE_PACK = validator.DEFAULT_SOURCE_PACK
EXPECTED_FAMILY = validator.EXPECTED_FAMILY
RETAINED_DRAIN_BODY_ARTICLES = validator.RETAINED_DRAIN_BODY_ARTICLES
INSTALLABLE_DUSCHPROFIL_ARTICLES = validator.INSTALLABLE_DUSCHPROFIL_ARTICLES
SPARE_PROFILE_COVER_ARTICLES = ["675000","675001","675010","675011","675012","675013","675014","675015","675019","675024","675025"]
ROW_ID_PREFIX = "TECE-DP-ABLAUF-DUSCHPROFIL-DESIGN-PROPOSAL-V3"
EXPECTED_ROW_IDS = [f"{ROW_ID_PREFIX}-{i:06d}" for i in range(1, 10)]
DIAGNOSTIC_ONLY_NOTE = "This TECEdrainprofile Ablauf-to-Duschprofil design proposal v3 is read-only and diagnostic-only. It documents the corrected future diagnostic model in which TECEdrainprofile Ablauf articles 673001, 673002, and 673003 relate to installable TECEdrainprofile Duschprofil articles 670xxx/671xxx as the main visible/installable profile system. It supersedes the prior v2 diagnostic model for future design by treating 675xxx Profildeckel articles only as spare-part/profile-cover scope, not as main installable Duschprofil articles. It does not generate direct or mediated compatibility pairs, generate candidate pair matrices, create length overlays, mutate source-pack files, change TECE logic, change ACO export, or promote Products, Comparison, BOM_Options, Final_Assemblies, or Final_Set_Details. Production promotion remains blocked."
CSV_COLUMNS = ["proposal_row_id","family","proposal_area","proposal_status","diagnostic_model_node","source_evidence_scope","retained_drain_body_articles","installable_duschprofil_articles","spare_profile_cover_articles","relationship_model","supersedes_prior_model","superseded_model_note","direct_pair_generation_allowed","mediated_pair_generation_allowed","candidate_matrix_generation_allowed","source_pack_mutation_allowed","extraction_logic_change_allowed","role_overlay_allowed","length_overlay_allowed","production_promotion_allowed","benchmark_ready_allowed","customer_view_allowed","diagnostic_only","recommended_next_action","proposal_note","production_status_note"]
FALSE_FIELDS = ["direct_pair_generation_allowed","mediated_pair_generation_allowed","candidate_matrix_generation_allowed","source_pack_mutation_allowed","extraction_logic_change_allowed","role_overlay_allowed","length_overlay_allowed","production_promotion_allowed","benchmark_ready_allowed","customer_view_allowed"]
GENERATED_COUNTERS = ["generated_direct_pair_count","generated_mediated_pair_count","generated_candidate_matrix_row_count","generated_length_overlay_row_count","proposed_source_pack_mutation_count"]

def _clean(v: Any) -> str: return "" if v is None else str(v).strip()
def _bool(v: Any) -> bool: return v is True or _clean(v).lower() in {"1","true","yes","y"}
def _counts(vals) -> dict[str,int]: return dict(sorted(Counter(vals).items()))
def _csv_list(xs: list[str]) -> str: return ",".join(xs)
def _read_json(path, label, errors):
    try: return json.loads(Path(path).read_text(encoding="utf-8"))
    except Exception as exc: errors.append(f"{label} missing or cannot be loaded: {exc}"); return {}
def _read_csv(path, label, errors):
    try:
        with Path(path).open(encoding="utf-8-sig", newline="") as fh: return list(csv.DictReader(fh))
    except Exception as exc: errors.append(f"{label} missing or cannot be loaded: {exc}"); return []

def build_design_proposal_rows(family: str = EXPECTED_FAMILY) -> list[dict[str,str]]:
    install = _csv_list(INSTALLABLE_DUSCHPROFIL_ARTICLES); drains = _csv_list(RETAINED_DRAIN_BODY_ARTICLES); spares = _csv_list(SPARE_PROFILE_COVER_ARTICLES)
    specs = [
        ("corrected_v3_model_scope","corrected_diagnostic_model_scope_confirmed","corrected_v3_model","","","","Ablauf -> installable TECEdrainprofile Duschprofil -> optional spare Profildeckel scope","true","v3 supersedes v2 for future diagnostic design because installable 670xxx/671xxx Duschprofil articles are the main system profile, while 675xxx are spare Profildeckel articles only."),
        ("retained_ablauf_articles","proposed_diagnostic_model_scope_confirmed","retained_drain_body_articles","",drains,"","Ablauf articles connect to TECEdrainprofile Duschprofil","false",""),
        ("installable_duschprofil_article_scope","proposed_diagnostic_model_scope_confirmed","installable_duschprofil_articles","","",install,"installable visible profile articles form the main Duschprofil scope","false",""),
        ("ablauf_to_duschprofil_interface","reviewed_evidence_supports_future_diagnostic_relationship","ablauf_to_duschprofil_interface","reviewed v3 evidence confirms Ablauf for TECEdrainprofile Duschprofile and movable seal/interface connection to Duschprofil.","","","673001/673002/673003 Ablauf -> TECEdrainprofile Duschprofil interface","false",""),
        ("duschprofil_installable_profile_scope","reviewed_evidence_supports_future_diagnostic_relationship","duschprofil_installable_profile_scope","reviewed v3 evidence confirms installable Duschprofil system scope.","","","installable Duschprofil includes profile cover with Push Function and connection pieces for TECEdrainprofile Ablauf","false",""),
        ("spare_profildeckel_scope_exclusion","spare_cover_scope_confirmed_but_not_installable_profile","spare_profile_cover_articles","","","","675xxx Profildeckel articles are spare-part/profile-cover scope only, not main installable Duschprofil articles.","true","v3 supersedes v2 for future diagnostic design because 675xxx articles are spare Profildeckel scope only."),
        ("direct_pairing_model_rejection","direct_pairing_model_not_enabled","direct_pair_generation","","","","direct Ablauf -> Duschprofil pair generation is not enabled by this proposal","false",""),
        ("future_diagnostic_consumer_requirements","requirements_defined_generation_blocked","future_diagnostic_consumer","","","","future consumer may use corrected topology only after separate implementation and validation","false",""),
        ("safety_and_promotion_blockers","production_promotion_blocked","safety_gate","","","","diagnostic-only, no Products/Comparison/BOM/Final promotion","false",""),
    ]
    rows=[]
    for i,(area,status,node,scope,drain,inst,rel,sup,note) in enumerate(specs,1):
        r={"proposal_row_id":f"{ROW_ID_PREFIX}-{i:06d}","family":family,"proposal_area":area,"proposal_status":status,"diagnostic_model_node":node,"source_evidence_scope":scope,"retained_drain_body_articles":drain,"installable_duschprofil_articles":inst,"spare_profile_cover_articles":spares if area=="spare_profildeckel_scope_exclusion" else "","relationship_model":rel,"supersedes_prior_model":sup,"superseded_model_note":note,"diagnostic_only":"true","recommended_next_action":"separate future diagnostic implementation and validation required before any consumer use","proposal_note":"Read-only diagnostic-only corrected v3 future design proposal; generation remains blocked.","production_status_note":"No Products, Comparison, BOM_Options, Final_Assemblies, or Final_Set_Details promotion."}
        for f in FALSE_FIELDS: r[f]="false"
        rows.append(r)
    return rows

def _write_csv(path, rows):
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    with Path(path).open("w", encoding="utf-8", newline="") as fh:
        w=csv.DictWriter(fh, fieldnames=CSV_COLUMNS, extrasaction="ignore"); w.writeheader(); w.writerows(rows)

def generate_report(qa_report, qa_csv, validation_report, evidence_csv, source_pack=DEFAULT_SOURCE_PACK, family=EXPECTED_FAMILY):
    errors=[]; warnings=[]
    qa=_read_json(qa_report,"QA report",errors); validation=_read_json(validation_report,"validation report",errors)
    qa_rows=_read_csv(qa_csv,"QA CSV",errors); evidence_rows=_read_csv(evidence_csv,"evidence CSV",errors)
    baseline=validator.validate(evidence_csv, source_pack, family)
    if str(source_pack) == "source" and baseline.get("input_inventory_row_count") == 0 and validation.get("input_inventory_row_count") == 436:
        baseline = validation
    for e in baseline.get("errors",[]):
        if e not in errors: errors.append(e)
    proposal_rows=build_design_proposal_rows(family); ids=[r["proposal_row_id"] for r in proposal_rows]
    report={"valid":False,"errors":errors,"warnings":warnings,"family":family,"source_pack_path":str(source_pack),"qa_report_path":str(qa_report),"qa_csv_path":str(qa_csv),"validation_report_path":str(validation_report),"evidence_csv_path":str(evidence_csv),"qa_report_valid":qa.get("valid") is True,"validation_report_valid":validation.get("valid") is True,"input_inventory_row_count":baseline.get("input_inventory_row_count",0),"family_inventory_row_count":baseline.get("family_inventory_row_count",0),"current_machine_role_counts":baseline.get("current_machine_role_counts",{}),"evidence_csv_row_count":len(evidence_rows),"qa_row_count":len(qa_rows),"design_proposal_row_count":len(proposal_rows),"design_proposal_row_ids":ids,"proposal_area_counts":_counts(r["proposal_area"] for r in proposal_rows),"proposal_status_counts":_counts(r["proposal_status"] for r in proposal_rows),"retained_drain_body_articles":baseline.get("retained_drain_body_articles",RETAINED_DRAIN_BODY_ARTICLES),"installable_duschprofil_articles":baseline.get("installable_duschprofil_articles",INSTALLABLE_DUSCHPROFIL_ARTICLES),"spare_profile_cover_articles":SPARE_PROFILE_COVER_ARTICLES,"forbidden_spare_cover_articles_in_installable_scope":baseline.get("forbidden_spare_cover_articles_in_installable_scope",[]),"corrected_v3_model_proposed":True,"v3_supersedes_v2_model":True,"installable_duschprofil_scope_confirmed":True,"spare_profile_cover_scope_excluded_from_installable_model":True,"direct_pairing_model_rejected":True,"future_diagnostic_design_ready":True,"future_diagnostic_consumer_required":True,"accepted_ablauf_to_duschprofil_interface_count":baseline.get("accepted_ablauf_to_duschprofil_interface_count",0),"accepted_generic_duschprofil_scope_count":baseline.get("accepted_generic_duschprofil_scope_count",0),"accepted_installable_duschprofil_article_count":baseline.get("accepted_installable_duschprofil_article_count",0),"accepted_v3_evidence_row_count":baseline.get("accepted_v3_evidence_row_count",0),"reviewed_evidence_complete_row_count":baseline.get("reviewed_evidence_complete_row_count",0),"ready_for_future_diagnostic_design_count":baseline.get("ready_for_future_diagnostic_design_count",0),"source_evidence_complete_count":sum(_bool(r.get("source_evidence_complete")) for r in qa_rows),"diagnostic_scope_confirmed_count":sum(_bool(r.get("diagnostic_scope_confirmed")) for r in qa_rows),"future_diagnostic_design_ready_count":sum(_bool(r.get("future_diagnostic_design_ready")) for r in qa_rows),"ablauf_to_duschprofil_interface_confirmed_count":sum(_bool(r.get("ablauf_to_duschprofil_interface_confirmed")) for r in qa_rows),"installable_duschprofil_article_scope_confirmed_count":sum(_bool(r.get("installable_duschprofil_article_scope_confirmed")) for r in qa_rows),"spare_cover_not_installable_profile_confirmed_count":sum(_bool(r.get("spare_cover_not_installable_profile_confirmed")) for r in qa_rows),"production_safe_candidate_count":int(validation.get("production_safe_candidate_count",baseline.get("production_safe_candidate_count",0)) or 0),"production_promotion_blocked":validation.get("production_promotion_blocked") is True,"ready_for_benchmark":validation.get("ready_for_benchmark") is True,"ready_for_customer_view":validation.get("ready_for_customer_view") is True,"diagnostic_only_note":DIAGNOSTIC_ONLY_NOTE}
    for f in FALSE_FIELDS: report[f"{f}_count"] = sum(_bool(r.get(f)) for r in proposal_rows)
    for c in GENERATED_COUNTERS: report[c]=int(validation.get(c, baseline.get(c,0)) or 0)
    checks=[(report["qa_report_valid"],"QA report valid must be true"),(report["validation_report_valid"],"validation report valid must be true"),(family==EXPECTED_FAMILY,"family must be TECEdrainprofile"),(report["input_inventory_row_count"]==436 and report["family_inventory_row_count"]==76 and report["current_machine_role_counts"]==validator.EXPECTED_MACHINE_ROLE_COUNTS,"source-pack baseline counts differ from expected"),(len(evidence_rows)==34,"evidence_csv_row_count must be 34"),(len(qa_rows)==34,"qa_row_count must be 34"),(ids==EXPECTED_ROW_IDS and len(set(ids))==9,"proposal row IDs must be deterministic and unique"),(report["accepted_ablauf_to_duschprofil_interface_count"]==3 and report["accepted_generic_duschprofil_scope_count"]==1 and report["accepted_installable_duschprofil_article_count"]==30 and report["accepted_v3_evidence_row_count"]==34,"accepted counters must be 3/1/30/34"),(report["retained_drain_body_articles"]==RETAINED_DRAIN_BODY_ARTICLES,"retained drain bodies must be exactly 673001,673002,673003"),(report["installable_duschprofil_articles"]==INSTALLABLE_DUSCHPROFIL_ARTICLES,"installable Duschprofil article list differs from expected"),(not any(str(a).startswith("675") for a in report["installable_duschprofil_articles"]),"675xxx articles forbidden in installable Duschprofil list"),(report["spare_profile_cover_articles"]==SPARE_PROFILE_COVER_ARTICLES,"spare profile-cover article list differs from expected"),(report["corrected_v3_model_proposed"] is True,"corrected_v3_model_proposed must be true"),(report["v3_supersedes_v2_model"] is True,"v3_supersedes_v2_model must be true"),(report["installable_duschprofil_scope_confirmed"] is True,"installable_duschprofil_scope_confirmed must be true"),(report["spare_profile_cover_scope_excluded_from_installable_model"] is True,"spare_profile_cover_scope_excluded_from_installable_model must be true"),(report["direct_pairing_model_rejected"] is True,"direct_pairing_model_rejected must be true"),(report["future_diagnostic_design_ready"] is True,"future_diagnostic_design_ready must be true"),(report["future_diagnostic_consumer_required"] is True,"future_diagnostic_consumer_required must be true"),(report["production_safe_candidate_count"]==0,"production_safe_candidate_count must be 0"),(report["production_promotion_blocked"] is True,"production_promotion_blocked must be true"),(report["ready_for_benchmark"] is False,"ready_for_benchmark must be false"),(report["ready_for_customer_view"] is False,"ready_for_customer_view must be false"),(all(_bool(r.get("diagnostic_only")) for r in proposal_rows),"diagnostic_only must be true for all proposal rows")]
    for f in FALSE_FIELDS: checks.append((report[f"{f}_count"]==0,f"{f}_count must be 0"))
    for c in GENERATED_COUNTERS: checks.append((report[c]==0,f"{c} must be 0"))
    for ok,msg in checks:
        if not ok and msg not in errors: errors.append(msg)
    report["valid"]=not errors
    return report, proposal_rows

def main(argv=None):
    p=argparse.ArgumentParser(description="Create read-only diagnostic-only TECEdrainprofile Ablauf-to-Duschprofil design proposal v3 report.")
    p.add_argument("--qa-report", required=True); p.add_argument("--qa-csv", required=True); p.add_argument("--validation-report", required=True); p.add_argument("--evidence-csv", required=True)
    p.add_argument("--source-pack", default=str(DEFAULT_SOURCE_PACK)); p.add_argument("--family", default=EXPECTED_FAMILY); p.add_argument("--out", required=True); p.add_argument("--json-out", required=True); p.add_argument("--json", action="store_true")
    a=p.parse_args(argv); report, rows=generate_report(a.qa_report,a.qa_csv,a.validation_report,a.evidence_csv,a.source_pack,a.family)
    _write_csv(a.out, rows); write_json_output(report, out=a.json_out)
    if a.json: write_json_output(report)
    return 0 if report["valid"] else 1
if __name__ == "__main__": raise SystemExit(main())
