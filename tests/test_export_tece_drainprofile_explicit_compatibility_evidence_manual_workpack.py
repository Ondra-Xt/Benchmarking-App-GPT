from __future__ import annotations
import csv,json,subprocess,sys
from types import SimpleNamespace
import pytest
from tools.export_tece_drainprofile_explicit_compatibility_evidence_collection_template import template_rows, CSV_COLUMNS as TEMPLATE_COLUMNS
from tools.export_tece_drainprofile_explicit_compatibility_evidence_source_aid import CSV_COLUMNS as AID_COLUMNS, REQUIRED_SOURCE_AID_IDS
from tools.report_tece_drainprofile_explicit_compatibility_evidence_source_aid_qa import QA_ROW_IDS
from tools.export_tece_drainprofile_explicit_compatibility_evidence_manual_workpack import DIAGNOSTIC_ONLY_NOTE, WORKPACK_ROW_IDS, build_workpack

def _inventory(profile_count=30,total=436,family_count=76):
    roles=["accessory"]*5+["complete_set"]*4+["drain_body"]*12+["profile_cover"]*profile_count+["unknown"]*(family_count-21-profile_count)
    rows=[SimpleNamespace(article_number=str(i),tece_family_candidate="TECEdrainprofile",product_family="TECEdrainprofile",tece_article_role_candidate=r,article_role=r) for i,r in enumerate(roles)]
    while len(rows)<total: rows.append(SimpleNamespace(article_number=f"x{len(rows)}",tece_family_candidate="Other",product_family="Other",tece_article_role_candidate="unknown",article_role="unknown"))
    return SimpleNamespace(rows=rows)

def _aid_rows():
    data=[(0,"673001","retained_drain_body_nominal_length_evidence","retained_drain_body_article_lookup","source_pack_article_context_found"),(1,"673002","retained_drain_body_nominal_length_evidence","retained_drain_body_article_lookup","source_pack_article_context_found"),(2,"673003","retained_drain_body_nominal_length_evidence","retained_drain_body_article_lookup","source_pack_article_context_found"),(3,"","article_level_drain_body_to_profile_cover_compatibility_matrix","compatibility_matrix_source_search_pointer","explicit_compatibility_matrix_not_found_in_current_source_pack"),(4,"","profile_cover_scope_confirmation","profile_cover_scope_summary","source_pack_profile_cover_scope_summary_available")]
    rows=[]
    for i,article,area,target,status in data:
        r={c:"" for c in AID_COLUMNS}; r.update({"source_aid_id":REQUIRED_SOURCE_AID_IDS[i],"family":"TECEdrainprofile","related_template_row_id":f"TECE-DP-COMPAT-EVIDENCE-TEMPLATE-{i+1:06d}","article_number":article,"aid_area":area,"aid_target_type":target,"source_aid_status":status,"source_pack_row_found":"true","diagnostic_only":"true"}); rows.append(r)
    return rows

def _write_csv(path, cols, rows):
    with path.open("w",encoding="utf-8",newline="") as fh: w=csv.DictWriter(fh,fieldnames=cols); w.writeheader(); w.writerows(rows)

@pytest.fixture
def case(tmp_path, monkeypatch):
    import tools.export_tece_drainprofile_explicit_compatibility_evidence_manual_workpack as mod
    monkeypatch.setattr(mod,"load_source_pack",lambda _p:_inventory())
    tmpl=tmp_path/"template.csv"; _write_csv(tmpl,TEMPLATE_COLUMNS,template_rows())
    aid=tmp_path/"aid.csv"; _write_csv(aid,AID_COLUMNS,_aid_rows())
    validation={"valid":True,"family":"TECEdrainprofile","evidence_csv_row_count":5,"template_csv_row_count":5,"template_report_row_count":5,"retained_drain_body_article_numbers":["673001","673002","673003"],"evidence_complete_row_count":0,"evidence_incomplete_row_count":5,"missing_manual_evidence_row_count":5,"ready_for_future_diagnostic_design":False,"generated_compatibility_pair_count":0,"generated_candidate_pair_matrix_row_count":0,"generated_length_overlay_row_count":0,"proposed_source_pack_mutation_count":0,"production_safe_candidate_count":0,"production_promotion_blocked":True,"ready_for_benchmark":False,"ready_for_customer_view":False}
    sr={"valid":True,"family":"TECEdrainprofile","source_aid_row_count":5,"generated_compatibility_pair_count":0,"generated_candidate_pair_matrix_row_count":0,"generated_length_overlay_row_count":0,"proposed_source_pack_mutation_count":0,"production_safe_candidate_count":0,"production_promotion_blocked":True,"ready_for_benchmark":False,"ready_for_customer_view":False}
    qa={"valid":True,"family":"TECEdrainprofile","qa_row_count":5,"mapping_status_counts":{"pass":5},"blocking_status_counts":{"pass":5},"qa_status_counts":{"pass":5},"explicit_compatibility_matrix_found_count":0,"generated_compatibility_pair_count":0,"generated_candidate_pair_matrix_row_count":0,"generated_length_overlay_row_count":0,"proposed_source_pack_mutation_count":0,"production_safe_candidate_count":0,"production_promotion_blocked":True,"ready_for_benchmark":False,"ready_for_customer_view":False}
    ev=tmp_path/"ev.json"; sp=tmp_path/"sr.json"; qp=tmp_path/"qa.json"; ev.write_text(json.dumps(validation)); sp.write_text(json.dumps(sr)); qp.write_text(json.dumps(qa))
    return {"tmpl":tmpl,"aid":aid,"ev":ev,"srp":sp,"qap":qp,"validation":validation,"sr":sr,"qa":qa,"wp":tmp_path/"wp.csv","draft":tmp_path/"draft.csv"}

def _run(c, **kw): return build_workpack(c["tmpl"],c["ev"],c["aid"],c["srp"],c["qap"],out_workpack=c["wp"],out_evidence_draft=c["draft"],**kw)
def _rep(c, **kw): return _run(c,**kw)[2]

def test_valid_manual_workpack_export_passes(case): assert _rep(case)["valid"] is True
def test_requires_valid_evidence_validation_report(case): case["validation"]["valid"]=False; case["ev"].write_text(json.dumps(case["validation"])); assert not _rep(case)["valid"]
def test_requires_valid_source_aid_report(case): case["sr"]["valid"]=False; case["srp"].write_text(json.dumps(case["sr"])); assert not _rep(case)["valid"]
def test_requires_valid_source_aid_qa_report(case): case["qa"]["valid"]=False; case["qap"].write_text(json.dumps(case["qa"])); assert not _rep(case)["valid"]
def test_requires_tecedrainprofile_family(case): assert not _rep(case,family="Other")["valid"]
def test_source_pack_baseline_counts_enforced(case, monkeypatch):
    import tools.export_tece_drainprofile_explicit_compatibility_evidence_manual_workpack as mod; monkeypatch.setattr(mod,"load_source_pack",lambda _p:_inventory(profile_count=29)); assert not _rep(case)["valid"]
def test_evidence_template_csv_must_have_exactly_5_rows(case): _write_csv(case["tmpl"],TEMPLATE_COLUMNS,template_rows()[:4]); assert not _rep(case)["valid"]
def test_source_aid_csv_must_have_exactly_5_rows(case): _write_csv(case["aid"],AID_COLUMNS,_aid_rows()[:4]); assert not _rep(case)["valid"]
def test_workpack_csv_has_exactly_5_rows(case): _run(case); assert len(list(csv.DictReader(open(case["wp"],encoding="utf-8"))))==5
def test_evidence_draft_csv_has_exactly_5_rows(case): _run(case); assert len(list(csv.DictReader(open(case["draft"],encoding="utf-8"))))==5
def test_evidence_draft_preserves_template_column_order(case): _run(case); assert csv.DictReader(open(case["draft"],encoding="utf-8")).fieldnames==TEMPLATE_COLUMNS
def test_required_workpack_row_ids_generated(case): assert _rep(case)["workpack_row_ids"]==WORKPACK_ROW_IDS
def test_required_template_ids_enforced(case): rows=template_rows(); rows[0]["template_row_id"]="bad"; _write_csv(case["tmpl"],TEMPLATE_COLUMNS,rows); assert not _rep(case)["valid"]
def test_required_source_aid_ids_enforced(case): rows=_aid_rows(); rows[0]["source_aid_id"]="bad"; _write_csv(case["aid"],AID_COLUMNS,rows); assert not _rep(case)["valid"]
def test_exact_row_mapping_enforced(case): wp,_,_=_run(case); wp[0]["article_number"]="bad"; assert not build_workpack(case["tmpl"],case["ev"],case["aid"],case["srp"],case["qap"],workpack_rows_override=wp)[2]["valid"]
def test_manual_fields_remain_blank_in_workpack(case): assert _rep(case)["manual_evidence_fields_prefilled_workpack_rows"]==[]
def test_manual_fields_remain_blank_in_evidence_draft(case): assert _rep(case)["manual_evidence_fields_prefilled_draft_rows"]==[]
def test_duplicate_workpack_row_ids_fail(case): wp,_,_=_run(case); wp[1]["workpack_row_id"]=wp[0]["workpack_row_id"]; assert not build_workpack(case["tmpl"],case["ev"],case["aid"],case["srp"],case["qap"],workpack_rows_override=wp)[2]["valid"]
def test_duplicate_template_row_ids_fail(case): rows=template_rows(); rows[1]["template_row_id"]=rows[0]["template_row_id"]; _write_csv(case["tmpl"],TEMPLATE_COLUMNS,rows); assert not _rep(case)["valid"]
def test_duplicate_source_aid_ids_fail(case): rows=_aid_rows(); rows[1]["source_aid_id"]=rows[0]["source_aid_id"]; _write_csv(case["aid"],AID_COLUMNS,rows); assert not _rep(case)["valid"]
def test_duplicate_retained_drain_body_article_rows_fail(case): wp,_,_=_run(case); wp[1]["article_number"]=wp[0]["article_number"]; assert not build_workpack(case["tmpl"],case["ev"],case["aid"],case["srp"],case["qap"],workpack_rows_override=wp)[2]["valid"]
def test_no_3_x_30_matrix_rows_are_generated(case): r=_rep(case); assert r["workpack_row_count"]==5 and r["generated_candidate_pair_matrix_row_count"]==0
@pytest.mark.parametrize("field",["evidence_decision_allowed","evidence_complete","ready_for_future_diagnostic_design","compatibility_pair_generation_allowed","candidate_pair_matrix_allowed","length_overlay_allowed","source_pack_mutation_allowed","production_promotion_allowed","benchmark_ready_allowed","customer_view_allowed"])
def test_true_workpack_flags_fail(case,field): wp,_,_=_run(case); wp[0][field]="true"; assert not build_workpack(case["tmpl"],case["ev"],case["aid"],case["srp"],case["qap"],workpack_rows_override=wp)[2]["valid"]
@pytest.mark.parametrize("field",["generated_compatibility_pair_count","generated_candidate_pair_matrix_row_count","generated_length_overlay_row_count","production_safe_candidate_count"])
def test_generated_counts_fail(case,field): case["qa"][field]=1; case["qap"].write_text(json.dumps(case["qa"])); assert not _rep(case)["valid"]
def test_diagnostic_only_note_is_present(case): assert _rep(case)["diagnostic_only_note"]==DIAGNOSTIC_ONLY_NOTE
def test_source_pack_input_immutability(case): b=case["tmpl"].read_bytes()+case["aid"].read_bytes()+case["ev"].read_bytes()+case["srp"].read_bytes()+case["qap"].read_bytes(); _run(case); assert b==case["tmpl"].read_bytes()+case["aid"].read_bytes()+case["ev"].read_bytes()+case["srp"].read_bytes()+case["qap"].read_bytes()
def test_aco_canonical_baseline_remains_pass_and_stable(): assert subprocess.run([sys.executable,"tools/report_aco_final_baseline.py","--help"],capture_output=True).returncode==0
