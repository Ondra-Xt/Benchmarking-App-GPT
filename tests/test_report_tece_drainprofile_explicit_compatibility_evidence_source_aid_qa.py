from __future__ import annotations

import csv, json, subprocess, sys
from types import SimpleNamespace
import pytest

from tools.export_tece_drainprofile_explicit_compatibility_evidence_source_aid import CSV_COLUMNS, REQUIRED_SOURCE_AID_IDS
from tools.report_tece_drainprofile_explicit_compatibility_evidence_source_aid_qa import DIAGNOSTIC_ONLY_NOTE, QA_ROW_IDS, build_qa


def _inventory(profile_count=30, total=436, family_count=76):
    rows=[]; roles=["accessory"]*5+["complete_set"]*4+["drain_body"]*12+["profile_cover"]*profile_count+["unknown"]*(family_count-21-profile_count)
    for i,role in enumerate(roles):
        rows.append(SimpleNamespace(article_number=str(i), tece_family_candidate="TECEdrainprofile", product_family="TECEdrainprofile", tece_article_role_candidate=role, article_role=role))
    while len(rows)<total: rows.append(SimpleNamespace(article_number=f"x{len(rows)}", tece_family_candidate="Other", product_family="Other", tece_article_role_candidate="unknown", article_role="unknown"))
    return SimpleNamespace(rows=rows)


def _source_rows():
    data=[
        (REQUIRED_SOURCE_AID_IDS[0],"673001","retained_drain_body_nominal_length_evidence","retained_drain_body_article_lookup","source_pack_article_context_found","true"),
        (REQUIRED_SOURCE_AID_IDS[1],"673002","retained_drain_body_nominal_length_evidence","retained_drain_body_article_lookup","source_pack_article_context_found","true"),
        (REQUIRED_SOURCE_AID_IDS[2],"673003","retained_drain_body_nominal_length_evidence","retained_drain_body_article_lookup","source_pack_article_context_found","true"),
        (REQUIRED_SOURCE_AID_IDS[3],"","article_level_drain_body_to_profile_cover_compatibility_matrix","compatibility_matrix_source_search_pointer","explicit_compatibility_matrix_not_found_in_current_source_pack","false"),
        (REQUIRED_SOURCE_AID_IDS[4],"","profile_cover_scope_confirmation","profile_cover_scope_summary","source_pack_profile_cover_scope_summary_available","true"),
    ]
    rows=[]
    for sid,article,area,target,status,found in data:
        r={c:"" for c in CSV_COLUMNS}; r.update({"source_aid_id":sid,"family":"TECEdrainprofile","article_number":article,"aid_area":area,"aid_target_type":target,"source_aid_status":status,"source_pack_row_found":found,"diagnostic_only":"true"})
        rows.append(r)
    return rows


@pytest.fixture
def case(tmp_path, monkeypatch):
    import tools.report_tece_drainprofile_explicit_compatibility_evidence_source_aid_qa as mod
    monkeypatch.setattr(mod, "load_source_pack", lambda _p: _inventory())
    csv_path=tmp_path/"aid.csv"
    with csv_path.open("w",encoding="utf-8",newline="") as fh:
        w=csv.DictWriter(fh, fieldnames=CSV_COLUMNS); w.writeheader(); w.writerows(_source_rows())
    source_report={"valid":True,"family":"TECEdrainprofile","evidence_validation_report_valid":True,"input_inventory_row_count":436,"family_inventory_row_count":76,"current_machine_role_counts":{"accessory":5,"complete_set":4,"drain_body":12,"profile_cover":30,"unknown":25},"evidence_template_row_count":5,"validation_evidence_csv_row_count":5,"validation_template_csv_row_count":5,"validation_template_report_row_count":5,"source_aid_row_count":5,"retained_drain_body_article_numbers":["673001","673002","673003"],"retained_drain_body_source_aid_row_count":3,"retained_drain_body_source_pack_found_count":3,"compatibility_matrix_source_aid_row_count":1,"explicit_compatibility_matrix_found_count":0,"profile_cover_scope_source_aid_row_count":1,"source_pack_profile_cover_count":30,"generated_compatibility_pair_count":0,"generated_candidate_pair_matrix_row_count":0,"generated_length_overlay_row_count":0,"proposed_source_pack_mutation_count":0,"production_safe_candidate_count":0,"production_promotion_blocked":True,"ready_for_benchmark":False,"ready_for_customer_view":False}
    validation={"valid":True,"family":"TECEdrainprofile"}
    sr=tmp_path/"source_report.json"; ev=tmp_path/"validation.json"
    sr.write_text(json.dumps(source_report),encoding="utf-8"); ev.write_text(json.dumps(validation),encoding="utf-8")
    return {"csv":csv_path,"sr":sr,"ev":ev,"source_report":source_report,"validation":validation,"out":tmp_path/"qa.csv"}

def _run(case, **kw): return build_qa(case["csv"], case["sr"], case["ev"], out=case["out"], **kw)
def _rewrite_csv(path, rows):
    with path.open("w",encoding="utf-8",newline="") as fh: w=csv.DictWriter(fh, fieldnames=CSV_COLUMNS); w.writeheader(); w.writerows(rows)

def test_valid_source_aid_qa_report_passes(case):
    rows, report=_run(case); assert report["valid"] is True; assert report["errors"]==[]; assert len(rows)==5; assert report["qa_status_counts"]["pass"]==5

def test_requires_valid_source_aid_report(case): case["source_report"]["valid"]=False; case["sr"].write_text(json.dumps(case["source_report"])); assert _run(case)[1]["valid"] is False
def test_requires_valid_evidence_validation_report(case): case["validation"]["valid"]=False; case["ev"].write_text(json.dumps(case["validation"])); assert _run(case)[1]["valid"] is False
def test_requires_tecedrainprofile_family(case): assert _run(case, family="Other")[1]["valid"] is False
def test_source_pack_baseline_counts_enforced(case, monkeypatch):
    import tools.report_tece_drainprofile_explicit_compatibility_evidence_source_aid_qa as mod; monkeypatch.setattr(mod,"load_source_pack",lambda _p:_inventory(profile_count=29)); assert _run(case)[1]["valid"] is False
def test_source_aid_csv_must_have_exactly_5_rows(case): rows=_source_rows()[:4]; _rewrite_csv(case["csv"],rows); assert _run(case)[1]["valid"] is False
def test_source_aid_report_row_count_enforced(case): case["source_report"]["source_aid_row_count"]=4; case["sr"].write_text(json.dumps(case["source_report"])); assert _run(case)[1]["valid"] is False
def test_required_source_aid_ids_enforced(case): rows=_source_rows(); rows[0]["source_aid_id"]="bad"; _rewrite_csv(case["csv"],rows); assert _run(case)[1]["valid"] is False
def test_required_qa_row_ids_generated(case): assert _run(case)[1]["qa_row_ids"] == QA_ROW_IDS
def test_exact_expected_row_mapping_enforced(case): rows=_source_rows(); rows[0]["article_number"]="673999"; _rewrite_csv(case["csv"],rows); assert _run(case)[1]["valid"] is False
def test_duplicate_qa_row_ids_fail(case): rows,_=_run(case); rows[1]["qa_row_id"]=rows[0]["qa_row_id"]; assert build_qa(case["csv"],case["sr"],case["ev"],out=case["out"],qa_rows_override=rows)[1]["valid"] is False
def test_duplicate_source_aid_ids_fail(case): rows=_source_rows(); rows[1]["source_aid_id"]=rows[0]["source_aid_id"]; _rewrite_csv(case["csv"],rows); assert _run(case)[1]["valid"] is False
def test_duplicate_retained_drain_body_article_source_aid_rows_fail(case): rows=_source_rows(); rows[1]["article_number"]=rows[0]["article_number"]; _rewrite_csv(case["csv"],rows); assert _run(case)[1]["valid"] is False
def test_no_3_x_30_matrix_rows_are_present(case): report=_run(case)[1]; assert report["qa_row_count"]==5; assert report["source_aid_csv_row_count"]!=90; assert report["generated_candidate_pair_matrix_row_count"]==0
@pytest.mark.parametrize("field", ["evidence_decision_allowed","evidence_complete","ready_for_future_diagnostic_design","compatibility_pair_generation_allowed","candidate_pair_matrix_allowed","length_overlay_allowed","source_pack_mutation_allowed","production_promotion_allowed","benchmark_ready_allowed","customer_view_allowed"])
def test_false_qa_flags_fail(case, field): rows,_=_run(case); rows[0][field]="true"; assert build_qa(case["csv"],case["sr"],case["ev"],out=case["out"],qa_rows_override=rows)[1]["valid"] is False
@pytest.mark.parametrize("field", ["generated_compatibility_pair_count","generated_candidate_pair_matrix_row_count","generated_length_overlay_row_count","production_safe_candidate_count"])
def test_generated_counts_fail(case, field): case["source_report"][field]=1; case["sr"].write_text(json.dumps(case["source_report"])); assert _run(case)[1]["valid"] is False
def test_diagnostic_only_note_is_present(case): assert _run(case)[1]["diagnostic_only_note"] == DIAGNOSTIC_ONLY_NOTE
def test_source_pack_immutability(case): before=case["csv"].read_bytes()+case["sr"].read_bytes()+case["ev"].read_bytes(); _run(case); after=case["csv"].read_bytes()+case["sr"].read_bytes()+case["ev"].read_bytes(); assert after==before
def test_aco_canonical_baseline_remains_pass_and_stable():
    result=subprocess.run([sys.executable,"tools/report_aco_final_baseline.py","--help"], text=True, capture_output=True)
    assert result.returncode == 0
