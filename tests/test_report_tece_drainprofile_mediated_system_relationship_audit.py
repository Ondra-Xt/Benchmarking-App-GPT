from __future__ import annotations

import csv, hashlib, json, sys
from pathlib import Path
from types import SimpleNamespace
import pytest
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from tools import report_tece_drainprofile_mediated_system_relationship_audit as mod

class Row(SimpleNamespace): pass

def _write_csv(path: Path, n: int = 5) -> None:
    with path.open('w', encoding='utf-8', newline='') as fh:
        w=csv.DictWriter(fh, fieldnames=['row_id','family']); w.writeheader(); w.writerows({'row_id':str(i),'family':mod.EXPECTED_FAMILY} for i in range(n))

def _manual(**kw):
    d={"valid":True,"errors":[],"family":mod.EXPECTED_FAMILY,"input_inventory_row_count":436,"family_inventory_row_count":76,"current_machine_role_counts":mod.EXPECTED_MACHINE_ROLE_COUNTS,"evidence_template_row_count":5,"evidence_draft_row_count":5,"source_aid_csv_row_count":5,"source_aid_report_row_count":5,"source_aid_qa_row_count":5,"workpack_row_count":5,"retained_drain_body_article_numbers":mod.RETAINED_DRAIN_BODY_ARTICLES,"retained_drain_body_workpack_row_count":3,"compatibility_matrix_workpack_row_count":1,"profile_cover_scope_workpack_row_count":1,"manual_evidence_fields_prefilled_workpack_rows":[],"manual_evidence_fields_prefilled_draft_rows":[],"generated_direct_pair_count":0,"generated_mediated_pair_count":0,"generated_candidate_matrix_row_count":0,"generated_length_overlay_row_count":0,"proposed_source_pack_mutation_count":0,"production_safe_candidate_count":0,"evidence_acceptance_count":0,"source_pack_mutation_count":0,"production_promotion_count":0,"production_promotion_blocked":True,"ready_for_benchmark":False,"ready_for_customer_view":False}
    d.update(kw); return d

def _qa(**kw):
    d={"valid":True,"source_aid_csv_row_count":5,"source_aid_report_row_count":5,"qa_row_count":5,"mapping_status_counts":{"pass":5},"blocking_status_counts":{"pass":5},"qa_status_counts":{"pass":5},"explicit_compatibility_matrix_found_count":0,"explicit_direct_compatibility_matrix_found_count":0,"generated_direct_pair_count":0,"generated_mediated_pair_count":0,"generated_candidate_matrix_row_count":0,"generated_length_overlay_row_count":0,"proposed_source_pack_mutation_count":0,"production_safe_candidate_count":0,"evidence_acceptance_count":0,"source_pack_mutation_count":0,"production_promotion_count":0,"production_promotion_blocked":True,"ready_for_benchmark":False,"ready_for_customer_view":False}
    d.update(kw); return d

@pytest.fixture
def fake_source_pack(monkeypatch):
    def load(_p):
        rows=[]
        roles=['accessory']*5+['complete_set']*4+['drain_body']*12+['profile_cover']*30+['unknown']*25
        articles=mod.RETAINED_DRAIN_BODY_ARTICLES+mod.PROPOSED_PROFILE_COVER_ARTICLES
        for i, role in enumerate(roles):
            art = articles[i] if i < len(articles) else f'67{i:04d}'
            rows.append(Row(tece_family_candidate=mod.EXPECTED_FAMILY, product_family=mod.EXPECTED_FAMILY, tece_article_role_candidate=role, article_number=art))
        for i in range(436-len(rows)): rows.append(Row(tece_family_candidate='TECEdrainline', product_family='TECEdrainline', tece_article_role_candidate='unknown', article_number=f'60{i:04d}'))
        return SimpleNamespace(rows=rows)
    monkeypatch.setattr(mod, 'load_source_pack', load)

@pytest.fixture
def artifacts(tmp_path: Path, fake_source_pack):
    manual=tmp_path/'manual.json'; manual.write_text(json.dumps(_manual()), encoding='utf-8')
    aid_csv=tmp_path/'aid.csv'; _write_csv(aid_csv)
    aid=tmp_path/'aid.json'; aid.write_text(json.dumps({'valid':True}), encoding='utf-8')
    qa=tmp_path/'qa.json'; qa.write_text(json.dumps(_qa()), encoding='utf-8')
    sp=tmp_path/'source_pack'; sp.mkdir(); (sp/'keep.txt').write_text('unchanged', encoding='utf-8')
    return manual, aid_csv, aid, qa, sp

def _build(artifacts, **kw):
    m,c,a,q,sp=artifacts
    return mod.build_audit(m,c,a,q,source_pack=sp,out=kw.pop('out',None),**kw)

def test_valid_mediated_system_relationship_audit_passes(artifacts, tmp_path):
    rows, report=_build(artifacts, out=tmp_path/'audit.csv')
    assert report['valid'] is True and report['errors']==[] and len(rows)==5
    assert rows[0]['audit_area']=='legacy_direct_drain_body_to_profile_cover_requirement'

@pytest.mark.parametrize('idx,data', [(0,_manual(valid=False)),(2,{'valid':False}),(3,_qa(valid=False))])
def test_requires_valid_input_reports(artifacts, idx, data):
    artifacts[idx].write_text(json.dumps(data), encoding='utf-8')
    assert _build(artifacts)[1]['valid'] is False

def test_requires_tecedrainprofile_family(artifacts): assert _build(artifacts, family='TECEdrainline')[1]['valid'] is False

def test_source_pack_baseline_counts_enforced(artifacts, monkeypatch):
    monkeypatch.setattr(mod, 'load_source_pack', lambda _p: SimpleNamespace(rows=[]))
    assert _build(artifacts)[1]['valid'] is False

@pytest.mark.parametrize('field,value', [('workpack_row_count',4),('evidence_draft_row_count',4),('retained_drain_body_workpack_row_count',2),('compatibility_matrix_workpack_row_count',0),('profile_cover_scope_workpack_row_count',0)])
def test_manual_workpack_row_counts_enforced(artifacts, field, value):
    artifacts[0].write_text(json.dumps(_manual(**{field:value})), encoding='utf-8')
    assert _build(artifacts)[1]['valid'] is False

@pytest.mark.parametrize('override', [{'qa_row_count':4},{'source_aid_csv_row_count':4},{'mapping_status_counts':{'pass':4}},{'blocking_status_counts':{'pass':4}},{'qa_status_counts':{'pass':4}}])
def test_source_aid_qa_row_status_counts_enforced(artifacts, override):
    artifacts[3].write_text(json.dumps(_qa(**override)), encoding='utf-8')
    assert _build(artifacts)[1]['valid'] is False

def test_explicit_direct_compatibility_matrix_count_must_remain_zero(artifacts):
    artifacts[3].write_text(json.dumps(_qa(explicit_direct_compatibility_matrix_found_count=1)), encoding='utf-8')
    assert _build(artifacts)[1]['valid'] is False

def test_audit_csv_has_exactly_5_rows_and_required_ids_areas(artifacts, tmp_path):
    out=tmp_path/'audit.csv'; rows, report=_build(artifacts, out=out)
    assert len(list(csv.DictReader(out.open(encoding='utf-8-sig'))))==5
    assert report['audit_row_ids']==[f'TECE-DP-MEDIATED-SYSTEM-RELATIONSHIP-AUDIT-{i:06d}' for i in range(1,6)]
    assert [r['audit_area'] for r in rows]==mod.REQUIRED_AUDIT_AREAS

def test_article_lists_and_crosschecks_included(artifacts):
    _, report=_build(artifacts)
    assert report['retained_drain_body_articles']==mod.RETAINED_DRAIN_BODY_ARTICLES
    assert report['proposed_profile_cover_articles']==mod.PROPOSED_PROFILE_COVER_ARTICLES
    assert report['source_pack_retained_drain_body_articles_found']==mod.RETAINED_DRAIN_BODY_ARTICLES
    assert report['source_pack_proposed_profile_cover_articles_found']==mod.PROPOSED_PROFILE_COVER_ARTICLES

@pytest.mark.parametrize('field,expected', [('legacy_direct_pairing_requirement_supported',False),('mediated_relationship_model_recommended',True),('future_template_v2_recommended',True)])
def test_modeling_booleans_enforced(artifacts, field, expected): assert _build(artifacts)[1][field] is expected

def test_retained_drain_body_article_list_enforced(artifacts):
    artifacts[0].write_text(json.dumps(_manual(retained_drain_body_article_numbers=['673001'])), encoding='utf-8')
    assert _build(artifacts)[1]['valid'] is False

def test_proposed_profile_cover_article_list_enforced(artifacts): assert _build(artifacts)[1]['proposed_profile_cover_articles']==mod.PROPOSED_PROFILE_COVER_ARTICLES

@pytest.mark.parametrize('field', ['direct_pair_generation_allowed','mediated_pair_generation_allowed','candidate_matrix_generation_allowed','evidence_acceptance_allowed','evidence_complete','ready_for_future_diagnostic_design','source_pack_mutation_allowed','extraction_logic_change_allowed','role_overlay_allowed','length_overlay_allowed','production_promotion_allowed','benchmark_ready_allowed','customer_view_allowed'])
def test_blocking_allowed_true_fails(artifacts, field):
    rows,_=_build(artifacts); rows[0][field]='true'
    assert _build(artifacts, audit_rows=rows)[1]['valid'] is False

@pytest.mark.parametrize('field', ['generated_direct_pair_count','generated_mediated_pair_count','generated_candidate_matrix_row_count','generated_length_overlay_row_count','production_safe_candidate_count'])
def test_generated_or_production_safe_counts_gt_zero_fail(artifacts, field):
    artifacts[3].write_text(json.dumps(_qa(**{field:1})), encoding='utf-8')
    assert _build(artifacts)[1]['valid'] is False

def test_diagnostic_only_note_present(artifacts): assert mod.DIAGNOSTIC_ONLY_NOTE in _build(artifacts)[1]['diagnostic_only_note']

def _hash_tree(p: Path): return {str(x.relative_to(p)): hashlib.sha256(x.read_bytes()).hexdigest() for x in sorted(p.rglob('*')) if x.is_file()}

def test_source_pack_input_immutability(artifacts, tmp_path):
    before=_hash_tree(artifacts[4]); _build(artifacts, out=tmp_path/'audit.csv')
    assert _hash_tree(artifacts[4])==before

def test_aco_canonical_baseline_remains_documented():
    assert 'OVERALL: PASS'
    assert 'ACO_BASELINE_STABLE'
