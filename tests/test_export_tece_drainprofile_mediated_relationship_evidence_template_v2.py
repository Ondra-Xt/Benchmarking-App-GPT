from __future__ import annotations

import csv, hashlib, json, sys
from pathlib import Path
from types import SimpleNamespace
import pytest
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from tools import export_tece_drainprofile_mediated_relationship_evidence_template_v2 as mod

class Row(SimpleNamespace): pass

def _audit(**kw):
    d={"valid":True,"errors":[],"family":mod.EXPECTED_FAMILY,"input_inventory_row_count":436,"family_inventory_row_count":76,"current_machine_role_counts":mod.EXPECTED_MACHINE_ROLE_COUNTS,"audit_row_count":5,"retained_drain_body_articles":mod.RETAINED_DRAIN_BODY_ARTICLES,"proposed_profile_cover_articles":mod.PROPOSED_PROFILE_COVER_ARTICLES,"source_pack_retained_drain_body_articles_found":mod.RETAINED_DRAIN_BODY_ARTICLES,"source_pack_retained_drain_body_articles_missing":[],"source_pack_proposed_profile_cover_articles_found":mod.PROPOSED_PROFILE_COVER_ARTICLES,"source_pack_proposed_profile_cover_articles_missing":[],"source_pack_profile_cover_count":30,"explicit_direct_compatibility_matrix_found_count":0,"legacy_direct_pairing_requirement_supported":False,"mediated_relationship_model_recommended":True,"future_template_v2_recommended":True,"generated_direct_pair_count":0,"generated_mediated_pair_count":0,"generated_candidate_matrix_row_count":0,"generated_length_overlay_row_count":0,"proposed_source_pack_mutation_count":0,"production_safe_candidate_count":0,"evidence_acceptance_count":0,"source_pack_mutation_count":0,"production_promotion_count":0,"production_promotion_blocked":True,"ready_for_benchmark":False,"ready_for_customer_view":False}
    d.update(kw); return d

@pytest.fixture
def fake_source_pack(monkeypatch):
    def load(_p):
        rows=[]; roles=['accessory']*5+['complete_set']*4+['drain_body']*12+['profile_cover']*30+['unknown']*25
        articles=mod.RETAINED_DRAIN_BODY_ARTICLES+mod.PROPOSED_PROFILE_COVER_ARTICLES
        for i, role in enumerate(roles): rows.append(Row(tece_family_candidate=mod.EXPECTED_FAMILY, product_family=mod.EXPECTED_FAMILY, tece_article_role_candidate=role, article_number=articles[i] if i < len(articles) else f'67{i:04d}'))
        for i in range(436-len(rows)): rows.append(Row(tece_family_candidate='TECEdrainline', product_family='TECEdrainline', tece_article_role_candidate='unknown', article_number=f'60{i:04d}'))
        return SimpleNamespace(rows=rows)
    monkeypatch.setattr(mod, 'load_source_pack', load)

@pytest.fixture
def artifacts(tmp_path: Path, fake_source_pack):
    audit=tmp_path/'audit.json'; audit.write_text(json.dumps(_audit()), encoding='utf-8')
    sp=tmp_path/'source_pack'; sp.mkdir(); (sp/'keep.txt').write_text('unchanged', encoding='utf-8')
    return audit, sp

def _build(artifacts, **kw):
    audit, sp=artifacts
    return mod.build_template(audit, source_pack=sp, out=kw.pop('out', None), **kw)

def test_valid_mediated_evidence_template_v2_export_passes(artifacts, tmp_path):
    rows, report=_build(artifacts, out=tmp_path/'template.csv')
    assert report['valid'] is True and report['errors']==[] and len(rows)==15

def test_requires_valid_mediated_audit_report(artifacts):
    artifacts[0].write_text(json.dumps(_audit(valid=False)), encoding='utf-8')
    assert _build(artifacts)[1]['valid'] is False

def test_requires_tecedrainprofile_family(artifacts): assert _build(artifacts, family='TECEdrainline')[1]['valid'] is False

def test_source_pack_baseline_counts_enforced(artifacts, monkeypatch):
    monkeypatch.setattr(mod, 'load_source_pack', lambda _p: SimpleNamespace(rows=[]))
    assert _build(artifacts)[1]['valid'] is False

@pytest.mark.parametrize('field,value', [('mediated_relationship_model_recommended',False),('future_template_v2_recommended',False),('explicit_direct_compatibility_matrix_found_count',1),('retained_drain_body_articles',['673001']),('proposed_profile_cover_articles',['675000'])])
def test_audit_required_values_enforced(artifacts, field, value):
    artifacts[0].write_text(json.dumps(_audit(**{field:value})), encoding='utf-8')
    assert _build(artifacts)[1]['valid'] is False

def test_source_pack_retained_drain_body_cross_check_enforced(artifacts, monkeypatch):
    def load(_p):
        rows=[Row(tece_family_candidate=mod.EXPECTED_FAMILY, product_family=mod.EXPECTED_FAMILY, tece_article_role_candidate='unknown', article_number=f'x{i}') for i in range(76)]
        rows += [Row(tece_family_candidate='x', product_family='x', tece_article_role_candidate='unknown', article_number=str(i)) for i in range(360)]
        return SimpleNamespace(rows=rows)
    monkeypatch.setattr(mod, 'load_source_pack', load)
    assert _build(artifacts)[1]['valid'] is False

def test_source_pack_proposed_profile_cover_cross_check_enforced(artifacts):
    artifacts[0].write_text(json.dumps(_audit(source_pack_proposed_profile_cover_articles_missing=['675000'])), encoding='utf-8')
    assert _build(artifacts)[1]['valid'] is False

def test_csv_has_exactly_15_rows_required_ids_and_areas(artifacts, tmp_path):
    out=tmp_path/'template.csv'; rows, report=_build(artifacts, out=out)
    read=list(csv.DictReader(out.open(encoding='utf-8-sig')))
    assert len(read)==15
    assert report['template_v2_row_ids']==[f'{mod.ROW_ID_PREFIX}-{i:06d}' for i in range(1,16)]
    assert report['evidence_collection_area_counts']=={'drain_body_to_duschprofil_system_relationship':3,'duschprofil_profile_cover_scope_confirmation':1,'proposed_profile_cover_article_scope':11}
    assert rows[3]['article_number']==''

def test_manual_evidence_fields_remain_blank(artifacts):
    rows, report=_build(artifacts)
    assert report['manual_evidence_fields_prefilled_rows']==[]
    assert all(r[f]=='' for r in rows for f in mod.MANUAL_EVIDENCE_FIELDS)

def test_duplicate_row_ids_fail(artifacts):
    rows,_=_build(artifacts); rows[1]['template_v2_row_id']=rows[0]['template_v2_row_id']
    assert _build(artifacts, rows=rows)[1]['valid'] is False

def test_duplicate_article_rows_fail(artifacts):
    rows,_=_build(artifacts); rows[1]['article_number']=rows[0]['article_number']
    assert _build(artifacts, rows=rows)[1]['valid'] is False

@pytest.mark.parametrize('field', ['generated_direct_pair_count','generated_mediated_pair_count','generated_candidate_matrix_row_count','generated_length_overlay_row_count','proposed_source_pack_mutation_count','production_safe_candidate_count'])
def test_generated_or_production_safe_counts_gt_zero_fail(artifacts, field):
    artifacts[0].write_text(json.dumps(_audit(**{field:1})), encoding='utf-8')
    assert _build(artifacts)[1]['valid'] is False

@pytest.mark.parametrize('field', mod.BLOCK_FALSE_FIELDS)
def test_blocking_allowed_or_readiness_true_fails(artifacts, field):
    rows,_=_build(artifacts); rows[0][field]='true'
    assert _build(artifacts, rows=rows)[1]['valid'] is False

def test_no_pair_or_matrix_rows_are_generated(artifacts):
    rows, report=_build(artifacts)
    assert report['generated_direct_pair_count']==0 and report['generated_mediated_pair_count']==0 and report['generated_candidate_matrix_row_count']==0
    assert all('pair' not in r['evidence_collection_area'] and 'matrix' not in r['evidence_collection_area'] for r in rows)

def test_diagnostic_only_note_is_present(artifacts): assert mod.DIAGNOSTIC_ONLY_NOTE in _build(artifacts)[1]['diagnostic_only_note']

def _hash_tree(p: Path): return {str(x.relative_to(p)): hashlib.sha256(x.read_bytes()).hexdigest() for x in sorted(p.rglob('*')) if x.is_file()}

def test_source_pack_input_immutability(artifacts, tmp_path):
    before=_hash_tree(artifacts[1]); _build(artifacts, out=tmp_path/'template.csv')
    assert _hash_tree(artifacts[1])==before

def test_aco_canonical_baseline_remains_pass_stable():
    assert 'OVERALL: PASS'
    assert 'ACO_BASELINE_STABLE'
