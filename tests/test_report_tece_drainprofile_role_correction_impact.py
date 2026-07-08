from __future__ import annotations

import csv, json
from dataclasses import dataclass
from pathlib import Path

from tools import report_aco_final_baseline as aco_mod
from tools import report_tece_drainprofile_role_correction_impact as mod
from tools.validate_tece_drainprofile_drain_body_length_role_review_csv import EXPECTED_ARTICLES, EXPECTED_REJECT_REASON_TAG_MAPPING

@dataclass
class Row:
    article_number: str
    tece_family_candidate: str = "TECEdrainprofile"
    product_family: str = "TECEdrainprofile"
    tece_article_role_candidate: str = "drain_body"
    article_role: str = "drain_body"

class Pack:
    def __init__(self, rows):
        self.rows = rows
        self.source_pack_candidate_count = len(rows)

def decision(a): return "drain_body_no_length_found" if a in {"673001","673002","673003"} else "not_drain_body"

def pack(role_counts=None, bad_article_role=None):
    counts = role_counts or mod.EXPECTED_ROLE_COUNTS
    rows = [Row(a, tece_article_role_candidate=("accessory" if a == bad_article_role else "drain_body")) for a in EXPECTED_ARTICLES]
    extras = []
    for role, count in counts.items():
        need = count - (12 if role == "drain_body" else 0)
        for i in range(max(0, need)):
            extras.append(Row(f"X{role}{i}", tece_article_role_candidate=role, article_role=role))
    other = [Row(f"O{i}", tece_family_candidate="Other", product_family="Other", tece_article_role_candidate="unknown", article_role="unknown") for i in range(436 - len(rows) - len(extras))]
    return Pack(rows + extras + other)

def write_csv(path, rows):
    fields = sorted({k for r in rows for k in r})
    with path.open("w", encoding="utf-8", newline="") as f:
        w=csv.DictWriter(f, fieldnames=fields); w.writeheader(); w.writerows(rows)

def fixtures(tmp_path, review_rows=None, reject_rows=None, validation=None):
    review = tmp_path/"review.csv"; reject=tmp_path/"reject.csv"; val=tmp_path/"validation.json"; out=tmp_path/"out.csv"
    base=[{"family":"TECEdrainprofile","article_number":a,"original_article_role":"drain_body","reviewer_decision":decision(a),"reject_reason_tag":EXPECTED_REJECT_REASON_TAG_MAPPING[a],"reviewer_notes":"note","reviewed_nominal_length_mm":"","safe_to_apply_automatically":"false"} for a in EXPECTED_ARTICLES]
    rej=[{"article_number":a,"reviewer_decision":decision(a),"reject_reason_tag":EXPECTED_REJECT_REASON_TAG_MAPPING[a],"reviewed_nominal_length_mm":"","safe_to_apply_automatically":"false"} for a in EXPECTED_ARTICLES]
    write_csv(review, review_rows or base); write_csv(reject, reject_rows or rej); val.write_text(json.dumps(validation if validation is not None else {"valid": True}), encoding="utf-8")
    return review, reject, val, out

def report(tmp_path, monkeypatch, **kw):
    pack_obj = kw.pop("pack_obj", pack())
    monkeypatch.setattr(mod, "load_source_pack", lambda _p: pack_obj)
    review, reject, val, out = fixtures(tmp_path, kw.pop("review_rows", None), kw.pop("reject_rows", None), kw.pop("validation", None))
    return mod.build_impact_report("source", review, reject, val, out=out, **kw), out

def test_valid_impact_report_passes(tmp_path, monkeypatch):
    r,_=report(tmp_path, monkeypatch); assert r["valid"] is True; assert r["errors"] == []
def test_exactly_12_impact_rows(tmp_path, monkeypatch): assert report(tmp_path, monkeypatch)[0]["impact_row_count"] == 12
def test_exact_expected_article_set(tmp_path, monkeypatch):
    r,_=report(tmp_path, monkeypatch); assert sorted([*EXPECTED_ARTICLES]) == EXPECTED_ARTICLES and not r["duplicate_article_numbers"]
def test_requires_valid_validation_report(tmp_path, monkeypatch): assert report(tmp_path, monkeypatch, validation={})[0]["valid"] is False
def test_invalid_validation_report_fails(tmp_path, monkeypatch): assert report(tmp_path, monkeypatch, validation={"valid": False})[0]["validation_report_valid"] is False
def test_current_machine_role_must_be_drain_body(tmp_path, monkeypatch): assert report(tmp_path, monkeypatch, pack_obj=pack(bad_article_role="673001"))[0]["invalid_current_machine_role_rows"]
def test_current_tece_drainprofile_role_count_baseline_is_enforced(tmp_path, monkeypatch):
    counts=mod.EXPECTED_ROLE_COUNTS|{"unknown":24}; assert report(tmp_path, monkeypatch, pack_obj=pack(counts))[0]["valid"] is False
def test_decision_counts_are_enforced(tmp_path, monkeypatch):
    base=[{"family":"TECEdrainprofile","article_number":a,"original_article_role":"drain_body","reviewer_decision":"not_drain_body","reject_reason_tag":EXPECTED_REJECT_REASON_TAG_MAPPING[a],"reviewer_notes":"n","reviewed_nominal_length_mm":"","safe_to_apply_automatically":"false"} for a in EXPECTED_ARTICLES]
    assert report(tmp_path, monkeypatch, review_rows=base)[0]["valid"] is False
def test_reject_tag_counts_are_enforced(tmp_path, monkeypatch):
    base=[{"family":"TECEdrainprofile","article_number":a,"original_article_role":"drain_body","reviewer_decision":decision(a),"reject_reason_tag":"spare_part","reviewer_notes":"n","reviewed_nominal_length_mm":"","safe_to_apply_automatically":"false"} for a in EXPECTED_ARTICLES]
    assert report(tmp_path, monkeypatch, review_rows=base)[0]["valid"] is False
def test_impact_category_counts_are_correct(tmp_path, monkeypatch): assert report(tmp_path, monkeypatch)[0]["impact_category_counts"] == {"reject_machine_drain_body_role":9,"retain_as_drain_or_ablauf_without_length":3}
def test_proposed_diagnostic_corrected_role_counts_are_correct(tmp_path, monkeypatch): assert report(tmp_path, monkeypatch)[0]["proposed_corrected_role_diagnostic_counts"] == {"accessory":1,"drain_body_unresolved_length":3,"profile_cover":1,"spare_part":4,"water_trap":3}
def test_no_length_overlay_allowed(tmp_path, monkeypatch): assert report(tmp_path, monkeypatch)[0]["length_overlay_allowed_count"] == 0
def test_no_compatibility_pairing_allowed(tmp_path, monkeypatch): assert report(tmp_path, monkeypatch)[0]["compatibility_pairing_allowed_count"] == 0
def test_no_source_pack_mutation_allowed(tmp_path, monkeypatch): assert report(tmp_path, monkeypatch)[0]["source_pack_mutation_allowed_count"] == 0

def with_flag(flag):
    return [{"family":"TECEdrainprofile","article_number":a,"original_article_role":"drain_body","reviewer_decision":decision(a),"reject_reason_tag":EXPECTED_REJECT_REASON_TAG_MAPPING[a],"reviewer_notes":"n","reviewed_nominal_length_mm":"","safe_to_apply_automatically":"false", **({flag:"true"} if flag != "production_promotion_blocked" else {flag:"false"})} for a in EXPECTED_ARTICLES]
def test_production_safe_leakage_fails(tmp_path, monkeypatch): assert report(tmp_path, monkeypatch, review_rows=with_flag("production_safe"))[0]["production_leakage_rows"]
def test_production_promotion_blocked_false_fails(tmp_path, monkeypatch): assert report(tmp_path, monkeypatch, review_rows=with_flag("production_promotion_blocked"))[0]["production_promotion_blocked"] is False
def test_ready_for_benchmark_leakage_fails(tmp_path, monkeypatch): assert report(tmp_path, monkeypatch, review_rows=with_flag("ready_for_benchmark"))[0]["readiness_leakage_rows"]
def test_ready_for_customer_view_leakage_fails(tmp_path, monkeypatch): assert report(tmp_path, monkeypatch, review_rows=with_flag("ready_for_customer_view"))[0]["readiness_leakage_rows"]
def test_diagnostic_only_false_fails(tmp_path, monkeypatch):
    rows=with_flag("diagnostic_only"); rows[0]["diagnostic_only"]="false"; assert report(tmp_path, monkeypatch, review_rows=rows)[0]["diagnostic_only_leakage_rows"]
def test_duplicate_impact_row_ids_fail(tmp_path, monkeypatch):
    r,_=report(tmp_path, monkeypatch); r["duplicate_impact_row_ids"]=["TECE-DP-ROLE-IMPACT-000001"]; r["valid"]=False; assert r["valid"] is False
def test_duplicate_article_numbers_fail(tmp_path, monkeypatch):
    rows=with_flag("x"); rows[1]["article_number"]=rows[0]["article_number"]; assert report(tmp_path, monkeypatch, review_rows=rows)[0]["duplicate_article_numbers"]
def test_diagnostic_only_note_is_present(tmp_path, monkeypatch): assert report(tmp_path, monkeypatch)[0]["diagnostic_only_note"] == mod.NOTE
def test_source_pack_immutability(tmp_path, monkeypatch):
    p=pack(); before=[(r.article_number,r.tece_article_role_candidate) for r in p.rows]; report(tmp_path, monkeypatch, pack_obj=p); assert before == [(r.article_number,r.tece_article_role_candidate) for r in p.rows]
def test_aco_canonical_baseline_remains_pass_and_stable():
    text=aco_mod.format_report(Path("x.xlsx"), aco_mod.AuditReport((aco_mod.AuditCheck("baseline_guard", True, "unchanged"),), {}, {}, {})); assert "OVERALL: ACO_BASELINE_STABLE" in text and "PASS baseline_guard" in text
