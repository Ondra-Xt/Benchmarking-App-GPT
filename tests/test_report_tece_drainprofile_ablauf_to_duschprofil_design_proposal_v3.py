from __future__ import annotations

import csv, hashlib, json, subprocess, sys
from pathlib import Path
from types import SimpleNamespace

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from tools import report_tece_drainprofile_ablauf_to_duschprofil_design_proposal_v3 as mod
from tools import validate_tece_drainprofile_ablauf_to_duschprofil_evidence_template_v3_csv as validator


def _source_rows(role_counts=None):
    role_counts = role_counts or validator.EXPECTED_MACHINE_ROLE_COUNTS
    roles = [role for role, count in role_counts.items() for _ in range(count)]
    arts = validator.RETAINED_DRAIN_BODY_ARTICLES + validator.INSTALLABLE_DUSCHPROFIL_ARTICLES
    rows = [SimpleNamespace(tece_family_candidate=validator.EXPECTED_FAMILY, product_family=validator.EXPECTED_FAMILY, tece_article_role_candidate=role, article_number=arts[i] if i < len(arts) else f"67{i:04d}") for i, role in enumerate(roles)]
    rows += [SimpleNamespace(tece_family_candidate="Other", product_family="Other", tece_article_role_candidate="unknown", article_number=f"60{i:04d}") for i in range(436 - len(rows))]
    return rows


@pytest.fixture
def fake_source_pack(monkeypatch):
    monkeypatch.setattr(validator, "load_source_pack", lambda _p: SimpleNamespace(rows=_source_rows()))


def _evidence_rows():
    rows=[]
    areas = ["ablauf_to_duschprofil_interface_statement"]*3 + ["duschprofil_installable_profile_scope_statement"] + ["installable_duschprofil_article_scope_and_length_evidence"]*30
    targets = ["retained_drain_body_article"]*3 + ["generic_duschprofil_scope"] + ["installable_duschprofil_article"]*30
    decisions = [validator.ACCEPTED_ABLAUF_TO_DUSCHPROFIL_INTERFACE_DECISION]*3 + [validator.ACCEPTED_GENERIC_DUSCHPROFIL_SCOPE_DECISION] + [validator.ACCEPTED_INSTALLABLE_DUSCHPROFIL_ARTICLE_DECISION]*30
    for i,(article,area,target,decision) in enumerate(zip(validator.EXPECTED_ARTICLE_ORDER, areas, targets, decisions),1):
        row={c:"" for c in validator.REQUIRED_COLUMNS}
        row.update({"template_v3_row_id":f"{validator.ROW_ID_PREFIX}-{i:06d}","family":validator.EXPECTED_FAMILY,"evidence_collection_area":area,"evidence_target_type":target,"article_number":article,"source_document_name":"TECE catalogue","source_document_version":"2024","source_page_or_section":"p.1","source_url_or_path":"https://example.test","source_text_excerpt":"Official evidence","reviewed_evidence_summary":"Reviewed evidence","reviewer_decision":decision,"reviewer_notes":"Diagnostic only","evidence_acceptance_allowed":"true","evidence_complete":"true","ready_for_future_diagnostic_design":"true","diagnostic_only":"true"})
        for f in validator.BLOCK_FALSE_FIELDS: row[f]="false"
        rows.append(row)
    return rows


def _write_csv(path: Path, rows):
    with path.open("w", encoding="utf-8-sig", newline="") as fh:
        w=csv.DictWriter(fh, fieldnames=list(rows[0].keys()), extrasaction="ignore"); w.writeheader(); w.writerows(rows)


def _qa_rows(evidence_rows):
    rows=[]
    for i,r in enumerate(evidence_rows,1):
        area=r["evidence_collection_area"]; target=r["evidence_target_type"]
        q={"qa_row_id":f"TECE-DP-ABLAUF-DUSCHPROFIL-EVIDENCE-V3-QA-{i:06d}","template_v3_row_id":r["template_v3_row_id"],"family":r["family"],"evidence_collection_area":area,"evidence_target_type":target,"article_number":r["article_number"],"qa_status":"pass","qa_blocking_status":"diagnostic_only_generation_blocked","source_evidence_complete":"true","diagnostic_scope_confirmed":"true","future_diagnostic_design_ready":"true","ablauf_to_duschprofil_interface_confirmed":"true" if area != "installable_duschprofil_article_scope_and_length_evidence" else "false","installable_duschprofil_article_scope_confirmed":"true" if target != "retained_drain_body_article" else "false","spare_cover_not_installable_profile_confirmed":"true","diagnostic_only":"true"}
        for f in mod.FALSE_FIELDS: q[f]="false"
        rows.append(q)
    return rows


@pytest.fixture
def artifacts(tmp_path, fake_source_pack):
    evidence=tmp_path/"evidence.csv"; _write_csv(evidence, _evidence_rows())
    qa_csv=tmp_path/"qa.csv"; _write_csv(qa_csv, _qa_rows(_evidence_rows()))
    validation=validator.validate(evidence, "source", validator.EXPECTED_FAMILY); validation_path=tmp_path/"validation.json"; validation_path.write_text(json.dumps(validation), encoding="utf-8")
    qa_report={"valid":True,"production_promotion_blocked":True,"ready_for_benchmark":False,"ready_for_customer_view":False}; qa_report_path=tmp_path/"qa.json"; qa_report_path.write_text(json.dumps(qa_report), encoding="utf-8")
    return SimpleNamespace(evidence=evidence, qa_csv=qa_csv, validation=validation_path, qa_report=qa_report_path, out=tmp_path/"proposal.csv", json_out=tmp_path/"proposal.json")


def _report(a):
    return mod.generate_report(a.qa_report, a.qa_csv, a.validation, a.evidence, "source", validator.EXPECTED_FAMILY)[0]


def test_valid_v3_design_proposal_generation_passes(artifacts):
    report, rows = mod.generate_report(artifacts.qa_report, artifacts.qa_csv, artifacts.validation, artifacts.evidence, "source", validator.EXPECTED_FAMILY)
    assert report["valid"] is True
    assert report["errors"] == []
    assert len(rows) == 9
    assert report["design_proposal_row_count"] == 9
    assert report["retained_drain_body_articles"] == ["673001","673002","673003"]
    assert report["installable_duschprofil_articles"] == validator.INSTALLABLE_DUSCHPROFIL_ARTICLES
    assert report["spare_profile_cover_articles"] == mod.SPARE_PROFILE_COVER_ARTICLES
    assert report["production_promotion_blocked"] is True
    assert report["ready_for_benchmark"] is False
    assert report["ready_for_customer_view"] is False


def test_cli_writes_csv_and_json(artifacts):
    cp = subprocess.run([sys.executable, "tools/report_tece_drainprofile_ablauf_to_duschprofil_design_proposal_v3.py", "--qa-report", str(artifacts.qa_report), "--qa-csv", str(artifacts.qa_csv), "--validation-report", str(artifacts.validation), "--evidence-csv", str(artifacts.evidence), "--source-pack", "source", "--out", str(artifacts.out), "--json-out", str(artifacts.json_out)], cwd=Path(__file__).resolve().parents[1], text=True, capture_output=True)
    assert cp.returncode == 0, cp.stderr
    assert artifacts.out.exists() and artifacts.json_out.exists()
    assert json.loads(artifacts.json_out.read_text())["valid"] is True
    assert len(list(csv.DictReader(artifacts.out.open()))) == 9


@pytest.mark.parametrize("mutate,expected", [
    (lambda a: a.qa_report.write_text(json.dumps({"valid":False}), encoding="utf-8"), "QA report valid must be true"),
    (lambda a: a.validation.write_text(json.dumps({"valid":False}), encoding="utf-8"), "validation report valid must be true"),
    (lambda a: None, "family must be TECEdrainprofile"),
])
def test_report_level_failures(artifacts, mutate, expected):
    mutate(artifacts)
    family = "Wrong" if expected.startswith("family") else validator.EXPECTED_FAMILY
    report = mod.generate_report(artifacts.qa_report, artifacts.qa_csv, artifacts.validation, artifacts.evidence, "source", family)[0]
    assert report["valid"] is False
    assert expected in report["errors"]


def test_source_pack_baseline_mismatch_fails(monkeypatch, artifacts):
    monkeypatch.setattr(validator, "load_source_pack", lambda _p: SimpleNamespace(rows=_source_rows({"accessory":1})))
    report=_report(artifacts)
    assert report["valid"] is False
    assert "source-pack baseline counts differ from expected" in report["errors"]


def test_evidence_and_qa_row_count_mismatch_fail(artifacts):
    _write_csv(artifacts.evidence, _evidence_rows()[:-1])
    _write_csv(artifacts.qa_csv, _qa_rows(_evidence_rows()[:-1]))
    report=_report(artifacts)
    assert report["valid"] is False
    assert "evidence_csv_row_count must be 34" in report["errors"]
    assert "qa_row_count must be 34" in report["errors"]


def test_proposal_row_count_ids_and_note(artifacts):
    report, rows=mod.generate_report(artifacts.qa_report, artifacts.qa_csv, artifacts.validation, artifacts.evidence, "source", validator.EXPECTED_FAMILY)
    assert [r["proposal_row_id"] for r in rows] == mod.EXPECTED_ROW_IDS
    assert len(set(report["design_proposal_row_ids"])) == 9
    assert report["diagnostic_only_note"] == mod.DIAGNOSTIC_ONLY_NOTE
    assert all(r["diagnostic_only"] == "true" for r in rows)


def test_accepted_counters_and_scope_enforced(artifacts):
    report=_report(artifacts)
    assert (report["accepted_ablauf_to_duschprofil_interface_count"], report["accepted_generic_duschprofil_scope_count"], report["accepted_installable_duschprofil_article_count"], report["accepted_v3_evidence_row_count"]) == (3,1,30,34)
    assert report["retained_drain_body_articles"] == mod.RETAINED_DRAIN_BODY_ARTICLES
    assert report["installable_duschprofil_articles"] == mod.INSTALLABLE_DUSCHPROFIL_ARTICLES
    assert not any(a.startswith("675") for a in report["installable_duschprofil_articles"])
    assert report["spare_profile_cover_articles"] == mod.SPARE_PROFILE_COVER_ARTICLES


def test_model_booleans_enforced(artifacts):
    report=_report(artifacts)
    for key in ["corrected_v3_model_proposed","v3_supersedes_v2_model","installable_duschprofil_scope_confirmed","spare_profile_cover_scope_excluded_from_installable_model","direct_pairing_model_rejected","future_diagnostic_design_ready","future_diagnostic_consumer_required"]:
        assert report[key] is True


def test_generation_mutation_promotion_flags_and_counters_blocked(artifacts):
    report=_report(artifacts)
    for f in mod.FALSE_FIELDS:
        assert report[f"{f}_count"] == 0
    for c in mod.GENERATED_COUNTERS:
        assert report[c] == 0
    assert report["production_safe_candidate_count"] == 0


@pytest.mark.parametrize("field", mod.GENERATED_COUNTERS + ["production_safe_candidate_count"])
def test_validation_counter_nonzero_fails(artifacts, field):
    data=json.loads(artifacts.validation.read_text()); data[field]=1; artifacts.validation.write_text(json.dumps(data))
    report=_report(artifacts)
    assert report["valid"] is False
    assert f"{field} must be 0" in report["errors"]


@pytest.mark.parametrize("field,value,msg", [("production_promotion_blocked", False, "production_promotion_blocked must be true"),("ready_for_benchmark", True, "ready_for_benchmark must be false"),("ready_for_customer_view", True, "ready_for_customer_view must be false")])
def test_promotion_readiness_gates_fail(artifacts, field, value, msg):
    data=json.loads(artifacts.validation.read_text()); data[field]=value; artifacts.validation.write_text(json.dumps(data))
    report=_report(artifacts)
    assert report["valid"] is False
    assert msg in report["errors"]


def test_source_pack_and_inputs_immutable(artifacts):
    paths=[artifacts.evidence, artifacts.qa_csv, artifacts.validation, artifacts.qa_report]
    before={p:hashlib.sha256(p.read_bytes()).hexdigest() for p in paths}
    mod.generate_report(artifacts.qa_report, artifacts.qa_csv, artifacts.validation, artifacts.evidence, "source", validator.EXPECTED_FAMILY)
    after={p:hashlib.sha256(p.read_bytes()).hexdigest() for p in paths}
    assert before == after


def test_aco_canonical_baseline_remains_pass():
    text = Path("tools/report_aco_final_baseline.py").read_text(encoding="utf-8")
    assert "STABLE = \"ACO_BASELINE_STABLE\"" in text
    assert "UNSTABLE = \"ACO_BASELINE_UNSTABLE\"" in text
