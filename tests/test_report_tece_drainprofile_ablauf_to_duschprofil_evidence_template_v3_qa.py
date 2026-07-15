from __future__ import annotations

import csv
import hashlib
import json
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from tools import report_aco_final_baseline as aco_mod
from tools import report_tece_drainprofile_ablauf_to_duschprofil_evidence_template_v3_qa as mod
from tools import validate_tece_drainprofile_ablauf_to_duschprofil_evidence_template_v3_csv as validator


class Row(SimpleNamespace):
    pass


def _source_rows(role_counts=None):
    role_counts = role_counts or validator.EXPECTED_MACHINE_ROLE_COUNTS
    roles = []
    for role, count in role_counts.items():
        roles += [role] * count
    articles = validator.RETAINED_DRAIN_BODY_ARTICLES + validator.INSTALLABLE_DUSCHPROFIL_ARTICLES + validator.FORBIDDEN_SPARE_COVER_ARTICLES
    rows = [Row(tece_family_candidate=validator.EXPECTED_FAMILY, product_family=validator.EXPECTED_FAMILY, tece_article_role_candidate=role, article_number=articles[i] if i < len(articles) else f"67{i:04d}") for i, role in enumerate(roles)]
    rows += [Row(tece_family_candidate="TECEdrainline", product_family="TECEdrainline", tece_article_role_candidate="unknown", article_number=f"60{i:04d}") for i in range(436 - len(rows))]
    return rows


@pytest.fixture
def fake_source_pack(monkeypatch):
    monkeypatch.setattr(validator, "load_source_pack", lambda _p: SimpleNamespace(rows=_source_rows()))


def _valid_rows():
    rows = []
    areas = ["ablauf_to_duschprofil_interface_statement"] * 3 + ["duschprofil_installable_profile_scope_statement"] + ["installable_duschprofil_article_scope_and_length_evidence"] * 30
    targets = ["retained_drain_body_article"] * 3 + ["generic_duschprofil_scope"] + ["installable_duschprofil_article"] * 30
    decisions = [validator.ACCEPTED_ABLAUF_TO_DUSCHPROFIL_INTERFACE_DECISION] * 3 + [validator.ACCEPTED_GENERIC_DUSCHPROFIL_SCOPE_DECISION] + [validator.ACCEPTED_INSTALLABLE_DUSCHPROFIL_ARTICLE_DECISION] * 30
    for i, (article, area, target, decision) in enumerate(zip(validator.EXPECTED_ARTICLE_ORDER, areas, targets, decisions), 1):
        row = {c: "" for c in validator.REQUIRED_COLUMNS}
        row.update({"template_v3_row_id": f"{validator.ROW_ID_PREFIX}-{i:06d}", "family": validator.EXPECTED_FAMILY, "evidence_collection_area": area, "evidence_target_type": target, "article_number": article, "source_document_name": "TECE catalogue", "source_document_version": "2024", "source_page_or_section": "p.1", "source_url_or_path": "https://example.test/tece", "source_text_excerpt": "Official TECE evidence.", "reviewed_evidence_summary": "Reviewed official evidence.", "reviewer_decision": decision, "reviewer_notes": "Diagnostic-only.", "evidence_acceptance_allowed": "true", "evidence_complete": "true", "ready_for_future_diagnostic_design": "true", "diagnostic_only": "true"})
        for f in validator.BLOCK_FALSE_FIELDS:
            row[f] = "false"
        rows.append(row)
    return rows


def _write_csv(path, rows):
    with Path(path).open("w", encoding="utf-8-sig", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=list(rows[0].keys()), extrasaction="ignore"); w.writeheader(); w.writerows(rows)


@pytest.fixture
def artifacts(tmp_path: Path, fake_source_pack):
    sp = tmp_path / "source_pack"; sp.mkdir(); (sp / "keep.txt").write_text("unchanged", encoding="utf-8")
    csv_path = tmp_path / "tecedrainprofile_ablauf_to_duschprofil_evidence_template_v3.csv"; _write_csv(csv_path, _valid_rows())
    validation_path = tmp_path / "tecedrainprofile_ablauf_to_duschprofil_evidence_template_v3_validation.json"
    validation_path.write_text(json.dumps({"valid": True, "family": validator.EXPECTED_FAMILY, "production_safe_candidate_count": 0, "production_promotion_blocked": True, "ready_for_benchmark": False, "ready_for_customer_view": False, **{c: 0 for c in mod.GENERATED_COUNTERS}}), encoding="utf-8")
    return validation_path, csv_path, sp


def _run(artifacts, **kw):
    validation_path, csv_path, sp = artifacts
    return mod.generate_report(kw.get("validation_report", validation_path), kw.get("evidence_csv", csv_path), kw.get("source_pack", sp), kw.get("family", validator.EXPECTED_FAMILY))


def _mut_csv(artifacts, fn):
    rows = list(csv.DictReader(artifacts[1].open(encoding="utf-8-sig"))); fn(rows); _write_csv(artifacts[1], rows)


def _mut_validation(artifacts, **updates):
    data = json.loads(artifacts[0].read_text(encoding="utf-8")); data.update(updates); artifacts[0].write_text(json.dumps(data), encoding="utf-8")


def test_valid_qa_report_generation_passes(artifacts):
    r, rows = _run(artifacts)
    assert r["valid"] is True and r["errors"] == []
    assert r["qa_row_count"] == 34 and len(rows) == 34
    assert r["validation_report_valid"] is True
    assert r["qa_status_counts"] == {"pass": 34}
    assert r["qa_blocking_status_counts"] == {"diagnostic_only_generation_blocked": 34}
    assert r["accepted_ablauf_to_duschprofil_interface_count"] == 3
    assert r["accepted_generic_duschprofil_scope_count"] == 1
    assert r["accepted_installable_duschprofil_article_count"] == 30
    assert r["ablauf_to_duschprofil_interface_confirmed_count"] == 4
    assert r["installable_duschprofil_article_scope_confirmed_count"] == 31


def test_cli_writes_csv_and_json(artifacts, tmp_path, capsys):
    out = tmp_path / "qa.csv"; jout = tmp_path / "qa.json"
    assert mod.main(["--validation-report", str(artifacts[0]), "--evidence-csv", str(artifacts[1]), "--source-pack", str(artifacts[2]), "--out", str(out), "--json-out", str(jout), "--json"]) == 0
    assert len(list(csv.DictReader(out.open(encoding="utf-8")))) == 34
    assert json.loads(jout.read_text(encoding="utf-8"))["valid"] is True
    assert json.loads(capsys.readouterr().out)["valid"] is True


def test_validation_report_valid_false_fails(artifacts):
    _mut_validation(artifacts, valid=False)
    assert _run(artifacts)[0]["valid"] is False


def test_wrong_family_fails(artifacts):
    assert _run(artifacts, family="TECEdrainline")[0]["valid"] is False


def test_source_pack_baseline_mismatch_fails(artifacts, monkeypatch):
    monkeypatch.setattr(validator, "load_source_pack", lambda _p: SimpleNamespace(rows=[]))
    assert _run(artifacts)[0]["valid"] is False


def test_evidence_csv_row_count_mismatch_fails(artifacts):
    _mut_csv(artifacts, lambda rows: rows.pop())
    assert _run(artifacts)[0]["valid"] is False


def test_qa_row_count_exactly_34(artifacts):
    assert _run(artifacts)[0]["qa_row_count"] == 34


def test_qa_row_ids_deterministic_and_unique(artifacts):
    r, _ = _run(artifacts)
    assert r["qa_row_ids"] == mod.EXPECTED_QA_ROW_IDS
    assert len(set(r["qa_row_ids"])) == 34


def test_accepted_counters_3_1_30_enforced(artifacts):
    _mut_csv(artifacts, lambda rows: rows[0].update(reviewer_decision=validator.ACCEPTED_INSTALLABLE_DUSCHPROFIL_ARTICLE_DECISION))
    assert _run(artifacts)[0]["valid"] is False


def test_retained_drain_bodies_exact_list_enforced(artifacts, monkeypatch):
    orig = validator.validate
    def bad(*args, **kwargs):
        report = orig(*args, **kwargs); report["retained_drain_body_articles"] = ["673001"]; return report
    monkeypatch.setattr(validator, "validate", bad)
    assert _run(artifacts)[0]["valid"] is False


def test_installable_duschprofil_article_list_exact(artifacts):
    assert _run(artifacts)[0]["installable_duschprofil_articles"] == validator.INSTALLABLE_DUSCHPROFIL_ARTICLES


def test_675xxx_forbidden_as_installable_duschprofil(artifacts):
    _mut_csv(artifacts, lambda rows: rows[4].update(article_number="675000"))
    r, _ = _run(artifacts)
    assert r["valid"] is False and "675000" in r["forbidden_spare_cover_articles_in_installable_scope"]


@pytest.mark.parametrize("key,expected", [("qa_status_counts", {"pass": 34}), ("qa_blocking_status_counts", {"diagnostic_only_generation_blocked": 34})])
def test_qa_status_counts(artifacts, key, expected):
    assert _run(artifacts)[0][key] == expected


@pytest.mark.parametrize("key,expected", [("source_evidence_complete_count", 34), ("diagnostic_scope_confirmed_count", 34), ("future_diagnostic_design_ready_count", 34), ("ablauf_to_duschprofil_interface_confirmed_count", 4), ("installable_duschprofil_article_scope_confirmed_count", 31), ("spare_cover_not_installable_profile_confirmed_count", 34)])
def test_required_true_counts(artifacts, key, expected):
    assert _run(artifacts)[0][key] == expected


@pytest.mark.parametrize("field", mod.BLOCK_FALSE_FIELDS)
def test_allowed_flags_fail(artifacts, monkeypatch, field):
    orig = mod.build_qa_rows
    def bad(rows):
        qa = orig(rows); qa[0][field] = "true"; return qa
    monkeypatch.setattr(mod, "build_qa_rows", bad)
    assert _run(artifacts)[0]["valid"] is False


@pytest.mark.parametrize("counter", mod.GENERATED_COUNTERS + ["production_safe_candidate_count"])
def test_generated_counters_nonzero_fail(artifacts, counter):
    _mut_validation(artifacts, **{counter: 1})
    assert _run(artifacts)[0]["valid"] is False


@pytest.mark.parametrize("field,value", [("production_promotion_blocked", False), ("ready_for_benchmark", True), ("ready_for_customer_view", True)])
def test_production_and_readiness_flags_fail(artifacts, field, value):
    _mut_validation(artifacts, **{field: value})
    assert _run(artifacts)[0]["valid"] is False


def test_diagnostic_only_false_fails(artifacts, monkeypatch):
    orig = mod.build_qa_rows
    def bad(rows):
        qa = orig(rows); qa[0]["diagnostic_only"] = "false"; return qa
    monkeypatch.setattr(mod, "build_qa_rows", bad)
    assert _run(artifacts)[0]["valid"] is False


def _hash_tree(p: Path):
    return {str(x.relative_to(p)): hashlib.sha256(x.read_bytes()).hexdigest() for x in sorted(p.rglob("*")) if x.is_file()}


def test_source_pack_input_immutability(artifacts):
    before_sp = _hash_tree(artifacts[2]); before_csv = hashlib.sha256(artifacts[1].read_bytes()).hexdigest(); before_json = hashlib.sha256(artifacts[0].read_bytes()).hexdigest()
    _run(artifacts)
    assert _hash_tree(artifacts[2]) == before_sp
    assert hashlib.sha256(artifacts[1].read_bytes()).hexdigest() == before_csv
    assert hashlib.sha256(artifacts[0].read_bytes()).hexdigest() == before_json


def test_diagnostic_only_note_present(artifacts):
    assert _run(artifacts)[0]["diagnostic_only_note"] == mod.DIAGNOSTIC_ONLY_NOTE


def test_aco_canonical_baseline_remains_pass_stable():
    text = aco_mod.format_report(Path("canonical.xlsx"), aco_mod.AuditReport(checks=(), sheet_counts={}, family_counts={}, customer_counts={}))
    assert "OVERALL: ACO_BASELINE_STABLE" in text
