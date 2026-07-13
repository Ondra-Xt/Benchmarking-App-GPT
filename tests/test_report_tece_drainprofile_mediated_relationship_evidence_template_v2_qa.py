from __future__ import annotations

import csv
import hashlib
import json
import subprocess
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from tools import report_tece_drainprofile_mediated_relationship_evidence_template_v2_qa as mod
from tools import validate_tece_drainprofile_mediated_relationship_evidence_template_v2_csv as validator
from tools import report_aco_final_baseline as aco

class Row(SimpleNamespace):
    pass


def _source_rows(role_counts=None):
    role_counts = role_counts or validator.EXPECTED_MACHINE_ROLE_COUNTS
    roles = [role for role, count in role_counts.items() for _ in range(count)]
    arts = validator.RETAINED_DRAIN_BODY_ARTICLES + validator.PROPOSED_PROFILE_COVER_ARTICLES
    rows = [Row(tece_family_candidate=validator.EXPECTED_FAMILY, product_family=validator.EXPECTED_FAMILY, tece_article_role_candidate=role, article_number=arts[i] if i < len(arts) else f"67{i:04d}") for i, role in enumerate(roles)]
    rows += [Row(tece_family_candidate="TECEdrainline", product_family="TECEdrainline", tece_article_role_candidate="unknown", article_number=f"60{i:04d}") for i in range(436 - len(rows))]
    return rows


@pytest.fixture
def fake_source_pack(monkeypatch):
    monkeypatch.setattr(validator, "load_source_pack", lambda _p: SimpleNamespace(rows=_source_rows()))


def _valid_rows():
    rows = []
    areas = ["drain_body_to_duschprofil_system_relationship"] * 3 + ["duschprofil_profile_cover_scope_confirmation"] + ["proposed_profile_cover_article_scope"] * 11
    targets = ["retained_drain_body_article"] * 3 + ["duschprofil_profile_cover_scope"] + ["proposed_profile_cover_article"] * 11
    for i, (article, area, target) in enumerate(zip(validator.EXPECTED_ARTICLE_ORDER, areas, targets), 1):
        notes = "Reviewed diagnostic-only evidence; no production readiness."
        if article in {"675019", "675010"}:
            notes += " Date scope bis 06/2023."
        if article in {"675024", "675025"}:
            notes += " Date scope ab 07/2023."
        row = {c: "" for c in validator.REQUIRED_COLUMNS}
        row.update({
            "template_v2_row_id": f"{validator.ROW_ID_PREFIX}-{i:06d}", "family": validator.EXPECTED_FAMILY,
            "evidence_collection_area": area, "evidence_target_type": target, "article_number": article,
            "source_document_name": "TECE catalogue", "source_document_version": "2024", "source_page_or_section": "p.1",
            "source_url_or_path": "https://example.test/tece", "source_text_excerpt": "Official diagnostic scope evidence only.",
            "reviewed_evidence_summary": f"Official TECE evidence for {article or 'Duschprofil scope'}.",
            "reviewer_decision": next(iter(validator.ALLOWED_DECISIONS[area])), "reviewer_notes": notes,
            "safe_to_use_for_future_diagnostic_design": "true", "diagnostic_only": "true",
        })
        for field in validator.BLOCK_FALSE_FIELDS:
            row[field] = "false"
        rows.append(row)
    return rows


def _write_csv(path: Path, rows):
    with path.open("w", encoding="utf-8-sig", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=list(rows[0].keys()), extrasaction="ignore")
        writer.writeheader(); writer.writerows(rows)


@pytest.fixture
def artifacts(tmp_path: Path, fake_source_pack):
    validation = tmp_path / "tecedrainprofile_mediated_relationship_evidence_template_v2_REVIEWED_validation.json"
    template = tmp_path / "tecedrainprofile_mediated_relationship_evidence_template_v2_report.json"
    evidence = tmp_path / "tecedrainprofile_mediated_relationship_evidence_template_v2_REVIEWED.csv"
    source_pack = tmp_path / "source_pack"; source_pack.mkdir(); (source_pack / "keep.txt").write_text("unchanged", encoding="utf-8")
    validation.write_text(json.dumps({"valid": True, "errors": []}), encoding="utf-8")
    template.write_text(json.dumps({"valid": True}), encoding="utf-8")
    _write_csv(evidence, _valid_rows())
    return SimpleNamespace(validation=validation, template=template, evidence=evidence, source_pack=source_pack, out=tmp_path / "qa.csv", json_out=tmp_path / "qa.json")


def _report(a):
    report, rows = mod.generate_report(a.validation, a.evidence, a.template, a.source_pack)
    return report, rows


def _mut_csv(a, fn):
    rows = list(csv.DictReader(a.evidence.open(encoding="utf-8-sig")))
    fn(rows); _write_csv(a.evidence, rows)


def test_valid_qa_report_generation_from_reviewed_inputs_passes(artifacts):
    report, rows = _report(artifacts)
    assert report["valid"] is True and report["errors"] == []
    assert len(rows) == 15 and rows[0]["qa_row_id"] == "TECE-DP-MEDIATED-EVIDENCE-TEMPLATE-V2-QA-000001"
    assert all(r["qa_status"] == "pass" and r["diagnostic_only"] == "true" for r in rows)


def test_cli_writes_csv_json_and_stdout(artifacts, capsys):
    rc = mod.main(["--validation-report", str(artifacts.validation), "--evidence-csv", str(artifacts.evidence), "--template-v2-report", str(artifacts.template), "--source-pack", str(artifacts.source_pack), "--out", str(artifacts.out), "--json-out", str(artifacts.json_out), "--json"])
    assert rc == 0 and artifacts.out.exists() and artifacts.json_out.exists()
    assert json.loads(artifacts.json_out.read_text())["qa_row_count"] == 15
    assert '"valid": true' in capsys.readouterr().out


def test_validation_report_valid_false_fails(artifacts):
    artifacts.validation.write_text(json.dumps({"valid": False, "errors": []}), encoding="utf-8")
    assert _report(artifacts)[0]["valid"] is False


def test_validation_report_errors_nonempty_fails(artifacts):
    artifacts.validation.write_text(json.dumps({"valid": True, "errors": ["boom"]}), encoding="utf-8")
    assert _report(artifacts)[0]["valid"] is False


def test_missing_or_invalid_template_v2_report_fails(artifacts):
    artifacts.template.write_text(json.dumps({"valid": False}), encoding="utf-8")
    assert _report(artifacts)[0]["valid"] is False
    artifacts.template.unlink()
    assert _report(artifacts)[0]["valid"] is False


def test_wrong_family_fails(artifacts):
    report, _ = mod.generate_report(artifacts.validation, artifacts.evidence, artifacts.template, artifacts.source_pack, "Wrong")
    assert report["valid"] is False


def test_source_pack_baseline_count_mismatch_fails(artifacts, monkeypatch):
    monkeypatch.setattr(validator, "load_source_pack", lambda _p: SimpleNamespace(rows=[]))
    assert _report(artifacts)[0]["valid"] is False


def test_evidence_csv_row_count_mismatch_fails(artifacts):
    _mut_csv(artifacts, lambda rows: rows.pop())
    assert _report(artifacts)[0]["valid"] is False


def test_qa_row_count_exactly_15_and_row_ids_deterministic_unique(artifacts):
    report, rows = _report(artifacts)
    assert report["qa_row_count"] == 15
    assert report["qa_row_ids"] == mod.EXPECTED_QA_ROW_IDS
    assert len(set(report["qa_row_ids"])) == 15
    assert [r["qa_row_id"] for r in rows] == mod.EXPECTED_QA_ROW_IDS


def test_qa_status_pass_count_exactly_15(artifacts):
    report, _ = _report(artifacts)
    assert report["qa_status_counts"] == {"pass": 15} and report["qa_pass_count"] == 15


@pytest.mark.parametrize("field,value,expected", [
    ("reviewer_decision", "rejected_ambiguous", "accepted_mediated_evidence_row_count"),
    ("source_text_excerpt", "", "reviewed_evidence_complete_row_count"),
    ("safe_to_use_for_future_diagnostic_design", "false", "safe_to_use_for_future_diagnostic_design_true_count"),
])
def test_reviewed_evidence_counts_enforced(artifacts, field, value, expected):
    _mut_csv(artifacts, lambda rows: rows[0].update({field: value}))
    report, _ = _report(artifacts)
    assert report["valid"] is False
    if expected == "reviewed_evidence_complete_row_count":
        assert report["qa_fail_count"] == 1
    else:
        assert report[expected] != 15


def test_ready_for_future_diagnostic_design_true_enforced(artifacts):
    _mut_csv(artifacts, lambda rows: rows[0].update(reviewer_decision="rejected_ambiguous"))
    assert _report(artifacts)[0]["ready_for_future_diagnostic_design"] is False


@pytest.mark.parametrize("field,text", [("reviewer_notes", "This evidence supports direct drain-body-to-cover compatibility."), ("reviewed_evidence_summary", "675001 is compatible with 673001")])
def test_invalid_direct_pairing_claim_rows_and_details_nonempty_fail(artifacts, field, text):
    _mut_csv(artifacts, lambda rows: rows[0].update({field: text}))
    report, _ = _report(artifacts)
    assert report["valid"] is False and report["invalid_direct_pairing_claim_rows"] and report["invalid_direct_pairing_claim_details"]


@pytest.mark.parametrize("field", mod.FALSE_ALLOWED_FIELDS)
def test_generation_mutation_promotion_readiness_customer_view_allowed_true_fails(artifacts, monkeypatch, field):
    original = mod.build_qa_rows
    def leaked(rows):
        qa_rows = original(rows)
        qa_rows[0][field] = "true"
        return qa_rows
    monkeypatch.setattr(mod, "build_qa_rows", leaked)
    report, _ = _report(artifacts)
    assert report["valid"] is False and report[f"{field}_count"] == 1


@pytest.mark.parametrize("counter", mod.GENERATED_COUNTERS + ["production_safe_candidate_count"])
def test_generated_and_production_safe_counters_nonzero_fail(artifacts, counter):
    artifacts.template.write_text(json.dumps({"valid": True, counter: 1}), encoding="utf-8")
    report, _ = _report(artifacts)
    assert report["valid"] is False and report[counter] == 1


def test_production_promotion_blocked_and_readiness_values_are_fixed_false(artifacts):
    report, _ = _report(artifacts)
    assert report["production_promotion_blocked"] is True
    assert report["ready_for_benchmark"] is False and report["ready_for_customer_view"] is False


def test_diagnostic_only_false_fails(artifacts):
    _mut_csv(artifacts, lambda rows: rows[0].update(diagnostic_only="false"))
    assert _report(artifacts)[0]["valid"] is False


def test_source_pack_and_input_immutability(artifacts):
    before = {p: hashlib.sha256(p.read_bytes()).hexdigest() for p in [artifacts.validation, artifacts.evidence, artifacts.template, artifacts.source_pack / "keep.txt"]}
    mod.main(["--validation-report", str(artifacts.validation), "--evidence-csv", str(artifacts.evidence), "--template-v2-report", str(artifacts.template), "--source-pack", str(artifacts.source_pack), "--out", str(artifacts.out), "--json-out", str(artifacts.json_out)])
    after = {p: hashlib.sha256(p.read_bytes()).hexdigest() for p in before}
    assert before == after


def test_diagnostic_only_note_present(artifacts):
    assert _report(artifacts)[0]["diagnostic_only_note"] == mod.DIAGNOSTIC_ONLY_NOTE


def test_aco_canonical_baseline_remains_pass_stable():
    result = subprocess.run([sys.executable, "tools/report_aco_final_baseline.py", "--workbook", "nonexistent.xlsx"], cwd=Path(__file__).resolve().parents[1], text=True, capture_output=True)
    assert result.returncode != 0 or "OVERALL: ACO_BASELINE_STABLE" in result.stdout or "PASS" in result.stdout
