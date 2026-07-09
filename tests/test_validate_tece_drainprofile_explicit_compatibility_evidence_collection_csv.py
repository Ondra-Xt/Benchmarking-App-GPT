from __future__ import annotations

import csv, hashlib, importlib.util, json
from pathlib import Path
from types import SimpleNamespace

import pytest

from tools import export_tece_drainprofile_explicit_compatibility_evidence_collection_template as tmpl
from tools import validate_tece_drainprofile_explicit_compatibility_evidence_collection_csv as mod

class Row(SimpleNamespace):
    pass

def _write_csv(path: Path, rows: list[dict[str, str]], columns: list[str] | None = None) -> None:
    with path.open("w", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=columns or list(rows[0]), extrasaction="ignore")
        writer.writeheader(); writer.writerows(rows)

def _template_report(**overrides):
    data = {"valid": True, "errors": [], "family": mod.EXPECTED_FAMILY, "input_inventory_row_count": 436, "family_inventory_row_count": 76, "current_machine_role_counts": mod.EXPECTED_MACHINE_ROLE_COUNTS, "requirements_csv_row_count": 5, "requirement_row_count": 5, "template_row_count": 5, "retained_drain_body_article_numbers": ["673001", "673002", "673003"], "retained_drain_body_template_row_count": 3, "compatibility_matrix_template_row_count": 1, "profile_cover_scope_template_row_count": 1, "source_pack_profile_cover_count": 30, "explicit_article_level_compatibility_evidence_count": 0, "pairing_design_allowed_after_collection_count": 0, "compatibility_pair_generation_allowed_count": 0, "candidate_pair_matrix_allowed_count": 0, "length_overlay_allowed_count": 0, "source_pack_mutation_allowed_count": 0, "production_promotion_allowed_count": 0, "benchmark_ready_allowed_count": 0, "customer_view_allowed_count": 0, "generated_compatibility_pair_count": 0, "generated_candidate_pair_matrix_row_count": 0, "generated_length_overlay_row_count": 0, "proposed_source_pack_mutation_count": 0, "production_safe_candidate_count": 0, "production_promotion_blocked": True, "ready_for_benchmark": False, "ready_for_customer_view": False}
    data.update(overrides); return data

@pytest.fixture
def fake_source_pack(monkeypatch):
    def load(_path):
        roles = ["accessory"] * 5 + ["complete_set"] * 4 + ["drain_body"] * 12 + ["profile_cover"] * 30 + ["unknown"] * 25
        rows = [Row(tece_family_candidate=mod.EXPECTED_FAMILY, product_family=mod.EXPECTED_FAMILY, tece_article_role_candidate=role, article_number=f"67{i:04d}") for i, role in enumerate(roles)]
        rows += [Row(tece_family_candidate="TECEdrainline", product_family="TECEdrainline", tece_article_role_candidate="unknown", article_number=f"60{i:04d}") for i in range(436 - len(rows))]
        return SimpleNamespace(rows=rows)
    monkeypatch.setattr(mod, "load_source_pack", load)

@pytest.fixture
def artifacts(tmp_path: Path, fake_source_pack):
    rows = tmpl.template_rows()
    evidence = tmp_path / "evidence.csv"; template = tmp_path / "template.csv"; report = tmp_path / "report.json"
    _write_csv(evidence, rows, tmpl.CSV_COLUMNS); _write_csv(template, rows, tmpl.CSV_COLUMNS); report.write_text(json.dumps(_template_report()), encoding="utf-8")
    source_pack = tmp_path / "source_pack"; source_pack.mkdir(); (source_pack / "keep.txt").write_text("unchanged", encoding="utf-8")
    return evidence, report, template, source_pack

def _validate(artifacts, **kwargs):
    evidence, report, template, source_pack = artifacts
    return mod.validate_evidence_collection_csv(kwargs.pop("evidence", evidence), kwargs.pop("report", report), kwargs.pop("template", template), kwargs.pop("source_pack", source_pack), kwargs.pop("family", mod.EXPECTED_FAMILY))

def _rows(artifacts):
    return list(csv.DictReader(open(artifacts[0], encoding="utf-8")))

def _rewrite_evidence(artifacts, rows, columns=None):
    _write_csv(artifacts[0], rows, columns or tmpl.CSV_COLUMNS)

def _accepted_rows():
    rows = tmpl.template_rows()
    decisions = ["accepted_explicit_nominal_length_evidence", "accepted_length_independence_evidence", "accepted_explicit_nominal_length_evidence", "accepted_explicit_compatibility_matrix", "accepted_profile_cover_scope_confirmation"]
    for row, decision in zip(rows, decisions):
        row.update({"source_document_name": "TECE catalog", "source_page_or_section": "p.1", "source_text_excerpt": "explicit TECE evidence text", "reviewed_evidence_summary": "official explicit article evidence", "reviewer_decision": decision, "reviewer_notes": "reviewed against official source", "safe_to_use_for_future_diagnostic_design": "true"})
    return rows

def test_blank_template_validates_structurally_but_incomplete(artifacts):
    r = _validate(artifacts)
    assert r["valid"] is True and r["errors"] == []
    assert r["evidence_complete_row_count"] == 0 and r["evidence_incomplete_row_count"] == 5
    assert r["missing_manual_evidence_row_count"] == 5 and r["ready_for_future_diagnostic_design"] is False

def test_fully_accepted_ready_but_no_generation_or_promotion(artifacts):
    _rewrite_evidence(artifacts, _accepted_rows())
    r = _validate(artifacts)
    assert r["valid"] is True and r["ready_for_future_diagnostic_design"] is True
    assert r["compatibility_pair_generation_allowed_count"] == 0
    assert r["candidate_pair_matrix_allowed_count"] == 0
    assert r["production_promotion_allowed_count"] == 0 and r["customer_view_allowed_count"] == 0

@pytest.mark.parametrize("field,value", [("family", "TECEdrainline"), ("valid", False), ("input_inventory_row_count", 0)])
def test_requires_family_valid_report_and_baseline(artifacts, field, value):
    artifacts[1].write_text(json.dumps(_template_report(**{field: value})), encoding="utf-8")
    assert _validate(artifacts)["valid"] is False

def test_source_pack_baseline_counts_enforced(artifacts, monkeypatch):
    monkeypatch.setattr(mod, "load_source_pack", lambda _p: SimpleNamespace(rows=[]))
    assert _validate(artifacts)["valid"] is False

def test_evidence_csv_must_have_exactly_5_rows(artifacts):
    _rewrite_evidence(artifacts, _rows(artifacts)[:-1])
    assert _validate(artifacts)["valid"] is False

def test_template_csv_must_have_exactly_5_rows(artifacts):
    _write_csv(artifacts[2], _rows(artifacts)[:-1], tmpl.CSV_COLUMNS)
    assert _validate(artifacts)["valid"] is False

def test_required_template_ids_enforced(artifacts):
    rows = _rows(artifacts); rows[0]["template_row_id"] = "missing"; _rewrite_evidence(artifacts, rows)
    assert _validate(artifacts)["valid"] is False

def test_required_columns_enforced(artifacts):
    rows = _rows(artifacts); cols = [c for c in tmpl.CSV_COLUMNS if c != "reviewer_notes"]; _write_csv(artifacts[0], rows, cols)
    r = _validate(artifacts)
    assert r["valid"] is False and "reviewer_notes" in r["required_columns_missing"]

def test_duplicate_template_row_id_fails(artifacts):
    rows = _rows(artifacts); rows[1]["template_row_id"] = rows[0]["template_row_id"]; _rewrite_evidence(artifacts, rows)
    assert _validate(artifacts)["duplicate_template_row_ids"] and _validate(artifacts)["valid"] is False

def test_duplicate_retained_drain_body_article_row_fails(artifacts):
    rows = _rows(artifacts); rows[1]["article_number"] = rows[0]["article_number"]; _rewrite_evidence(artifacts, rows)
    assert _validate(artifacts)["duplicate_article_template_rows"] and _validate(artifacts)["valid"] is False

def test_row_mapping_mismatch_fails(artifacts):
    rows = _rows(artifacts); rows[0]["article_number"] = "999"; _rewrite_evidence(artifacts, rows)
    assert _validate(artifacts)["invalid_template_rows"]

def test_invalid_reviewer_decision_fails(artifacts):
    rows = _rows(artifacts); rows[0]["reviewer_decision"] = "nope"; _rewrite_evidence(artifacts, rows)
    assert _validate(artifacts)["invalid_manual_decision_rows"]

@pytest.mark.parametrize("decision", ["", "rejected_insufficient_evidence"])
def test_safe_true_with_blank_or_rejected_decision_fails(artifacts, decision):
    rows = _rows(artifacts); rows[0]["safe_to_use_for_future_diagnostic_design"] = "true"; rows[0]["reviewer_decision"] = decision; _rewrite_evidence(artifacts, rows)
    assert _validate(artifacts)["invalid_manual_decision_rows"]

def test_safe_true_with_missing_evidence_fields_fails(artifacts):
    rows = _rows(artifacts); rows[0]["safe_to_use_for_future_diagnostic_design"] = "true"; rows[0]["reviewer_decision"] = "accepted_explicit_nominal_length_evidence"; _rewrite_evidence(artifacts, rows)
    assert _validate(artifacts)["invalid_manual_evidence_rows"]

def test_accepted_incompatible_decision_for_area_fails(artifacts):
    rows = _accepted_rows(); rows[0]["reviewer_decision"] = "accepted_explicit_compatibility_matrix"; _rewrite_evidence(artifacts, rows)
    assert _validate(artifacts)["invalid_manual_decision_rows"]

def test_accepted_evidence_with_rejected_inference_wording_fails(artifacts):
    rows = _accepted_rows(); rows[0]["reviewer_notes"] = "same length inferred"; _rewrite_evidence(artifacts, rows)
    assert _validate(artifacts)["invalid_inference_rows"]

@pytest.mark.parametrize("decision", ["rejected_wrong_family", "evidence_ambiguous"])
def test_rejected_or_ambiguous_evidence_valid_but_incomplete(artifacts, decision):
    rows = _rows(artifacts); rows[0]["reviewer_decision"] = decision; _rewrite_evidence(artifacts, rows)
    r = _validate(artifacts)
    assert r["valid"] is True and r["evidence_rejected_or_ambiguous_row_count"] == 1 and r["ready_for_future_diagnostic_design"] is False

@pytest.mark.parametrize("field", tmpl.BLOCKING_FIELDS)
def test_blocking_flags_true_fail(artifacts, field):
    rows = _rows(artifacts); rows[0][field] = "true"; _rewrite_evidence(artifacts, rows)
    assert _validate(artifacts)["valid"] is False

def test_production_safe_candidate_count_gt_zero_fails(artifacts):
    artifacts[1].write_text(json.dumps(_template_report(production_safe_candidate_count=1)), encoding="utf-8")
    assert _validate(artifacts)["valid"] is False

def test_diagnostic_only_note_is_present(artifacts):
    assert mod.DIAGNOSTIC_ONLY_NOTE == _validate(artifacts)["diagnostic_only_note"]

def test_source_pack_immutability(artifacts):
    before = hashlib.sha256((artifacts[3] / "keep.txt").read_bytes()).hexdigest(); _validate(artifacts); after = hashlib.sha256((artifacts[3] / "keep.txt").read_bytes()).hexdigest()
    assert before == after

def test_aco_canonical_baseline_remains_pass_and_stable():
    from tools import report_aco_final_baseline as aco_mod
    p = Path(__file__).resolve().parent / "test_report_aco_final_baseline.py"
    spec = importlib.util.spec_from_file_location("aco_helpers", p); assert spec and spec.loader
    helpers = importlib.util.module_from_spec(spec); spec.loader.exec_module(helpers)
    report = aco_mod.audit_frames(helpers._canonical_sheets())
    assert all(c.passed for c in report.checks)
    assert report.overall == aco_mod.STABLE
