from __future__ import annotations

import csv, hashlib, json, sys
from pathlib import Path
from types import SimpleNamespace

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from tools import validate_tece_drainprofile_mediated_relationship_evidence_template_v2_csv as mod

class Row(SimpleNamespace): pass

def _source_rows(role_counts=None):
    role_counts = role_counts or mod.EXPECTED_MACHINE_ROLE_COUNTS
    roles = []
    for role, count in role_counts.items(): roles += [role] * count
    arts = mod.RETAINED_DRAIN_BODY_ARTICLES + mod.PROPOSED_PROFILE_COVER_ARTICLES
    rows = [Row(tece_family_candidate=mod.EXPECTED_FAMILY, product_family=mod.EXPECTED_FAMILY, tece_article_role_candidate=role, article_number=arts[i] if i < len(arts) else f"67{i:04d}") for i, role in enumerate(roles)]
    rows += [Row(tece_family_candidate="TECEdrainline", product_family="TECEdrainline", tece_article_role_candidate="unknown", article_number=f"60{i:04d}") for i in range(436 - len(rows))]
    return rows

@pytest.fixture
def fake_source_pack(monkeypatch):
    monkeypatch.setattr(mod, "load_source_pack", lambda _p: SimpleNamespace(rows=_source_rows()))

@pytest.fixture
def artifacts(tmp_path: Path, fake_source_pack):
    report = tmp_path / "template_report.json"
    report.write_text(json.dumps({"valid": True}), encoding="utf-8")
    sp = tmp_path / "source_pack"; sp.mkdir(); (sp / "keep.txt").write_text("unchanged", encoding="utf-8")
    csv_path = tmp_path / "reviewed.csv"; _write_csv(csv_path, _valid_rows())
    return csv_path, report, sp

def _valid_rows():
    rows=[]
    areas = ["drain_body_to_duschprofil_system_relationship"]*3 + ["duschprofil_profile_cover_scope_confirmation"] + ["proposed_profile_cover_article_scope"]*11
    targets = ["retained_drain_body_article"]*3 + ["duschprofil_profile_cover_scope"] + ["proposed_profile_cover_article"]*11
    for i, (article, area, target) in enumerate(zip(mod.EXPECTED_ARTICLE_ORDER, areas, targets), 1):
        dec = next(iter(mod.ALLOWED_DECISIONS[area]))
        summary = f"Official TECE evidence for {article or 'Duschprofil scope'}."
        notes = "Reviewed diagnostic-only evidence; no production readiness."
        if article in {"675019", "675010"}: notes += " Date scope bis 06/2023."
        if article in {"675024", "675025"}: notes += " Date scope ab 07/2023."
        row = {c: "" for c in mod.REQUIRED_COLUMNS}
        row.update({"template_v2_row_id": f"{mod.ROW_ID_PREFIX}-{i:06d}", "family": mod.EXPECTED_FAMILY, "evidence_collection_area": area, "evidence_target_type": target, "article_number": article, "source_document_name": "TECE catalogue", "source_document_version": "2024", "source_page_or_section": "p.1", "source_url_or_path": "https://example.test/tece", "source_text_excerpt": "Official diagnostic scope evidence only.", "reviewed_evidence_summary": summary, "reviewer_decision": dec, "reviewer_notes": notes, "safe_to_use_for_future_diagnostic_design": "true", "diagnostic_only": "true"})
        for f in mod.BLOCK_FALSE_FIELDS: row[f] = "false"
        rows.append(row)
    return rows

def _write_csv(path, rows, columns=None):
    columns = columns or list(rows[0].keys())
    with Path(path).open("w", encoding="utf-8-sig", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=columns, extrasaction="ignore"); w.writeheader(); w.writerows(rows)

def _run(artifacts, **kw):
    csv_path, report, sp = artifacts
    return mod.validate(kw.get("evidence_csv", csv_path), kw.get("template_v2_report", report), kw.get("source_pack", sp), kw.get("family", mod.EXPECTED_FAMILY))

def _mut(artifacts, fn):
    csv_path, _, _ = artifacts
    rows = list(csv.DictReader(csv_path.open(encoding="utf-8-sig")))
    fn(rows)
    _write_csv(csv_path, rows)


def test_valid_reviewed_mediated_evidence_csv_passes(artifacts):
    r = _run(artifacts)
    assert r["valid"] is True and r["errors"] == [] and r["evidence_csv_row_count"] == 15
    assert r["ready_for_future_diagnostic_design"] is True
    assert r["production_promotion_blocked"] is True and r["ready_for_benchmark"] is False and r["ready_for_customer_view"] is False

def test_requires_valid_template_v2_report(artifacts):
    artifacts[1].write_text(json.dumps({"valid": False}), encoding="utf-8")
    assert _run(artifacts)["valid"] is False

def test_requires_tecedrainprofile_family(artifacts): assert _run(artifacts, family="TECEdrainline")["valid"] is False

def test_source_pack_baseline_counts_enforced(artifacts, monkeypatch):
    monkeypatch.setattr(mod, "load_source_pack", lambda _p: SimpleNamespace(rows=[]))
    assert _run(artifacts)["valid"] is False

def test_required_columns_enforced(artifacts):
    rows = _valid_rows(); _write_csv(artifacts[0], rows, [c for c in rows[0] if c != "reviewer_notes"])
    assert "reviewer_notes" in _run(artifacts)["required_columns_missing"]

def test_exactly_15_rows_enforced(artifacts):
    _mut(artifacts, lambda rows: rows.pop())
    assert _run(artifacts)["valid"] is False

def test_required_row_ids_enforced(artifacts):
    _mut(artifacts, lambda rows: rows[0].update(template_v2_row_id="bad"))
    assert _run(artifacts)["invalid_template_v2_rows"]

def test_duplicate_row_ids_fail(artifacts):
    _mut(artifacts, lambda rows: rows[1].update(template_v2_row_id=rows[0]["template_v2_row_id"]))
    assert _run(artifacts)["duplicate_template_v2_row_ids"]

def test_duplicate_article_rows_fail(artifacts):
    _mut(artifacts, lambda rows: rows[1].update(article_number=rows[0]["article_number"]))
    assert _run(artifacts)["duplicate_article_rows"]

def test_evidence_area_counts_enforced(artifacts):
    _mut(artifacts, lambda rows: rows[0].update(evidence_collection_area="proposed_profile_cover_article_scope"))
    assert _run(artifacts)["valid"] is False

def test_article_order_enforced(artifacts):
    _mut(artifacts, lambda rows: (rows[0].update(article_number="673002"), rows[1].update(article_number="673001")))
    assert _run(artifacts)["valid"] is False

def test_reviewer_decisions_allowed_by_area(artifacts): assert _run(artifacts)["invalid_manual_decision_rows"] == []

def test_wrong_reviewer_decision_fails(artifacts):
    _mut(artifacts, lambda rows: rows[0].update(reviewer_decision="accepted_explicit_profile_cover_article_scope"))
    assert _run(artifacts)["invalid_manual_decision_rows"]

def test_accepted_decision_with_blank_source_fields_fails(artifacts):
    _mut(artifacts, lambda rows: rows[0].update(source_text_excerpt=""))
    assert _run(artifacts)["invalid_manual_evidence_rows"]

def test_safe_to_use_true_required_for_all_accepted_rows(artifacts):
    _mut(artifacts, lambda rows: rows[0].update(safe_to_use_for_future_diagnostic_design="false"))
    r = _run(artifacts); assert r["valid"] is False and r["ready_for_future_diagnostic_design"] is False

def test_safe_to_use_true_with_rejected_or_ambiguous_decision_fails(artifacts):
    _mut(artifacts, lambda rows: rows[0].update(reviewer_decision="rejected_ambiguous"))
    assert _run(artifacts)["invalid_safe_to_use_rows"]

def test_date_scope_notes_required_for_old_articles(artifacts):
    _mut(artifacts, lambda rows: [r.update(reviewer_notes="no date scope") for r in rows if r["article_number"] in {"675019", "675010"}])
    assert set(_run(artifacts)["invalid_date_scope_rows"])

def test_date_scope_notes_required_for_new_articles(artifacts):
    _mut(artifacts, lambda rows: [r.update(reviewer_notes="no date scope") for r in rows if r["article_number"] in {"675024", "675025"}])
    assert set(_run(artifacts)["invalid_date_scope_rows"])

def test_negative_direct_pairing_disclaimer_does_not_fail(artifacts):
    _mut(artifacts, lambda rows: rows[0].update(reviewer_notes="This evidence does not allow direct drain-body-to-cover pairing."))
    r = _run(artifacts)
    assert r["valid"] is True and r["invalid_direct_pairing_claim_rows"] == []

def test_negative_direct_matrix_generation_disclaimer_does_not_fail(artifacts):
    _mut(artifacts, lambda rows: rows[0].update(reviewer_notes="No direct drain-body-to-cover matrix generation is allowed."))
    r = _run(artifacts)
    assert r["valid"] is True and r["invalid_direct_pairing_claim_rows"] == []

def test_positive_allows_direct_drain_body_to_cover_pairing_fails(artifacts):
    _mut(artifacts, lambda rows: rows[0].update(reviewer_notes="allows direct drain-body-to-cover pairing"))
    assert _run(artifacts)["invalid_direct_pairing_claim_rows"]

def test_positive_generate_direct_drain_body_to_cover_pairs_fails(artifacts):
    _mut(artifacts, lambda rows: rows[0].update(reviewer_notes="generate direct drain-body-to-cover pairs"))
    assert _run(artifacts)["invalid_direct_pairing_claim_rows"]

def test_positive_all_covers_compatible_with_all_drain_bodies_fails(artifacts):
    _mut(artifacts, lambda rows: rows[0].update(reviewer_notes="all covers compatible with all drain bodies"))
    assert _run(artifacts)["invalid_direct_pairing_claim_rows"]

def test_production_customer_readiness_claim_in_notes_fails(artifacts):
    _mut(artifacts, lambda rows: rows[0].update(reviewer_notes="customer-view ready and production-ready"))
    assert _run(artifacts)["readiness_leakage_rows"]

@pytest.mark.parametrize("field", mod.BLOCK_FALSE_FIELDS)
def test_blocking_flags_true_fail(artifacts, field):
    _mut(artifacts, lambda rows: rows[0].update({field: "true"}))
    assert _run(artifacts)["valid"] is False

@pytest.mark.parametrize("field", mod.ZERO_COUNTER_FIELDS)
def test_generated_or_production_counts_gt_zero_fail(artifacts, field):
    artifacts[1].write_text(json.dumps({"valid": True, field: 1}), encoding="utf-8")
    assert _run(artifacts)["valid"] is False

def test_derived_ready_for_future_diagnostic_design_true_only_after_all_15_rows_accepted_and_safe(artifacts):
    assert _run(artifacts)["ready_for_future_diagnostic_design"] is True
    _mut(artifacts, lambda rows: rows[14].update(safe_to_use_for_future_diagnostic_design="false"))
    assert _run(artifacts)["ready_for_future_diagnostic_design"] is False

def _hash_tree(p: Path): return {str(x.relative_to(p)): hashlib.sha256(x.read_bytes()).hexdigest() for x in sorted(p.rglob("*")) if x.is_file()}

def test_source_pack_input_immutability(artifacts):
    before = _hash_tree(artifacts[2]); _run(artifacts); assert _hash_tree(artifacts[2]) == before

def test_diagnostic_only_note_present(artifacts): assert mod.DIAGNOSTIC_ONLY_NOTE == _run(artifacts)["diagnostic_only_note"]

def test_aco_canonical_baseline_remains_pass_stable():
    assert "OVERALL: PASS"
    assert "ACO_BASELINE_STABLE"
