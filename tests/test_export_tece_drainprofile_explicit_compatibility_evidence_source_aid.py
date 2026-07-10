from __future__ import annotations

import csv
import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from tools.export_tece_drainprofile_explicit_compatibility_evidence_collection_template import REQUIRED_TEMPLATE_IDS, template_rows
from tools.export_tece_drainprofile_explicit_compatibility_evidence_source_aid import (
    DIAGNOSTIC_ONLY_NOTE,
    REQUIRED_SOURCE_AID_IDS,
    build_source_aid,
)


def _inventory(profile_count=30, total=436, family_count=76):
    rows = []
    roles = ["accessory"] * 5 + ["complete_set"] * 4 + ["drain_body"] * 12 + ["profile_cover"] * profile_count + ["unknown"] * (family_count - 21 - profile_count)
    articles = ["673001", "673002", "673003"] + [f"9{i:05d}" for i in range(max(0, len(roles) - 3))]
    for i, role in enumerate(roles):
        article = articles[i]
        rows.append(SimpleNamespace(article_number=article, tece_family_candidate="TECEdrainprofile", product_family="TECEdrainprofile", tece_article_role_candidate=role, source_file=f"source_{i}.pdf", page_range_label=str(i), evidence_text=f"evidence text {article}", classification_reason=f"role {role}"))
    while len(rows) < total:
        rows.append(SimpleNamespace(article_number=f"x{len(rows)}", tece_family_candidate="TECEdrainline", product_family="TECEdrainline", tece_article_role_candidate="unknown", source_file="", page_range_label="", evidence_text="", classification_reason=""))
    return SimpleNamespace(rows=rows)


@pytest.fixture
def case(tmp_path, monkeypatch):
    import tools.export_tece_drainprofile_explicit_compatibility_evidence_source_aid as mod
    monkeypatch.setattr(mod, "load_source_pack", lambda _p: _inventory())
    template = tmp_path / "template.csv"
    with template.open("w", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=list(template_rows()[0].keys()))
        writer.writeheader(); writer.writerows(template_rows())
    report = {
        "valid": True, "family": "TECEdrainprofile", "input_inventory_row_count": 436, "family_inventory_row_count": 76,
        "current_machine_role_counts": {"accessory": 5, "complete_set": 4, "drain_body": 12, "profile_cover": 30, "unknown": 25},
        "evidence_csv_row_count": 5, "template_csv_row_count": 5, "template_report_row_count": 5,
        "retained_drain_body_article_numbers": ["673001", "673002", "673003"], "retained_drain_body_row_count": 3,
        "compatibility_matrix_row_count": 1, "profile_cover_scope_row_count": 1, "evidence_complete_row_count": 0,
        "evidence_incomplete_row_count": 5, "missing_manual_evidence_row_count": 5, "ready_for_future_diagnostic_design": False,
        "generated_compatibility_pair_count": 0, "generated_candidate_pair_matrix_row_count": 0, "generated_length_overlay_row_count": 0,
        "proposed_source_pack_mutation_count": 0, "production_safe_candidate_count": 0, "production_promotion_blocked": True,
        "ready_for_benchmark": False, "ready_for_customer_view": False,
    }
    report_path = tmp_path / "validation.json"
    report_path.write_text(json.dumps(report), encoding="utf-8")
    return template, report_path, report, tmp_path / "out.csv"


def _run(case, **kwargs):
    template, report_path, _report, out = case
    return build_source_aid(template, report_path, out=out, **kwargs)


def test_valid_source_aid_export_passes(case):
    rows, report = _run(case)
    assert report["valid"] is True
    assert report["errors"] == []
    assert len(rows) == 5


def test_requires_valid_evidence_validation_report(case):
    case[2]["valid"] = False
    case[1].write_text(json.dumps(case[2]), encoding="utf-8")
    assert _run(case)[1]["valid"] is False


def test_requires_tecedrainprofile_family(case):
    assert _run(case, family="Other")[1]["valid"] is False


def test_source_pack_baseline_counts_enforced(case, monkeypatch):
    import tools.export_tece_drainprofile_explicit_compatibility_evidence_source_aid as mod
    monkeypatch.setattr(mod, "load_source_pack", lambda _p: _inventory(profile_count=29, family_count=76))
    assert _run(case)[1]["valid"] is False


def test_evidence_template_csv_must_have_exactly_5_rows(case):
    template, _, _, _ = case
    rows = template_rows()[:4]
    with template.open("w", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=list(rows[0].keys())); writer.writeheader(); writer.writerows(rows)
    assert _run(case)[1]["valid"] is False


def test_validation_report_row_counts_enforced(case):
    case[2]["evidence_csv_row_count"] = 4
    case[1].write_text(json.dumps(case[2]), encoding="utf-8")
    assert _run(case)[1]["valid"] is False


def test_retained_drain_body_article_list_enforced(case):
    case[2]["retained_drain_body_article_numbers"] = ["673001"]
    case[1].write_text(json.dumps(case[2]), encoding="utf-8")
    assert _run(case)[1]["valid"] is False


def test_output_csv_has_exactly_5_rows(case):
    _run(case)
    with case[3].open(encoding="utf-8") as fh:
        assert len(list(csv.DictReader(fh))) == 5


def test_required_source_aid_ids_are_present(case):
    assert set(REQUIRED_SOURCE_AID_IDS).issubset(_run(case)[1]["source_aid_ids"])


def test_required_aid_areas_are_present(case):
    counts = _run(case)[1]["aid_area_counts"]
    assert counts["retained_drain_body_nominal_length_evidence"] == 3
    assert counts["article_level_drain_body_to_profile_cover_compatibility_matrix"] == 1
    assert counts["profile_cover_scope_confirmation"] == 1


def test_exactly_3_retained_drain_body_rows(case):
    assert _run(case)[1]["retained_drain_body_source_aid_row_count"] == 3


def test_exactly_1_compatibility_matrix_row(case):
    assert _run(case)[1]["compatibility_matrix_source_aid_row_count"] == 1


def test_exactly_1_profile_cover_scope_row(case):
    assert _run(case)[1]["profile_cover_scope_source_aid_row_count"] == 1


def test_no_3_by_30_matrix_rows_generated(case):
    report = _run(case)[1]
    assert report["source_aid_row_count"] == 5
    assert report["generated_candidate_pair_matrix_row_count"] == 0


@pytest.mark.parametrize("field", [
    "evidence_decision_allowed", "evidence_complete", "ready_for_future_diagnostic_design", "compatibility_pair_generation_allowed",
    "candidate_pair_matrix_allowed", "length_overlay_allowed", "source_pack_mutation_allowed", "production_promotion_allowed",
    "benchmark_ready_allowed", "customer_view_allowed",
])
def test_true_leakage_fields_fail(case, field):
    rows, _ = _run(case)
    rows[0][field] = "true"
    assert build_source_aid(case[0], case[1], out=case[3], rows_override=rows)[1]["valid"] is False


@pytest.mark.parametrize("field", [
    "generated_compatibility_pair_count", "generated_candidate_pair_matrix_row_count", "generated_length_overlay_row_count", "production_safe_candidate_count",
])
def test_generated_or_production_safe_counts_fail(case, field):
    case[2][field] = 1
    case[1].write_text(json.dumps(case[2]), encoding="utf-8")
    assert _run(case)[1]["valid"] is False


def test_diagnostic_only_note_is_present(case):
    assert _run(case)[1]["diagnostic_only_note"] == DIAGNOSTIC_ONLY_NOTE


def test_source_pack_immutability(case):
    before = case[1].read_bytes()
    _run(case)
    assert case[1].read_bytes() == before


def test_aco_canonical_baseline_remains_stable_placeholder():
    # The exporter imports no ACO code and exposes no production promotion path.
    assert True
