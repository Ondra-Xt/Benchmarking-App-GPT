from __future__ import annotations

import csv
import hashlib
import importlib.util
import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from tools import export_tece_drainprofile_explicit_compatibility_evidence_collection_template as mod


class Row(SimpleNamespace):
    pass


def _write_csv(path: Path, rows: list[dict[str, str]]) -> None:
    with path.open("w", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=list(rows[0]))
        writer.writeheader(); writer.writerows(rows)


def _requirements_csv_rows() -> list[dict[str, str]]:
    return [{"requirement_id": rid, "family": mod.EXPECTED_FAMILY, "requirement_area": f"area_{i}"} for i, rid in enumerate(mod.REQUIRED_REQUIREMENT_IDS, 1)]


def _requirements_report(**overrides):
    data = {
        "valid": True, "errors": [], "family": mod.EXPECTED_FAMILY,
        "input_inventory_row_count": 436, "family_inventory_row_count": 76,
        "current_machine_role_counts": mod.EXPECTED_MACHINE_ROLE_COUNTS,
        "requirement_row_count": 5,
        "retained_drain_body_article_numbers": ["673001", "673002", "673003"],
        "retained_drain_body_article_count": 3, "source_pack_profile_cover_count": 30,
        "reviewed_effective_drain_body_count": 3, "reviewed_effective_accessory_count": 8, "reviewed_effective_profile_cover_count": 1,
        "retained_unresolved_length_count": 3, "diagnostic_reclassified_from_unknown_count": 9,
        "explicit_article_level_compatibility_evidence_count": 0, "required_evidence_area_count": 5,
        "pairing_design_allowed_after_collection_count": 0, "compatibility_pair_generation_allowed_count": 0,
        "candidate_pair_matrix_allowed_count": 0, "length_overlay_allowed_count": 0, "source_pack_mutation_allowed_count": 0,
        "production_promotion_allowed_count": 0, "benchmark_ready_allowed_count": 0, "customer_view_allowed_count": 0,
        "generated_compatibility_pair_count": 0, "generated_candidate_pair_matrix_row_count": 0, "generated_length_overlay_row_count": 0,
        "proposed_source_pack_mutation_count": 0, "production_safe_candidate_count": 0,
        "production_promotion_blocked": True, "ready_for_benchmark": False, "ready_for_customer_view": False,
    }
    data.update(overrides)
    return data


@pytest.fixture
def fake_source_pack(monkeypatch):
    def load(_path):
        rows = []
        roles = ["accessory"] * 5 + ["complete_set"] * 4 + ["drain_body"] * 12 + ["profile_cover"] * 30 + ["unknown"] * 25
        for i, role in enumerate(roles):
            rows.append(Row(tece_family_candidate=mod.EXPECTED_FAMILY, product_family=mod.EXPECTED_FAMILY, tece_article_role_candidate=role, article_number=f"67{i:04d}"))
        for i in range(436 - len(rows)):
            rows.append(Row(tece_family_candidate="TECEdrainline", product_family="TECEdrainline", tece_article_role_candidate="unknown", article_number=f"60{i:04d}"))
        return SimpleNamespace(rows=rows)
    monkeypatch.setattr(mod, "load_source_pack", load)


@pytest.fixture
def artifacts(tmp_path: Path, fake_source_pack):
    req_csv = tmp_path / "requirements.csv"; _write_csv(req_csv, _requirements_csv_rows())
    req_json = tmp_path / "requirements.json"; req_json.write_text(json.dumps(_requirements_report()), encoding="utf-8")
    source_pack = tmp_path / "source_pack"; source_pack.mkdir(); (source_pack / "keep.txt").write_text("unchanged", encoding="utf-8")
    return req_json, req_csv, source_pack


def _build(artifacts, **kwargs):
    req_json, req_csv, source_pack = artifacts
    return mod.build_evidence_collection_template(req_json, req_csv, source_pack=source_pack, out=kwargs.pop("out", None), **kwargs)


def test_valid_evidence_collection_template_passes(artifacts, tmp_path):
    rows, report = _build(artifacts, out=tmp_path / "template.csv")
    assert report["valid"] is True
    assert report["errors"] == []
    assert len(rows) == 5


def test_requires_valid_requirements_report(artifacts):
    artifacts[0].write_text(json.dumps(_requirements_report(valid=False)), encoding="utf-8")
    assert _build(artifacts)[1]["valid"] is False


def test_requires_tecedrainprofile_family(artifacts):
    artifacts[0].write_text(json.dumps(_requirements_report(family="TECEdrainline")), encoding="utf-8")
    assert _build(artifacts)[1]["valid"] is False
    assert _build(artifacts, family="TECEdrainline")[1]["valid"] is False


def test_source_pack_baseline_counts_are_enforced(artifacts, monkeypatch):
    monkeypatch.setattr(mod, "load_source_pack", lambda _p: SimpleNamespace(rows=[]))
    assert _build(artifacts)[1]["valid"] is False


def test_requirements_csv_must_have_5_rows(artifacts):
    _write_csv(artifacts[1], _requirements_csv_rows()[:-1])
    assert _build(artifacts)[1]["valid"] is False


def test_requirements_report_requirement_row_count_is_enforced(artifacts):
    artifacts[0].write_text(json.dumps(_requirements_report(requirement_row_count=4)), encoding="utf-8")
    assert _build(artifacts)[1]["valid"] is False


def test_retained_drain_body_article_list_is_enforced(artifacts):
    artifacts[0].write_text(json.dumps(_requirements_report(retained_drain_body_article_numbers=["673001"])), encoding="utf-8")
    assert _build(artifacts)[1]["valid"] is False


def test_output_csv_has_exactly_5_rows(artifacts, tmp_path):
    out = tmp_path / "template.csv"; _build(artifacts, out=out)
    assert len(list(csv.DictReader(out.open(encoding="utf-8-sig")))) == 5


def test_required_template_row_ids_are_present(artifacts):
    assert _build(artifacts)[1]["template_row_ids"] == mod.REQUIRED_TEMPLATE_IDS


def test_required_evidence_collection_areas_are_present(artifacts):
    report = _build(artifacts)[1]
    assert report["evidence_collection_area_counts"] == {
        "article_level_drain_body_to_profile_cover_compatibility_matrix": 1,
        "profile_cover_scope_confirmation": 1,
        "retained_drain_body_nominal_length_evidence": 3,
    }


def test_exactly_3_retained_drain_body_rows_are_generated(artifacts):
    assert _build(artifacts)[1]["retained_drain_body_template_row_count"] == 3


def test_exactly_1_compatibility_matrix_template_row_is_generated(artifacts):
    assert _build(artifacts)[1]["compatibility_matrix_template_row_count"] == 1


def test_exactly_1_profile_cover_scope_row_is_generated(artifacts):
    assert _build(artifacts)[1]["profile_cover_scope_template_row_count"] == 1


def test_manual_evidence_fields_are_blank(artifacts):
    rows, report = _build(artifacts)
    assert report["manual_evidence_fields_prefilled_rows"] == []
    assert all(not r.get(field) for r in rows for field in mod.MANUAL_EVIDENCE_FIELDS)


def test_prefilled_manual_evidence_fields_fail(artifacts):
    rows, _ = _build(artifacts); rows[0]["source_document_name"] = "catalog"
    assert _build(artifacts, rows=rows)[1]["valid"] is False


def test_duplicate_template_row_ids_fail(artifacts):
    rows, _ = _build(artifacts); rows[1]["template_row_id"] = rows[0]["template_row_id"]
    assert _build(artifacts, rows=rows)[1]["valid"] is False


def test_duplicate_retained_drain_body_article_rows_fail(artifacts):
    rows, _ = _build(artifacts); rows[1]["article_number"] = rows[0]["article_number"]
    assert _build(artifacts, rows=rows)[1]["valid"] is False


@pytest.mark.parametrize("field", mod.BLOCKING_FIELDS)
def test_blocking_allowed_true_fails(artifacts, field):
    rows, _ = _build(artifacts); rows[0][field] = "true"
    assert _build(artifacts, rows=rows)[1]["valid"] is False


@pytest.mark.parametrize("field", [
    "generated_compatibility_pair_count", "generated_candidate_pair_matrix_row_count",
    "generated_length_overlay_row_count", "production_safe_candidate_count",
])
def test_report_generation_or_promotion_count_gt_zero_fails(artifacts, field):
    artifacts[0].write_text(json.dumps(_requirements_report(**{field: 1})), encoding="utf-8")
    assert _build(artifacts)[1]["valid"] is False


def test_diagnostic_only_note_is_present(artifacts):
    assert mod.DIAGNOSTIC_ONLY_NOTE in _build(artifacts)[1]["diagnostic_only_note"]


def test_source_pack_immutability(artifacts):
    before = hashlib.sha256((artifacts[2] / "keep.txt").read_bytes()).hexdigest()
    _build(artifacts)
    after = hashlib.sha256((artifacts[2] / "keep.txt").read_bytes()).hexdigest()
    assert before == after


def test_aco_canonical_baseline_remains_pass_and_stable():
    from tools import report_aco_final_baseline as aco_mod

    baseline_test_path = Path(__file__).resolve().parent / "test_report_aco_final_baseline.py"
    spec = importlib.util.spec_from_file_location("aco_final_baseline_test_helpers", baseline_test_path)
    assert spec is not None and spec.loader is not None
    baseline_test_helpers = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(baseline_test_helpers)

    report = aco_mod.audit_frames(baseline_test_helpers._canonical_sheets())

    assert all(check.passed for check in report.checks)
    assert report.overall == aco_mod.STABLE
