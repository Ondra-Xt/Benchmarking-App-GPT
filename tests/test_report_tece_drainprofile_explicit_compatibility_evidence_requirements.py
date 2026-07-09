from __future__ import annotations

import csv
import hashlib
import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from tools import report_tece_drainprofile_explicit_compatibility_evidence_requirements as mod


class Row(SimpleNamespace):
    pass


def _write_csv(path: Path, rows: list[dict[str, str]]) -> None:
    with path.open("w", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=list(rows[0]))
        writer.writeheader(); writer.writerows(rows)


def _readiness_csv_rows() -> list[dict[str, str]]:
    return [{"audit_row_id": f"AUD-{i}", "family": mod.EXPECTED_FAMILY, "audit_area": area} for i, area in enumerate(mod.EXPECTED_READINESS_AUDIT_AREAS, 1)]


def _readiness_report(**overrides):
    data = {
        "valid": True, "errors": [], "family": mod.EXPECTED_FAMILY,
        "input_inventory_row_count": 436, "family_inventory_row_count": 76,
        "current_machine_role_counts": mod.EXPECTED_MACHINE_ROLE_COUNTS,
        "audit_row_count": 8, "role_overlay_chain_valid": True, "source_pack_baseline_valid": True,
        "reviewed_effective_drain_body_count": 3, "reviewed_effective_accessory_count": 8, "reviewed_effective_profile_cover_count": 1,
        "retained_unresolved_length_count": 3, "diagnostic_reclassified_from_unknown_count": 9,
        "source_pack_profile_cover_count": 30, "explicit_article_level_compatibility_evidence_count": 0,
        "compatibility_pair_generation_allowed_count": 0, "length_overlay_allowed_count": 0, "source_pack_mutation_allowed_count": 0,
        "production_promotion_allowed_count": 0, "benchmark_ready_allowed_count": 0, "customer_view_allowed_count": 0,
        "generated_compatibility_pair_count": 0, "proposed_source_pack_mutation_count": 0, "production_safe_candidate_count": 0,
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
    readiness_csv = tmp_path / "readiness.csv"; _write_csv(readiness_csv, _readiness_csv_rows())
    readiness_json = tmp_path / "readiness.json"; readiness_json.write_text(json.dumps(_readiness_report()), encoding="utf-8")
    source_pack = tmp_path / "source_pack"; source_pack.mkdir(); (source_pack / "keep.txt").write_text("unchanged", encoding="utf-8")
    return readiness_json, readiness_csv, source_pack


def _build(artifacts, **kwargs):
    readiness_json, readiness_csv, source_pack = artifacts
    return mod.build_evidence_requirements_report(readiness_json, readiness_csv, source_pack=source_pack, out=kwargs.pop("out", None), **kwargs)


def test_valid_evidence_requirements_report_passes(artifacts, tmp_path):
    rows, report = _build(artifacts, out=tmp_path / "requirements.csv")
    assert report["valid"] is True
    assert report["errors"] == []
    assert report["requirement_row_count"] == 5
    assert rows[0]["requirement_id"] == "TECE-DP-COMPAT-EVIDENCE-REQ-000001"


def test_requires_valid_readiness_report(artifacts):
    artifacts[0].write_text(json.dumps(_readiness_report(valid=False)), encoding="utf-8")
    assert _build(artifacts)[1]["valid"] is False


def test_requires_tecedrainprofile_family(artifacts):
    artifacts[0].write_text(json.dumps(_readiness_report(family="TECEdrainline")), encoding="utf-8")
    assert _build(artifacts)[1]["valid"] is False
    assert _build(artifacts, family="TECEdrainline")[1]["valid"] is False


def test_source_pack_baseline_counts_are_enforced(artifacts, monkeypatch):
    monkeypatch.setattr(mod, "load_source_pack", lambda _p: SimpleNamespace(rows=[]))
    assert _build(artifacts)[1]["valid"] is False


def test_readiness_csv_must_have_8_rows(artifacts):
    _write_csv(artifacts[1], _readiness_csv_rows()[:-1])
    assert _build(artifacts)[1]["valid"] is False


def test_readiness_report_audit_row_count_is_enforced(artifacts):
    artifacts[0].write_text(json.dumps(_readiness_report(audit_row_count=7)), encoding="utf-8")
    assert _build(artifacts)[1]["valid"] is False


def test_role_overlay_chain_valid_is_enforced(artifacts):
    artifacts[0].write_text(json.dumps(_readiness_report(role_overlay_chain_valid=False)), encoding="utf-8")
    assert _build(artifacts)[1]["valid"] is False


def test_source_pack_baseline_valid_is_enforced(artifacts):
    artifacts[0].write_text(json.dumps(_readiness_report(source_pack_baseline_valid=False)), encoding="utf-8")
    assert _build(artifacts)[1]["valid"] is False


@pytest.mark.parametrize("field", ["reviewed_effective_drain_body_count", "reviewed_effective_accessory_count", "reviewed_effective_profile_cover_count"])
def test_reviewed_effective_role_counts_are_enforced(artifacts, field):
    artifacts[0].write_text(json.dumps(_readiness_report(**{field: 99})), encoding="utf-8")
    assert _build(artifacts)[1]["valid"] is False


def test_retained_unresolved_length_count_is_enforced(artifacts):
    artifacts[0].write_text(json.dumps(_readiness_report(retained_unresolved_length_count=2)), encoding="utf-8")
    assert _build(artifacts)[1]["valid"] is False


def test_explicit_article_level_compatibility_evidence_count_zero_is_enforced(artifacts):
    artifacts[0].write_text(json.dumps(_readiness_report(explicit_article_level_compatibility_evidence_count=1)), encoding="utf-8")
    assert _build(artifacts)[1]["valid"] is False


def test_output_csv_has_exactly_5_rows(artifacts, tmp_path):
    out = tmp_path / "requirements.csv"
    _build(artifacts, out=out)
    assert len(list(csv.DictReader(out.open(encoding="utf-8-sig")))) == 5


def test_required_requirement_ids_are_present(artifacts):
    assert _build(artifacts)[1]["requirement_ids"] == [f"TECE-DP-COMPAT-EVIDENCE-REQ-{i:06d}" for i in range(1, 6)]


def test_required_requirement_areas_are_present(artifacts):
    rows, _ = _build(artifacts)
    assert [r["requirement_area"] for r in rows] == [r["requirement_area"] for r in mod.requirement_rows()]


def test_duplicate_requirement_ids_fail(artifacts):
    rows, _ = _build(artifacts); rows[1]["requirement_id"] = rows[0]["requirement_id"]
    assert _build(artifacts, rows=rows)[1]["valid"] is False


@pytest.mark.parametrize("field", [
    "pairing_design_allowed_after_collection", "compatibility_pair_generation_allowed", "candidate_pair_matrix_allowed",
    "length_overlay_allowed", "source_pack_mutation_allowed", "production_promotion_allowed",
    "benchmark_ready_allowed", "customer_view_allowed",
])
def test_allowed_true_fails(artifacts, field):
    rows, _ = _build(artifacts); rows[0][field] = "true"
    assert _build(artifacts, rows=rows)[1]["valid"] is False


def test_generated_compatibility_pair_count_gt_zero_fails(artifacts):
    artifacts[0].write_text(json.dumps(_readiness_report(generated_compatibility_pair_count=1)), encoding="utf-8")
    assert _build(artifacts)[1]["valid"] is False


def test_generated_candidate_pair_matrix_row_count_gt_zero_fails(artifacts):
    artifacts[0].write_text(json.dumps(_readiness_report(generated_candidate_pair_matrix_row_count=1)), encoding="utf-8")
    assert _build(artifacts)[1]["valid"] is False


def test_generated_length_overlay_row_count_gt_zero_fails(artifacts):
    artifacts[0].write_text(json.dumps(_readiness_report(generated_length_overlay_row_count=1)), encoding="utf-8")
    assert _build(artifacts)[1]["valid"] is False


def test_production_safe_candidate_count_gt_zero_fails(artifacts):
    artifacts[0].write_text(json.dumps(_readiness_report(production_safe_candidate_count=1)), encoding="utf-8")
    assert _build(artifacts)[1]["valid"] is False


def test_diagnostic_only_note_is_present(artifacts):
    assert mod.DIAGNOSTIC_ONLY_NOTE == _build(artifacts)[1]["diagnostic_only_note"]


def _hash_tree(path: Path) -> dict[str, str]:
    return {str(p.relative_to(path)): hashlib.sha256(p.read_bytes()).hexdigest() for p in sorted(path.rglob("*")) if p.is_file()}


def test_source_pack_immutability(artifacts, tmp_path):
    before = _hash_tree(artifacts[2])
    _build(artifacts, out=tmp_path / "requirements.csv")
    assert _hash_tree(artifacts[2]) == before


def test_aco_canonical_baseline_expected_tokens_remain_documented():
    assert "OVERALL: PASS"
    assert "OVERALL: ACO_BASELINE_STABLE"
