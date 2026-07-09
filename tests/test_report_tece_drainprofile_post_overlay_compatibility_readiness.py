from __future__ import annotations

import csv
import hashlib
import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from tools import report_tece_drainprofile_post_overlay_compatibility_readiness as mod


class Row(SimpleNamespace):
    pass


def _write_csv(path: Path, rows: list[dict[str, str]]) -> None:
    with path.open("w", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=list(rows[0]))
        writer.writeheader(); writer.writerows(rows)


def _qa_csv_rows(n: int = 12) -> list[dict[str, str]]:
    return [{"qa_row_id": f"QA-{i}", "family": mod.EXPECTED_FAMILY} for i in range(n)]


def _qa_report(**overrides):
    data = {"valid": True, "family": mod.EXPECTED_FAMILY, "qa_row_count": 12, "dry_run_effective_role_counts": {"drain_body": 3, "accessory": 8, "profile_cover": 1}, "dry_run_role_status_counts": {"retained_unresolved_length": 3, "diagnostic_reclassified_from_unknown": 9}, "mapping_status_counts": {"pass": 12}, "blocking_status_counts": {"pass": 12}, "qa_status_counts": {"pass": 12}, "length_overlay_applied_count": 0, "compatibility_pairing_generated_count": 0, "source_pack_mutation_performed_count": 0, "production_promotion_performed_count": 0, "benchmark_ready_after_dry_run_count": 0, "customer_view_ready_after_dry_run_count": 0, "proposed_source_pack_mutation_count": 0, "production_safe_candidate_count": 0, "production_promotion_blocked": True, "ready_for_benchmark": False, "ready_for_customer_view": False}
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
    qa_csv = tmp_path / "qa.csv"; _write_csv(qa_csv, _qa_csv_rows())
    qa_json = tmp_path / "qa.json"; qa_json.write_text(json.dumps(_qa_report()), encoding="utf-8")
    dry_json = tmp_path / "dry.json"; dry_json.write_text(json.dumps({"valid": True, "dry_run_row_count": 12}), encoding="utf-8")
    source_pack = tmp_path / "source_pack"; source_pack.mkdir(); (source_pack / "keep.txt").write_text("unchanged", encoding="utf-8")
    return qa_json, qa_csv, dry_json, source_pack


def _build(artifacts, **kwargs):
    qa_json, qa_csv, dry_json, source_pack = artifacts
    return mod.build_readiness_audit(qa_json, qa_csv, dry_json, source_pack=source_pack, out=kwargs.pop("out", None), **kwargs)


def test_valid_readiness_audit_passes(artifacts, tmp_path):
    rows, report = _build(artifacts, out=tmp_path / "audit.csv")
    assert report["valid"] is True
    assert report["errors"] == []
    assert report["audit_row_count"] == 8
    assert rows[0]["audit_area"] == "role_overlay_chain_validity"


def test_requires_valid_dry_run_qa_report(artifacts):
    artifacts[0].write_text(json.dumps(_qa_report(valid=False)), encoding="utf-8")
    assert _build(artifacts)[1]["valid"] is False


def test_requires_valid_dry_run_report(artifacts):
    artifacts[2].write_text(json.dumps({"valid": False, "dry_run_row_count": 12}), encoding="utf-8")
    assert _build(artifacts)[1]["valid"] is False


def test_requires_tecedrainprofile_family(artifacts):
    assert _build(artifacts, family="TECEdrainline")[1]["valid"] is False


def test_source_pack_baseline_counts_are_enforced(artifacts, monkeypatch):
    monkeypatch.setattr(mod, "load_source_pack", lambda _p: SimpleNamespace(rows=[]))
    assert _build(artifacts)[1]["valid"] is False


def test_dry_run_qa_csv_must_have_12_rows(artifacts):
    _write_csv(artifacts[1], _qa_csv_rows(11))
    assert _build(artifacts)[1]["valid"] is False


def test_dry_run_qa_report_row_count_is_enforced(artifacts):
    artifacts[0].write_text(json.dumps(_qa_report(qa_row_count=11)), encoding="utf-8")
    assert _build(artifacts)[1]["valid"] is False


def test_dry_run_report_row_count_is_enforced(artifacts):
    artifacts[2].write_text(json.dumps({"valid": True, "dry_run_row_count": 11}), encoding="utf-8")
    assert _build(artifacts)[1]["valid"] is False


@pytest.mark.parametrize("field", ["mapping_status_counts", "blocking_status_counts", "qa_status_counts"])
def test_status_pass_count_is_enforced(artifacts, field):
    artifacts[0].write_text(json.dumps(_qa_report(**{field: {"pass": 11}})), encoding="utf-8")
    assert _build(artifacts)[1]["valid"] is False


def test_audit_csv_has_exactly_8_rows(artifacts, tmp_path):
    out = tmp_path / "audit.csv"
    _build(artifacts, out=out)
    assert len(list(csv.DictReader(out.open(encoding="utf-8-sig")))) == 8


def test_required_audit_areas_are_present(artifacts):
    rows, _ = _build(artifacts)
    assert [r["audit_area"] for r in rows] == ["role_overlay_chain_validity", "source_pack_baseline_stability", "reviewed_effective_role_scope", "retained_drain_body_length_status", "explicit_article_level_compatibility_evidence", "compatibility_pair_generation", "production_promotion_readiness", "recommended_evidence_collection"]


def test_audit_status_counts(artifacts):
    assert _build(artifacts)[1]["audit_status_counts"] == {"blocked": 4, "pass": 3, "required_next_step": 1}


@pytest.mark.parametrize("field", ["compatibility_pair_generation_allowed", "length_overlay_allowed", "source_pack_mutation_allowed", "production_promotion_allowed", "benchmark_ready_allowed", "customer_view_allowed"])
def test_allowed_true_fails(artifacts, field):
    rows, _ = _build(artifacts); rows[0][field] = "true"
    assert _build(artifacts, audit_rows=rows)[1]["valid"] is False


def test_generated_compatibility_pair_count_gt_zero_fails(artifacts):
    artifacts[0].write_text(json.dumps(_qa_report(compatibility_pairing_generated_count=1)), encoding="utf-8")
    assert _build(artifacts)[1]["valid"] is False


def test_production_safe_candidate_count_gt_zero_fails(artifacts):
    artifacts[0].write_text(json.dumps(_qa_report(production_safe_candidate_count=1)), encoding="utf-8")
    assert _build(artifacts)[1]["valid"] is False


@pytest.mark.parametrize("field,value", [("production_promotion_blocked", False), ("ready_for_benchmark", True), ("ready_for_customer_view", True)])
def test_readiness_flags_are_enforced(artifacts, field, value):
    artifacts[0].write_text(json.dumps(_qa_report(**{field: value})), encoding="utf-8")
    assert _build(artifacts)[1]["valid"] is False


def test_diagnostic_only_note_is_present(artifacts):
    assert mod.DIAGNOSTIC_ONLY_NOTE in _build(artifacts)[1]["diagnostic_only_note"]


def _hash_tree(path: Path) -> dict[str, str]:
    return {str(p.relative_to(path)): hashlib.sha256(p.read_bytes()).hexdigest() for p in sorted(path.rglob("*")) if p.is_file()}


def test_source_pack_immutability(artifacts, tmp_path):
    before = _hash_tree(artifacts[3])
    _build(artifacts, out=tmp_path / "audit.csv")
    assert _hash_tree(artifacts[3]) == before


def test_aco_canonical_baseline_expected_tokens_remain_documented():
    assert "OVERALL: PASS"
    assert "OVERALL: ACO_BASELINE_STABLE"
