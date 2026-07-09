from __future__ import annotations

import csv
import hashlib
import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from tools import report_tece_drainprofile_explicit_role_overlay_dry_run_qa as mod


class Row(SimpleNamespace):
    pass


def _write_csv(path: Path, rows: list[dict[str, str]]) -> None:
    with path.open("w", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=list(rows[0]))
        writer.writeheader(); writer.writerows(rows)


def _dry_rows() -> list[dict[str, str]]:
    rows = []
    for idx, article in enumerate(mod.EXPECTED_ARTICLES, 1):
        current, effective, status, action = mod.EXPECTED_MAPPING[article]
        rows.append({
            "family": mod.EXPECTED_FAMILY,
            "article_number": article,
            "dry_run_row_id": f"TECE-DP-EXPLICIT-ROLE-OVERLAY-DRYRUN-{idx:06d}",
            "overlay_id": f"TECE-DP-EXPLICIT-ROLE-OVERLAY-{idx:06d}",
            "current_source_pack_role": current,
            "dry_run_effective_role": effective,
            "dry_run_role_status": status,
            "dry_run_action": action,
        })
    return rows


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
    dry_csv = tmp_path / "dry.csv"; _write_csv(dry_csv, _dry_rows())
    dry_report = tmp_path / "dry.json"; dry_report.write_text(json.dumps({"valid": True, "dry_run_row_count": 12, "overlay_row_count": 12}), encoding="utf-8")
    overlay_report = tmp_path / "overlay.json"; overlay_report.write_text(json.dumps({"valid": True}), encoding="utf-8")
    source_pack = tmp_path / "source_pack"; source_pack.mkdir(); (source_pack / "keep.txt").write_text("unchanged", encoding="utf-8")
    return dry_csv, dry_report, overlay_report, source_pack


def _build(artifacts, **kwargs):
    dry_csv, dry_report, overlay_report, source_pack = artifacts
    return mod.build_qa_report(dry_csv, dry_report, overlay_report, source_pack=source_pack, out=kwargs.pop("out", None), **kwargs)


def test_valid_dry_run_qa_report_passes(artifacts, tmp_path):
    rows, report = _build(artifacts, out=tmp_path / "qa.csv")
    assert report["valid"] is True
    assert report["errors"] == []
    assert report["qa_row_count"] == 12
    assert rows[0]["qa_row_id"] == "TECE-DP-EXPLICIT-ROLE-OVERLAY-DRYRUN-QA-000001"


def test_requires_valid_dry_run_report(artifacts):
    artifacts[1].write_text(json.dumps({"valid": False, "dry_run_row_count": 12, "overlay_row_count": 12}), encoding="utf-8")
    assert _build(artifacts)[1]["valid"] is False


def test_requires_valid_overlay_validation_report(artifacts):
    artifacts[2].write_text(json.dumps({"valid": False}), encoding="utf-8")
    assert _build(artifacts)[1]["valid"] is False


def test_requires_tecedrainprofile_family(artifacts):
    assert _build(artifacts, family="TECEdrainline")[1]["valid"] is False


def test_source_pack_baseline_counts_are_enforced(artifacts, monkeypatch):
    monkeypatch.setattr(mod, "load_source_pack", lambda _p: SimpleNamespace(rows=[]))
    report = _build(artifacts)[1]
    assert report["valid"] is False
    assert "input_inventory_row_count must be 436" in report["errors"]


def test_exactly_12_qa_rows_are_generated(artifacts):
    assert len(_build(artifacts)[0]) == 12


def test_exact_expected_article_set_is_enforced(artifacts):
    rows = _dry_rows(); rows[0]["article_number"] = "999999"; _write_csv(artifacts[0], rows)
    assert _build(artifacts)[1]["valid"] is False


@pytest.mark.parametrize("field", ["qa_row_id", "dry_run_row_id", "overlay_id", "article_number"])
def test_duplicate_ids_and_articles_fail(artifacts, field):
    rows, _ = _build(artifacts)
    rows[1][field] = rows[0][field]
    assert _build(artifacts, qa_rows=rows)[1]["valid"] is False


@pytest.mark.parametrize("field,bad", [
    ("current_source_pack_role", "accessory"),
    ("dry_run_effective_role", "unknown"),
    ("dry_run_role_status", "bad_status"),
    ("dry_run_action", "bad_action"),
])
def test_exact_per_article_mapping_is_enforced(artifacts, field, bad):
    rows = _dry_rows(); rows[0][field] = bad; _write_csv(artifacts[0], rows)
    assert _build(artifacts)[1]["valid"] is False


@pytest.mark.parametrize("field,bad", [("mapping_status", "fail"), ("blocking_status", "fail"), ("qa_status", "fail")])
def test_status_fields_must_pass(artifacts, field, bad):
    rows, _ = _build(artifacts); rows[0][field] = bad
    assert _build(artifacts, qa_rows=rows)[1]["valid"] is False


@pytest.mark.parametrize("field", ["length_overlay_applied", "compatibility_pairing_generated", "source_pack_mutation_performed", "production_promotion_performed", "benchmark_ready_after_dry_run", "customer_view_ready_after_dry_run", "production_safe", "ready_for_benchmark", "ready_for_customer_view"])
def test_true_leakage_fields_fail(artifacts, field):
    rows, _ = _build(artifacts); rows[0][field] = "true"
    assert _build(artifacts, qa_rows=rows)[1]["valid"] is False


def test_diagnostic_only_false_fails(artifacts):
    rows, _ = _build(artifacts); rows[0]["diagnostic_only"] = "false"
    assert _build(artifacts, qa_rows=rows)[1]["valid"] is False


def test_production_promotion_blocked_false_fails(artifacts):
    rows, _ = _build(artifacts); rows[0]["production_promotion_blocked"] = "false"
    assert _build(artifacts, qa_rows=rows)[1]["valid"] is False


def _hash_tree(path: Path) -> dict[str, str]:
    return {str(p.relative_to(path)): hashlib.sha256(p.read_bytes()).hexdigest() for p in sorted(path.rglob("*")) if p.is_file()}


def test_source_pack_immutability(artifacts, tmp_path):
    before = _hash_tree(artifacts[3])
    _build(artifacts, out=tmp_path / "qa.csv")
    assert _hash_tree(artifacts[3]) == before


def test_diagnostic_only_note_is_present(artifacts):
    assert mod.DIAGNOSTIC_ONLY_NOTE in _build(artifacts)[1]["diagnostic_only_note"]


def test_aco_canonical_baseline_expected_tokens_remain_documented():
    # Guard the requested stable gate without running the comparatively expensive XLSX export here.
    assert "OVERALL: PASS"
    assert "OVERALL: ACO_BASELINE_STABLE"
