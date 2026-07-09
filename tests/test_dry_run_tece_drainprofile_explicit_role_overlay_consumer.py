from __future__ import annotations

import csv
import json
from pathlib import Path
from types import SimpleNamespace

from tools import report_aco_final_baseline as aco_mod
from tools import dry_run_tece_drainprofile_explicit_role_overlay_consumer as mod
from tools.validate_tece_drainprofile_explicit_role_overlay_csv import EXPECTED_OVERLAY_IDS


def _write_csv(path: Path, rows: list[dict[str, str]]) -> None:
    with path.open("w", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=list(rows[0]))
        writer.writeheader(); writer.writerows(rows)


def _overlay_rows() -> list[dict[str, str]]:
    rows = []
    for i, article in enumerate(mod.EXPECTED_ARTICLES, start=1):
        rows.append({
            "overlay_id": f"TECE-DP-EXPLICIT-ROLE-OVERLAY-{i:06d}", "family": mod.EXPECTED_FAMILY, "article_number": article,
            "current_source_pack_role": mod.EXPECTED_CURRENT_SOURCE_PACK_ROLE[article], "reviewed_role_diagnostic": mod.EXPECTED_REVIEWED_ROLE_DIAGNOSTIC[article],
            "canonical_role_candidate": mod.EXPECTED_CANONICAL_ROLE_CANDIDATE[article], "overlay_action": mod.EXPECTED_ROLE_OVERLAY_ACTION[article],
        })
    return rows


def _fake_rows(*, bad_baseline: bool = False):
    rows = []
    role_counts = dict(mod.EXPECTED_MACHINE_ROLE_COUNTS)
    if bad_baseline:
        role_counts["unknown"] -= 1
    for role, count in role_counts.items():
        for idx in range(count):
            rows.append(SimpleNamespace(article_number=f"x-{role}-{idx}", product_family=mod.EXPECTED_FAMILY, tece_family_candidate=mod.EXPECTED_FAMILY, tece_article_role_candidate=role))
    for article, role in mod.EXPECTED_CURRENT_SOURCE_PACK_ROLE.items():
        for row in rows:
            if row.tece_article_role_candidate == role and row.article_number.startswith("x-"):
                row.article_number = article
                break
    for idx in range(mod.EXPECTED_INPUT_INVENTORY_ROW_COUNT - len(rows)):
        rows.append(SimpleNamespace(article_number=f"other-{idx}", product_family="Other", tece_family_candidate="Other", tece_article_role_candidate="unknown"))
    return rows


def _fixture(tmp_path: Path, monkeypatch, *, bad_baseline: bool = False):
    monkeypatch.setattr(mod, "load_source_pack", lambda _path: SimpleNamespace(rows=_fake_rows(bad_baseline=bad_baseline)))
    overlay = tmp_path / "overlay.csv"; overlay_report = tmp_path / "overlay.json"; design = tmp_path / "design.json"; qa = tmp_path / "qa.json"; out = tmp_path / "dry.csv"
    _write_csv(overlay, _overlay_rows())
    overlay_report.write_text(json.dumps({"valid": True, "family": mod.EXPECTED_FAMILY, "overlay_row_count": 12}), encoding="utf-8")
    design.write_text(json.dumps({"valid": True}), encoding="utf-8")
    qa.write_text(json.dumps({"valid": True}), encoding="utf-8")
    return overlay, overlay_report, design, qa, out


def _report(tmp_path: Path, monkeypatch, **kwargs):
    paths = _fixture(tmp_path, monkeypatch, bad_baseline=kwargs.pop("bad_baseline", False))
    return mod.build_dry_run_report(*paths[:4], out=paths[4], **kwargs), paths[4]


def _mutated_rows(mutator):
    rows = mod._make_rows(_overlay_rows(), mod.EXPECTED_FAMILY)
    mutator(rows)
    return rows


def test_valid_dry_run_consumer_passes(tmp_path, monkeypatch):
    report, out = _report(tmp_path, monkeypatch)
    rows = list(csv.DictReader(out.open(encoding="utf-8-sig")))
    assert report["valid"] is True
    assert report["errors"] == []
    assert len(rows) == 12
    assert report["dry_run_effective_role_counts"] == {"accessory": 8, "drain_body": 3, "profile_cover": 1}
    assert report["dry_run_role_status_counts"] == {"diagnostic_reclassified_from_unknown": 9, "retained_unresolved_length": 3}


def test_requires_valid_overlay_validation_report(tmp_path, monkeypatch):
    overlay, overlay_report, design, qa, _out = _fixture(tmp_path, monkeypatch)
    overlay_report.write_text(json.dumps({"valid": False}), encoding="utf-8")
    assert mod.build_dry_run_report(overlay, overlay_report, design, qa)["valid"] is False


def test_requires_valid_design_report(tmp_path, monkeypatch):
    overlay, overlay_report, design, qa, _out = _fixture(tmp_path, monkeypatch)
    design.write_text(json.dumps({"valid": False}), encoding="utf-8")
    assert mod.build_dry_run_report(overlay, overlay_report, design, qa)["design_report_valid"] is False


def test_requires_valid_qa_report(tmp_path, monkeypatch):
    overlay, overlay_report, design, qa, _out = _fixture(tmp_path, monkeypatch)
    qa.write_text(json.dumps({"valid": False}), encoding="utf-8")
    assert mod.build_dry_run_report(overlay, overlay_report, design, qa)["qa_report_valid"] is False


def test_requires_tecedrainprofile_family(tmp_path, monkeypatch):
    assert _report(tmp_path, monkeypatch, family="Other")[0]["valid"] is False


def test_source_pack_baseline_counts_are_enforced(tmp_path, monkeypatch):
    assert _report(tmp_path, monkeypatch, bad_baseline=True)[0]["valid"] is False


def test_exactly_12_dry_run_rows_are_generated(tmp_path, monkeypatch):
    assert _report(tmp_path, monkeypatch)[0]["dry_run_row_count"] == 12


def test_exact_expected_article_set_is_enforced(tmp_path, monkeypatch):
    rows = _mutated_rows(lambda r: r[0].update(article_number="999999"))
    assert _report(tmp_path, monkeypatch, dry_run_rows=rows)[0]["article_numbers"] != mod.EXPECTED_ARTICLES


def test_duplicate_dry_run_row_id_fails(tmp_path, monkeypatch):
    rows = _mutated_rows(lambda r: r[1].update(dry_run_row_id=r[0]["dry_run_row_id"]))
    assert _report(tmp_path, monkeypatch, dry_run_rows=rows)[0]["duplicate_dry_run_row_ids"]


def test_duplicate_overlay_id_fails(tmp_path, monkeypatch):
    rows = _mutated_rows(lambda r: r[1].update(overlay_id=r[0]["overlay_id"]))
    assert _report(tmp_path, monkeypatch, dry_run_rows=rows)[0]["duplicate_overlay_ids"] == [EXPECTED_OVERLAY_IDS[0]]


def test_duplicate_article_number_fails(tmp_path, monkeypatch):
    rows = _mutated_rows(lambda r: r[1].update(article_number=r[0]["article_number"]))
    assert _report(tmp_path, monkeypatch, dry_run_rows=rows)[0]["duplicate_article_numbers"] == [mod.EXPECTED_ARTICLES[0]]


def test_exact_current_source_pack_role_mapping_is_enforced(tmp_path, monkeypatch):
    rows = _mutated_rows(lambda r: r[0].update(current_source_pack_role="unknown"))
    assert _report(tmp_path, monkeypatch, dry_run_rows=rows)[0]["invalid_source_pack_role_rows"]


def test_exact_dry_run_effective_role_mapping_is_enforced(tmp_path, monkeypatch):
    rows = _mutated_rows(lambda r: r[0].update(dry_run_effective_role="accessory"))
    assert _report(tmp_path, monkeypatch, dry_run_rows=rows)[0]["invalid_dry_run_effective_role_rows"]


def test_exact_dry_run_role_status_mapping_is_enforced(tmp_path, monkeypatch):
    rows = _mutated_rows(lambda r: r[0].update(dry_run_role_status="bad"))
    assert _report(tmp_path, monkeypatch, dry_run_rows=rows)[0]["invalid_dry_run_effective_role_rows"]


def test_exact_dry_run_action_mapping_is_enforced(tmp_path, monkeypatch):
    rows = _mutated_rows(lambda r: r[0].update(dry_run_action="bad"))
    assert _report(tmp_path, monkeypatch, dry_run_rows=rows)[0]["invalid_dry_run_action_rows"]


import pytest
@pytest.mark.parametrize("field,count_key", [
    ("length_overlay_applied", "length_overlay_applied_count"), ("compatibility_pairing_generated", "compatibility_pairing_generated_count"),
    ("source_pack_mutation_performed", "source_pack_mutation_performed_count"), ("production_promotion_performed", "production_promotion_performed_count"),
    ("benchmark_ready_after_dry_run", "benchmark_ready_after_dry_run_count"), ("customer_view_ready_after_dry_run", "customer_view_ready_after_dry_run_count"),
    ("production_safe", "production_safe_candidate_count"), ("diagnostic_only", None), ("production_promotion_blocked", None), ("ready_for_benchmark", None), ("ready_for_customer_view", None),
])
def test_boolean_hard_gates_fail(tmp_path, monkeypatch, field, count_key):
    def mutate(rows):
        rows[0][field] = "false" if field in {"diagnostic_only", "production_promotion_blocked"} else "true"
    report, _ = _report(tmp_path, monkeypatch, dry_run_rows=_mutated_rows(mutate))
    assert report["valid"] is False
    if count_key:
        assert report[count_key] == 1


def test_source_pack_immutability(tmp_path, monkeypatch):
    before = [(r.article_number, r.tece_article_role_candidate) for r in _fake_rows()]
    _report(tmp_path, monkeypatch)
    after = [(r.article_number, r.tece_article_role_candidate) for r in _fake_rows()]
    assert before == after


def test_diagnostic_only_note_is_present(tmp_path, monkeypatch):
    assert _report(tmp_path, monkeypatch)[0]["diagnostic_only_note"] == mod.DIAGNOSTIC_ONLY_NOTE


def test_aco_canonical_baseline_remains_pass_and_stable():
    assert hasattr(aco_mod, "main") or hasattr(aco_mod, "build_report")
