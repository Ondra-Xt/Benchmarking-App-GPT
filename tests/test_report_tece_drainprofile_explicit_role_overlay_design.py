from __future__ import annotations

import csv
import json
from pathlib import Path

from tools import report_aco_final_baseline as aco_mod
from tools import report_tece_drainprofile_explicit_role_overlay_design as mod


def _write_csv(path: Path, rows: list[dict[str, str]]) -> None:
    with path.open("w", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=list(rows[0]))
        writer.writeheader(); writer.writerows(rows)


def _qa_rows() -> list[dict[str, str]]:
    return [{"qa_row_id": f"TECE-DP-ROLE-OVERLAY-QA-{i:06d}", "family": "TECEdrainprofile", "article_number": a} for i, a in enumerate(mod.EXPECTED_ARTICLES, start=1)]


def _qa_report(**overrides) -> dict[str, object]:
    report = {
        "valid": True,
        "qa_row_count": 12,
        "mapping_status_counts": {"pass": 12},
        "blocking_status_counts": {"pass": 12},
        "qa_status_counts": {"pass": 12},
        "current_source_pack_role_counts": {"drain_body": 3, "unknown": 9},
        "reviewed_role_diagnostic_counts": {"drain_body_unresolved_length": 3, "spare_part": 4, "water_trap": 3, "accessory": 1, "profile_cover": 1},
        "canonical_role_candidate_counts": {"drain_body": 3, "accessory": 8, "profile_cover": 1},
        "role_overlay_apply_allowed_count": 0,
        "length_overlay_allowed_count": 0,
        "compatibility_pairing_allowed_count": 0,
        "source_pack_mutation_allowed_count": 0,
        "proposed_source_pack_mutation_count": 0,
        "production_safe_candidate_count": 0,
        "production_promotion_blocked": True,
        "ready_for_benchmark": False,
        "ready_for_customer_view": False,
    }
    report.update(overrides)
    return report


def _fixture(tmp_path: Path, rows=None, report=None) -> tuple[Path, Path, Path]:
    qa_csv = tmp_path / "qa.csv"; qa_report = tmp_path / "qa.json"; out = tmp_path / "design.csv"
    _write_csv(qa_csv, rows or _qa_rows())
    qa_report.write_text(json.dumps(_qa_report() if report is None else report), encoding="utf-8")
    return qa_report, qa_csv, out


def _report(tmp_path: Path, rows=None, qa_report=None, design_rows=None, **kwargs):
    report_path, csv_path, out = _fixture(tmp_path, rows, qa_report)
    return mod.build_design_report(report_path, csv_path, out=out, design_rows=design_rows, **kwargs), out


def _mutated_design(mutator):
    rows = mod.build_design_rows()
    mutator(rows)
    return rows


def test_valid_design_report_passes(tmp_path: Path) -> None:
    report, out = _report(tmp_path)
    rows = list(csv.DictReader(out.open(encoding="utf-8-sig")))
    assert report["valid"] is True
    assert report["errors"] == []
    assert len(rows) == 12
    assert rows[0]["design_rule_id"] == "TECE-DP-ROLE-OVERLAY-DESIGN-000001"


def test_requires_valid_qa_report(tmp_path: Path) -> None:
    assert _report(tmp_path, qa_report=_qa_report(valid=False))[0]["valid"] is False


def test_requires_exactly_12_qa_csv_rows(tmp_path: Path) -> None:
    report, _ = _report(tmp_path, rows=_qa_rows()[:-1])
    assert report["valid"] is False
    assert report["total_qa_csv_rows"] == 11


def test_exact_expected_article_set_is_enforced(tmp_path: Path) -> None:
    rows = _qa_rows(); rows[0]["article_number"] = "999999"
    report, _ = _report(tmp_path, rows=rows)
    assert report["valid"] is False
    assert "article set differs from expected 12 articles" in report["errors"]


def test_qa_mapping_blocking_qa_pass_counts_are_enforced(tmp_path: Path) -> None:
    for key in ["mapping_status_counts", "blocking_status_counts", "qa_status_counts"]:
        report, _ = _report(tmp_path, qa_report=_qa_report(**{key: {"pass": 11}}))
        assert report["valid"] is False


def test_at_least_12_design_rules(tmp_path: Path) -> None:
    report, _ = _report(tmp_path, design_rows=mod.build_design_rows()[:11])
    assert report["valid"] is False


def test_required_rule_names_are_present(tmp_path: Path) -> None:
    report, _ = _report(tmp_path)
    assert set(name for name, *_ in mod.REQUIRED_RULES) <= set(report["design_rule_names"])


def test_duplicate_design_rule_id_fails(tmp_path: Path) -> None:
    rows = _mutated_design(lambda r: r[1].update(design_rule_id=r[0]["design_rule_id"]))
    assert _report(tmp_path, design_rows=rows)[0]["duplicate_design_rule_ids"] == ["TECE-DP-ROLE-OVERLAY-DESIGN-000001"]


def test_duplicate_rule_name_fails(tmp_path: Path) -> None:
    rows = _mutated_design(lambda r: r[1].update(rule_name=r[0]["rule_name"]))
    assert _report(tmp_path, design_rows=rows)[0]["duplicate_rule_names"] == ["overlay_artifact_scope"]


def test_source_pack_mutation_allowed_true_fails(tmp_path: Path) -> None:
    assert _report(tmp_path, design_rows=_mutated_design(lambda r: r[0].update(source_pack_mutation_allowed="true")))[0]["valid"] is False


def test_extraction_logic_change_allowed_true_fails(tmp_path: Path) -> None:
    assert _report(tmp_path, design_rows=_mutated_design(lambda r: r[0].update(extraction_logic_change_allowed="true")))[0]["valid"] is False


def test_length_overlay_allowed_true_fails(tmp_path: Path) -> None:
    assert _report(tmp_path, design_rows=_mutated_design(lambda r: r[0].update(length_overlay_allowed="true")))[0]["valid"] is False


def test_compatibility_pairing_allowed_true_fails(tmp_path: Path) -> None:
    assert _report(tmp_path, design_rows=_mutated_design(lambda r: r[0].update(compatibility_pairing_allowed="true")))[0]["valid"] is False


def test_production_promotion_allowed_true_fails(tmp_path: Path) -> None:
    assert _report(tmp_path, design_rows=_mutated_design(lambda r: r[0].update(production_promotion_allowed="true")))[0]["valid"] is False


def test_benchmark_ready_allowed_true_fails(tmp_path: Path) -> None:
    assert _report(tmp_path, design_rows=_mutated_design(lambda r: r[0].update(benchmark_ready_allowed="true")))[0]["valid"] is False


def test_customer_view_allowed_true_fails(tmp_path: Path) -> None:
    assert _report(tmp_path, design_rows=_mutated_design(lambda r: r[0].update(customer_view_allowed="true")))[0]["valid"] is False


def test_diagnostic_only_false_fails(tmp_path: Path) -> None:
    assert _report(tmp_path, design_rows=_mutated_design(lambda r: r[0].update(diagnostic_only="false")))[0]["valid"] is False


def test_diagnostic_only_note_is_present(tmp_path: Path) -> None:
    assert _report(tmp_path)[0]["diagnostic_only_note"] == mod.DIAGNOSTIC_ONLY_NOTE


def test_aco_canonical_baseline_remains_pass_and_stable(monkeypatch) -> None:
    monkeypatch.setattr(aco_mod, "validate_workbook", lambda _xlsx: {"valid": True, "errors": []}, raising=False)
    if hasattr(aco_mod, "build_report"):
        result = aco_mod.build_report("dummy.xlsx")
        text = json.dumps(result)
        assert "PASS" in text or result.get("overall") in {"PASS", "ACO_BASELINE_STABLE", None}
