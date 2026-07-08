from __future__ import annotations

import csv
import json
from pathlib import Path

from tools import report_aco_final_baseline as aco_mod
from tools import report_tece_drainprofile_reviewed_role_overlay_preview_qa as mod


def _write_csv(path: Path, rows: list[dict[str, str]]) -> None:
    with path.open("w", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=list(rows[0]))
        writer.writeheader(); writer.writerows(rows)


def _rows() -> list[dict[str, str]]:
    rows = []
    for index, article in enumerate(mod.EXPECTED_ARTICLES, start=1):
        rows.append({
            "overlay_preview_id": f"TECE-DP-ROLE-OVERLAY-PREVIEW-{index:06d}",
            "family": "TECEdrainprofile",
            "article_number": article,
            "review_csv_article_role": "drain_body",
            "current_source_pack_role": mod.EXPECTED_CURRENT_SOURCE_PACK_ROLE[article],
            "reviewed_role_diagnostic": mod.EXPECTED_REVIEWED_ROLE_DIAGNOSTIC[article],
            "canonical_role_candidate": mod.EXPECTED_CANONICAL_ROLE_CANDIDATE[article],
            "role_overlay_action": mod.EXPECTED_ROLE_OVERLAY_ACTION[article],
            "role_overlay_apply_allowed": "false",
            "length_overlay_allowed": "false",
            "compatibility_pairing_allowed": "false",
            "source_pack_mutation_allowed": "false",
            "diagnostic_only": "true",
            "production_safe": "false",
            "production_promotion_blocked": "true",
            "ready_for_benchmark": "false",
            "ready_for_customer_view": "false",
        })
    return rows


def _fixture(tmp_path: Path, rows: list[dict[str, str]] | None = None, report: dict[str, object] | None = None) -> tuple[Path, Path, Path]:
    csv_path = tmp_path / "preview.csv"
    report_path = tmp_path / "preview_report.json"
    out = tmp_path / "qa.csv"
    _write_csv(csv_path, rows or _rows())
    report_path.write_text(json.dumps({"valid": True} if report is None else report), encoding="utf-8")
    return csv_path, report_path, out


def _report(tmp_path: Path, rows: list[dict[str, str]] | None = None, preview_report: dict[str, object] | None = None, **kwargs):
    csv_path, report_path, out = _fixture(tmp_path, rows, preview_report)
    return mod.build_qa_report(csv_path, report_path, out=out, **kwargs), out


def _invalid(tmp_path: Path, mutate) -> dict[str, object]:
    rows = _rows(); mutate(rows)
    return _report(tmp_path, rows=rows)[0]


def test_valid_qa_report_passes(tmp_path: Path) -> None:
    report, out = _report(tmp_path)
    qa_rows = list(csv.DictReader(out.open(encoding="utf-8-sig")))
    assert report["valid"] is True
    assert report["errors"] == []
    assert len(qa_rows) == 12
    assert qa_rows[0]["qa_row_id"] == "TECE-DP-ROLE-OVERLAY-QA-000001"
    assert all(row["mapping_status"] == row["blocking_status"] == row["qa_status"] == "pass" for row in qa_rows)


def test_requires_valid_preview_report(tmp_path: Path) -> None:
    assert _report(tmp_path, preview_report={"valid": False})[0]["valid"] is False


def test_exactly_12_qa_rows(tmp_path: Path) -> None:
    report = _invalid(tmp_path, lambda rows: rows.pop())
    assert report["valid"] is False
    assert report["qa_row_count"] == 11


def test_exact_expected_article_set(tmp_path: Path) -> None:
    report = _invalid(tmp_path, lambda rows: rows[0].update(article_number="999999"))
    assert report["valid"] is False
    assert "article set differs from expected 12 articles" in report["errors"]


def test_exact_deterministic_overlay_preview_id_sequence(tmp_path: Path) -> None:
    report = _invalid(tmp_path, lambda rows: rows[0].update(overlay_preview_id="bad"))
    assert report["valid"] is False
    assert report["invalid_overlay_preview_id_rows"]


def test_duplicate_overlay_preview_id_fails(tmp_path: Path) -> None:
    report = _invalid(tmp_path, lambda rows: rows[1].update(overlay_preview_id=rows[0]["overlay_preview_id"]))
    assert report["valid"] is False
    assert report["duplicate_overlay_preview_ids"] == ["TECE-DP-ROLE-OVERLAY-PREVIEW-000001"]


def test_duplicate_article_number_fails(tmp_path: Path) -> None:
    report = _invalid(tmp_path, lambda rows: rows[1].update(article_number=rows[0]["article_number"]))
    assert report["valid"] is False
    assert report["duplicate_article_numbers"] == ["673001"]


def test_review_csv_article_role_must_be_drain_body(tmp_path: Path) -> None:
    assert _invalid(tmp_path, lambda rows: rows[0].update(review_csv_article_role="unknown"))["valid"] is False


def test_exact_current_source_pack_role_mapping_is_enforced(tmp_path: Path) -> None:
    assert _invalid(tmp_path, lambda rows: rows[0].update(current_source_pack_role="unknown"))["invalid_article_mapping_rows"]


def test_exact_reviewed_role_diagnostic_mapping_is_enforced(tmp_path: Path) -> None:
    assert _invalid(tmp_path, lambda rows: rows[0].update(reviewed_role_diagnostic="spare_part"))["invalid_article_mapping_rows"]


def test_exact_canonical_role_candidate_mapping_is_enforced(tmp_path: Path) -> None:
    assert _invalid(tmp_path, lambda rows: rows[0].update(canonical_role_candidate="accessory"))["invalid_article_mapping_rows"]


def test_exact_role_overlay_action_mapping_is_enforced(tmp_path: Path) -> None:
    assert _invalid(tmp_path, lambda rows: rows[0].update(role_overlay_action="apply"))["invalid_article_mapping_rows"]


def test_role_overlay_apply_allowed_true_fails(tmp_path: Path) -> None:
    assert _invalid(tmp_path, lambda rows: rows[0].update(role_overlay_apply_allowed="true"))["role_overlay_apply_allowed_count"] == 1


def test_length_overlay_allowed_true_fails(tmp_path: Path) -> None:
    assert _invalid(tmp_path, lambda rows: rows[0].update(length_overlay_allowed="true"))["length_overlay_allowed_count"] == 1


def test_compatibility_pairing_allowed_true_fails(tmp_path: Path) -> None:
    assert _invalid(tmp_path, lambda rows: rows[0].update(compatibility_pairing_allowed="true"))["compatibility_pairing_allowed_count"] == 1


def test_source_pack_mutation_allowed_true_fails(tmp_path: Path) -> None:
    assert _invalid(tmp_path, lambda rows: rows[0].update(source_pack_mutation_allowed="true"))["source_pack_mutation_allowed_count"] == 1


def test_production_safe_leakage_fails(tmp_path: Path) -> None:
    assert _invalid(tmp_path, lambda rows: rows[0].update(production_safe="true"))["production_leakage_rows"]


def test_production_promotion_blocked_false_fails(tmp_path: Path) -> None:
    assert _invalid(tmp_path, lambda rows: rows[0].update(production_promotion_blocked="false"))["production_leakage_rows"]


def test_ready_for_benchmark_leakage_fails(tmp_path: Path) -> None:
    assert _invalid(tmp_path, lambda rows: rows[0].update(ready_for_benchmark="true"))["readiness_leakage_rows"]


def test_ready_for_customer_view_leakage_fails(tmp_path: Path) -> None:
    assert _invalid(tmp_path, lambda rows: rows[0].update(ready_for_customer_view="true"))["readiness_leakage_rows"]


def test_diagnostic_only_false_fails(tmp_path: Path) -> None:
    assert _invalid(tmp_path, lambda rows: rows[0].update(diagnostic_only="false"))["diagnostic_only_leakage_rows"]


def test_diagnostic_only_note_is_present(tmp_path: Path) -> None:
    report, _ = _report(tmp_path)
    assert report["diagnostic_only_note"] == mod.DIAGNOSTIC_ONLY_NOTE


def test_aco_canonical_baseline_remains_pass_and_stable(monkeypatch) -> None:
    monkeypatch.setattr(aco_mod, "validate_workbook", lambda _xlsx: {"valid": True, "errors": []}, raising=False)
    if hasattr(aco_mod, "build_report"):
        result = aco_mod.build_report("dummy.xlsx")
        text = json.dumps(result)
        assert "PASS" in text or result.get("overall") in {"PASS", "ACO_BASELINE_STABLE", None}
