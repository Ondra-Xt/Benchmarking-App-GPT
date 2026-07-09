from __future__ import annotations

import csv
import json
from pathlib import Path

from tools.report_tece_drainprofile_reviewed_role_overlay_preview_qa import (
    EXPECTED_ARTICLES,
    EXPECTED_CANONICAL_ROLE_CANDIDATE,
    EXPECTED_CURRENT_SOURCE_PACK_ROLE,
    EXPECTED_REVIEWED_ROLE_DIAGNOSTIC,
    EXPECTED_ROLE_OVERLAY_ACTION,
)
from tools.validate_tece_drainprofile_explicit_role_overlay_csv import (
    DIAGNOSTIC_ONLY_NOTE,
    EXPECTED_EVIDENCE_CHAIN_STATUS,
    EXPECTED_OVERLAY_SCOPE,
    EXPECTED_OVERLAY_IDS,
    PRODUCTION_STATUS_NOTE,
    RECOMMENDED_NEXT_ACTION,
    REQUIRED_COLUMNS,
    REQUIRED_EVIDENCE_TOKENS,
    validate_overlay_csv,
)


def _write_csv(path: Path, rows: list[dict[str, str]], columns: list[str] | None = None) -> None:
    cols = columns or REQUIRED_COLUMNS
    with path.open("w", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=cols)
        writer.writeheader()
        writer.writerows([{c: row.get(c, "") for c in cols} for row in rows])


def _overlay_rows() -> list[dict[str, str]]:
    evidence = ";".join(REQUIRED_EVIDENCE_TOKENS)
    rows = []
    for index, article in enumerate(EXPECTED_ARTICLES, start=1):
        rows.append({
            "overlay_id": f"TECE-DP-EXPLICIT-ROLE-OVERLAY-{index:06d}",
            "family": "TECEdrainprofile",
            "article_number": article,
            "current_source_pack_role": EXPECTED_CURRENT_SOURCE_PACK_ROLE[article],
            "reviewed_role_diagnostic": EXPECTED_REVIEWED_ROLE_DIAGNOSTIC[article],
            "canonical_role_candidate": EXPECTED_CANONICAL_ROLE_CANDIDATE[article],
            "overlay_action": EXPECTED_ROLE_OVERLAY_ACTION[article],
            "overlay_scope": EXPECTED_OVERLAY_SCOPE,
            "evidence_chain_ids": evidence,
            "evidence_chain_status": EXPECTED_EVIDENCE_CHAIN_STATUS,
            "role_overlay_apply_allowed": "false",
            "source_pack_mutation_allowed": "false",
            "extraction_logic_change_allowed": "false",
            "length_overlay_allowed": "false",
            "compatibility_pairing_allowed": "false",
            "production_promotion_allowed": "false",
            "benchmark_ready_allowed": "false",
            "customer_view_allowed": "false",
            "diagnostic_only": "true",
            "production_safe": "false",
            "production_promotion_blocked": "true",
            "ready_for_benchmark": "false",
            "ready_for_customer_view": "false",
            "recommended_next_action": RECOMMENDED_NEXT_ACTION,
            "production_status_note": PRODUCTION_STATUS_NOTE,
        })
    return rows


def _fixture(tmp_path: Path) -> tuple[Path, Path, Path, Path, Path]:
    overlay = tmp_path / "overlay.csv"
    design_report = tmp_path / "design_report.json"
    design_csv = tmp_path / "design.csv"
    qa_report = tmp_path / "qa_report.json"
    qa_csv = tmp_path / "qa.csv"
    _write_csv(overlay, _overlay_rows())
    _write_csv(design_csv, [{"design_rule_id": f"rule-{i}"} for i in range(12)], ["design_rule_id"])
    _write_csv(qa_csv, [{"article_number": a} for a in EXPECTED_ARTICLES], ["article_number"])
    design_report.write_text(json.dumps({"valid": True, "design_rule_count": 12}), encoding="utf-8")
    qa_report.write_text(json.dumps({"valid": True, "qa_row_count": 12}), encoding="utf-8")
    return overlay, design_report, design_csv, qa_report, qa_csv


def _validate(tmp_path: Path):
    return validate_overlay_csv(*_fixture(tmp_path))


def test_valid_overlay_csv_passes(tmp_path):
    result = _validate(tmp_path)
    assert result["valid"] is True
    assert result["errors"] == []
    assert result["overlay_row_count"] == 12
    assert result["article_numbers"] == EXPECTED_ARTICLES
    assert result["current_source_pack_role_counts"] == {"drain_body": 3, "unknown": 9}
    assert result["reviewed_role_diagnostic_counts"] == {"accessory": 1, "drain_body_unresolved_length": 3, "profile_cover": 1, "spare_part": 4, "water_trap": 3}
    assert result["canonical_role_candidate_counts"] == {"accessory": 8, "drain_body": 3, "profile_cover": 1}


def test_requires_valid_design_report(tmp_path):
    overlay, design_report, design_csv, qa_report, qa_csv = _fixture(tmp_path)
    design_report.write_text(json.dumps({"valid": False, "design_rule_count": 12}), encoding="utf-8")
    result = validate_overlay_csv(overlay, design_report, design_csv, qa_report, qa_csv)
    assert result["valid"] is False
    assert "design report valid must be true" in result["errors"]


def test_requires_valid_qa_report(tmp_path):
    overlay, design_report, design_csv, qa_report, qa_csv = _fixture(tmp_path)
    qa_report.write_text(json.dumps({"valid": False, "qa_row_count": 12}), encoding="utf-8")
    result = validate_overlay_csv(overlay, design_report, design_csv, qa_report, qa_csv)
    assert result["valid"] is False
    assert "QA report valid must be true" in result["errors"]


def test_required_columns_are_enforced(tmp_path):
    overlay, design_report, design_csv, qa_report, qa_csv = _fixture(tmp_path)
    columns = [c for c in REQUIRED_COLUMNS if c != "overlay_action"]
    _write_csv(overlay, _overlay_rows(), columns)
    result = validate_overlay_csv(overlay, design_report, design_csv, qa_report, qa_csv)
    assert result["valid"] is False
    assert result["required_columns_missing"] == ["overlay_action"]


def test_exactly_12_overlay_rows_are_enforced(tmp_path):
    overlay, design_report, design_csv, qa_report, qa_csv = _fixture(tmp_path)
    _write_csv(overlay, _overlay_rows()[:-1])
    result = validate_overlay_csv(overlay, design_report, design_csv, qa_report, qa_csv)
    assert result["valid"] is False
    assert "overlay row count must be 12" in result["errors"]


def test_exact_article_set_is_enforced(tmp_path):
    overlay, design_report, design_csv, qa_report, qa_csv = _fixture(tmp_path)
    rows = _overlay_rows(); rows[-1]["article_number"] = "999999"
    _write_csv(overlay, rows)
    result = validate_overlay_csv(overlay, design_report, design_csv, qa_report, qa_csv)
    assert result["valid"] is False
    assert "article set differs from expected 12 articles" in result["errors"]


def test_deterministic_overlay_id_sequence_is_enforced(tmp_path):
    overlay, design_report, design_csv, qa_report, qa_csv = _fixture(tmp_path)
    rows = _overlay_rows(); rows[0]["overlay_id"] = "bad"
    _write_csv(overlay, rows)
    result = validate_overlay_csv(overlay, design_report, design_csv, qa_report, qa_csv)
    assert result["valid"] is False
    assert result["invalid_overlay_id_rows"]
    assert "overlay_id sequence differs from expected deterministic IDs" in result["errors"]


def test_duplicate_overlay_id_fails(tmp_path):
    overlay, design_report, design_csv, qa_report, qa_csv = _fixture(tmp_path)
    rows = _overlay_rows(); rows[1]["overlay_id"] = rows[0]["overlay_id"]
    _write_csv(overlay, rows)
    result = validate_overlay_csv(overlay, design_report, design_csv, qa_report, qa_csv)
    assert result["valid"] is False
    assert result["duplicate_overlay_ids"] == [EXPECTED_OVERLAY_IDS[0]]


def test_duplicate_article_number_fails(tmp_path):
    overlay, design_report, design_csv, qa_report, qa_csv = _fixture(tmp_path)
    rows = _overlay_rows(); rows[1]["article_number"] = rows[0]["article_number"]
    _write_csv(overlay, rows)
    result = validate_overlay_csv(overlay, design_report, design_csv, qa_report, qa_csv)
    assert result["valid"] is False
    assert result["duplicate_article_numbers"] == [EXPECTED_ARTICLES[0]]


def test_exact_current_source_pack_role_mapping_is_enforced(tmp_path):
    overlay, design_report, design_csv, qa_report, qa_csv = _fixture(tmp_path)
    rows = _overlay_rows(); rows[0]["current_source_pack_role"] = "unknown"
    _write_csv(overlay, rows)
    result = validate_overlay_csv(overlay, design_report, design_csv, qa_report, qa_csv)
    assert result["valid"] is False
    assert result["invalid_article_mapping_rows"]


def test_exact_reviewed_role_diagnostic_mapping_is_enforced(tmp_path):
    overlay, design_report, design_csv, qa_report, qa_csv = _fixture(tmp_path)
    rows = _overlay_rows(); rows[3]["reviewed_role_diagnostic"] = "accessory"
    _write_csv(overlay, rows)
    result = validate_overlay_csv(overlay, design_report, design_csv, qa_report, qa_csv)
    assert result["valid"] is False
    assert result["invalid_article_mapping_rows"]


def test_exact_canonical_role_candidate_mapping_is_enforced(tmp_path):
    overlay, design_report, design_csv, qa_report, qa_csv = _fixture(tmp_path)
    rows = _overlay_rows(); rows[3]["canonical_role_candidate"] = "drain_body"
    _write_csv(overlay, rows)
    result = validate_overlay_csv(overlay, design_report, design_csv, qa_report, qa_csv)
    assert result["valid"] is False
    assert result["invalid_article_mapping_rows"]


def test_exact_overlay_action_mapping_is_enforced(tmp_path):
    overlay, design_report, design_csv, qa_report, qa_csv = _fixture(tmp_path)
    rows = _overlay_rows(); rows[3]["overlay_action"] = "bad"
    _write_csv(overlay, rows)
    result = validate_overlay_csv(overlay, design_report, design_csv, qa_report, qa_csv)
    assert result["valid"] is False
    assert result["invalid_article_mapping_rows"]


def test_overlay_scope_mismatch_fails(tmp_path):
    overlay, design_report, design_csv, qa_report, qa_csv = _fixture(tmp_path)
    rows = _overlay_rows(); rows[0]["overlay_scope"] = "bad"
    _write_csv(overlay, rows)
    result = validate_overlay_csv(overlay, design_report, design_csv, qa_report, qa_csv)
    assert result["valid"] is False
    assert result["invalid_overlay_scope_rows"]


def test_evidence_chain_ids_missing_required_token_fails(tmp_path):
    overlay, design_report, design_csv, qa_report, qa_csv = _fixture(tmp_path)
    rows = _overlay_rows(); rows[0]["evidence_chain_ids"] = rows[0]["evidence_chain_ids"].replace("manual_review_csv", "")
    _write_csv(overlay, rows)
    result = validate_overlay_csv(overlay, design_report, design_csv, qa_report, qa_csv)
    assert result["valid"] is False
    assert result["invalid_evidence_chain_rows"]


def test_evidence_chain_status_mismatch_fails(tmp_path):
    overlay, design_report, design_csv, qa_report, qa_csv = _fixture(tmp_path)
    rows = _overlay_rows(); rows[0]["evidence_chain_status"] = "bad"
    _write_csv(overlay, rows)
    result = validate_overlay_csv(overlay, design_report, design_csv, qa_report, qa_csv)
    assert result["valid"] is False
    assert result["invalid_evidence_chain_rows"]


import pytest


@pytest.mark.parametrize("field", [
    "role_overlay_apply_allowed", "source_pack_mutation_allowed", "extraction_logic_change_allowed", "length_overlay_allowed",
    "compatibility_pairing_allowed", "production_promotion_allowed", "benchmark_ready_allowed", "customer_view_allowed", "production_safe",
])
def test_true_blocking_and_production_flags_fail(tmp_path, field):
    overlay, design_report, design_csv, qa_report, qa_csv = _fixture(tmp_path)
    rows = _overlay_rows(); rows[0][field] = "true"
    _write_csv(overlay, rows)
    result = validate_overlay_csv(overlay, design_report, design_csv, qa_report, qa_csv)
    assert result["valid"] is False
    assert result["invalid_blocking_rows"]


@pytest.mark.parametrize("field", ["production_promotion_blocked", "diagnostic_only"])
def test_required_true_flags_false_fail(tmp_path, field):
    overlay, design_report, design_csv, qa_report, qa_csv = _fixture(tmp_path)
    rows = _overlay_rows(); rows[0][field] = "false"
    _write_csv(overlay, rows)
    result = validate_overlay_csv(overlay, design_report, design_csv, qa_report, qa_csv)
    assert result["valid"] is False
    assert result["invalid_blocking_rows"]


@pytest.mark.parametrize("field", ["ready_for_benchmark", "ready_for_customer_view"])
def test_readiness_true_flags_fail(tmp_path, field):
    overlay, design_report, design_csv, qa_report, qa_csv = _fixture(tmp_path)
    rows = _overlay_rows(); rows[0][field] = "true"
    _write_csv(overlay, rows)
    result = validate_overlay_csv(overlay, design_report, design_csv, qa_report, qa_csv)
    assert result["valid"] is False
    assert result["readiness_leakage_rows"]


def test_diagnostic_only_note_is_present(tmp_path):
    result = _validate(tmp_path)
    assert result["diagnostic_only_note"] == DIAGNOSTIC_ONLY_NOTE


def test_aco_canonical_baseline_contract_remains_stable():
    # This validator is read-only and does not import or mutate ACO export logic.
    path = Path("tools/report_aco_final_baseline.py")
    text = path.read_text(encoding="utf-8")
    assert "OVERALL" in text
    assert "ACO_BASELINE_STABLE" in text
