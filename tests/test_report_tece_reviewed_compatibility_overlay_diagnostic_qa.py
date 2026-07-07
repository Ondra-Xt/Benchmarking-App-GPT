from __future__ import annotations

import csv
import hashlib
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from tools import report_aco_final_baseline as aco_mod
from tools.export_tece_reviewed_compatibility_overlay_diagnostic import OUTPUT_COLUMNS as OVERLAY_COLUMNS
from tools.export_tece_reviewed_compatibility_overlay_diagnostic import _overlay_row
from tools.export_tece_reviewed_compatibility_pairs import OUTPUT_COLUMNS as PAIR_COLUMNS
from tools.report_tece_reviewed_compatibility_overlay_diagnostic_qa import report_overlay_diagnostic_qa
from tests.test_export_tece_reviewed_compatibility_overlay_diagnostic import _rows


def _write_csv(path: Path, rows: list[dict[str, str]], columns: list[str]) -> None:
    with path.open("w", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=columns, extrasaction="ignore")
        writer.writeheader(); writer.writerows(rows)


def _report(valid: bool = True, family: str = "TECEdrainline") -> dict[str, object]:
    return {
        "valid": valid, "errors": [] if valid else ["bad"], "family": family,
        "overlay_row_count": 34, "expected_overlay_row_count": 34, "source_reviewed_pair_count": 34,
        "exported_pair_count": 34, "expected_exported_pair_count": 34,
        "reviewed_pairs_row_count": 34, "expected_reviewed_pairs_row_count": 34,
        "compatible_safe_source_rows_count": 34, "production_safe_candidate_count": 0,
        "production_promotion_blocked": True, "ready_for_benchmark": False, "ready_for_customer_view": False,
    }


def _fixture(tmp_path: Path, *, overlay_rows: list[dict[str, str]] | None = None, reviewed_rows: list[dict[str, str]] | None = None, overlay_valid: bool = True, pairs_valid: bool = True, qa_valid: bool = True, family: str = "TECEdrainline") -> dict[str, Path]:
    reviewed = reviewed_rows or _rows()
    overlay = overlay_rows or [_overlay_row(i, row) for i, row in enumerate(reviewed, 1)]
    paths = {name: tmp_path / name for name in ["overlay.csv", "overlay.json", "pairs.csv", "pairs.json", "qa.json", "out.json"]}
    _write_csv(paths["overlay.csv"], overlay, OVERLAY_COLUMNS)
    _write_csv(paths["pairs.csv"], reviewed, PAIR_COLUMNS)
    paths["overlay.json"].write_text(json.dumps(_report(overlay_valid, family)), encoding="utf-8")
    paths["pairs.json"].write_text(json.dumps(_report(pairs_valid, family)), encoding="utf-8")
    paths["qa.json"].write_text(json.dumps(_report(qa_valid, family)), encoding="utf-8")
    return paths


def _qa(tmp_path: Path, **kw: object) -> dict[str, object]:
    p = _fixture(tmp_path, **kw)
    return report_overlay_diagnostic_qa(p["overlay.csv"], p["overlay.json"], p["pairs.csv"], p["pairs.json"], p["qa.json"], out=p["out.json"])


def test_valid_overlay_qa_fixture_passes(tmp_path: Path) -> None:
    report = _qa(tmp_path)
    assert report["valid"] is True
    assert report["overlay_row_count"] == 34
    assert report["reviewed_pairs_row_count"] == 34
    assert report["source_reviewed_pair_count"] == 34
    assert report["production_safe_candidate_count"] == 0


def test_overlay_row_count_mismatch_fails(tmp_path: Path) -> None:
    report = _qa(tmp_path, overlay_rows=[_overlay_row(i, r) for i, r in enumerate(_rows()[:-1], 1)])
    assert report["valid"] is False
    assert any("overlay CSV row count" in e for e in report["errors"])


def test_reviewed_pairs_row_count_mismatch_fails(tmp_path: Path) -> None:
    report = _qa(tmp_path, reviewed_rows=_rows()[:-1], overlay_rows=[_overlay_row(i, r) for i, r in enumerate(_rows()[:-1], 1)])
    assert report["valid"] is False
    assert any("reviewed pairs CSV row count" in e for e in report["errors"])


def test_missing_reviewed_pair_in_overlay_fails(tmp_path: Path) -> None:
    overlay = [_overlay_row(i, r) for i, r in enumerate(_rows(), 1)]
    overlay.pop(0); overlay.append(dict(overlay[-1]))
    report = _qa(tmp_path, overlay_rows=overlay)
    assert report["valid"] is False
    assert report["reviewed_pairs_missing_overlay_row"]


def test_unexpected_overlay_row_fails(tmp_path: Path) -> None:
    overlay = [_overlay_row(i, r) for i, r in enumerate(_rows(), 1)]
    overlay[0]["candidate_article"] = "999999"
    report = _qa(tmp_path, overlay_rows=overlay)
    assert report["valid"] is False
    assert report["overlay_rows_missing_reviewed_pair"]


def test_overlay_reviewed_pair_mismatch_fails(tmp_path: Path) -> None:
    overlay = [_overlay_row(i, r) for i, r in enumerate(_rows(), 1)]
    overlay[0]["source_shortlist_id"] = "TECE-HP-9999"
    report = _qa(tmp_path, overlay_rows=overlay)
    assert report["valid"] is False
    assert report["overlay_reviewed_pair_mismatch_rows"]


def test_broken_overlay_pair_id_sequence_fails(tmp_path: Path) -> None:
    overlay = [_overlay_row(i, r) for i, r in enumerate(_rows(), 1)]; overlay[0]["overlay_pair_id"] = "TECE-DO-9999"
    assert _qa(tmp_path, overlay_rows=overlay)["invalid_overlay_pair_ids"]


def test_broken_reviewed_pair_id_sequence_fails(tmp_path: Path) -> None:
    reviewed = _rows(); reviewed[0]["reviewed_pair_id"] = "TECE-RP-9999"
    overlay = [_overlay_row(i, r) for i, r in enumerate(reviewed, 1)]
    assert _qa(tmp_path, reviewed_rows=reviewed, overlay_rows=overlay)["invalid_reviewed_pair_ids"]


def test_duplicate_overlay_pair_id_fails(tmp_path: Path) -> None:
    overlay = [_overlay_row(i, r) for i, r in enumerate(_rows(), 1)]; overlay[1]["overlay_pair_id"] = overlay[0]["overlay_pair_id"]
    assert _qa(tmp_path, overlay_rows=overlay)["duplicate_overlay_pair_ids"] == ["TECE-DO-0001"]


def test_duplicate_reviewed_pair_id_fails(tmp_path: Path) -> None:
    reviewed = _rows(); reviewed[1]["reviewed_pair_id"] = reviewed[0]["reviewed_pair_id"]
    overlay = [_overlay_row(i, r) for i, r in enumerate(reviewed, 1)]
    assert _qa(tmp_path, reviewed_rows=reviewed, overlay_rows=overlay)["duplicate_reviewed_pair_ids"]


def test_duplicate_pair_key_fails(tmp_path: Path) -> None:
    overlay = [_overlay_row(i, r) for i, r in enumerate(_rows(), 1)]
    overlay[1]["drain_body_article"] = overlay[0]["drain_body_article"]; overlay[1]["candidate_article"] = overlay[0]["candidate_article"]
    assert _qa(tmp_path, overlay_rows=overlay)["duplicate_pair_keys"]


def test_blocked_articles_fail(tmp_path: Path) -> None:
    overlay = [_overlay_row(i, r) for i, r in enumerate(_rows(), 1)]; overlay[0]["candidate_article"] = "650700"
    assert _qa(tmp_path, overlay_rows=overlay)["blocked_articles_present"] == ["650700"]


def test_production_readiness_leakage_fails(tmp_path: Path) -> None:
    overlay = [_overlay_row(i, r) for i, r in enumerate(_rows(), 1)]
    overlay[0]["production_safe"] = "true"; overlay[1]["ready_for_benchmark"] = "true"; overlay[2]["ready_for_customer_view"] = "true"
    report = _qa(tmp_path, overlay_rows=overlay)
    assert report["production_leakage_rows"] and report["readiness_leakage_rows"]


def test_missing_forbidden_downstream_use_blocker_fails(tmp_path: Path) -> None:
    overlay = [_overlay_row(i, r) for i, r in enumerate(_rows(), 1)]; overlay[0]["forbidden_downstream_use"] = "Products"
    assert _qa(tmp_path, overlay_rows=overlay)["forbidden_downstream_use_missing_rows"]


def test_invalid_overlay_report_fails(tmp_path: Path) -> None:
    assert any("overlay report is invalid" in e for e in _qa(tmp_path, overlay_valid=False)["errors"])


def test_invalid_reviewed_pairs_report_fails(tmp_path: Path) -> None:
    assert any("reviewed pairs report is invalid" in e for e in _qa(tmp_path, pairs_valid=False)["errors"])


def test_invalid_reviewed_pairs_qa_report_fails(tmp_path: Path) -> None:
    assert any("reviewed pairs QA report is invalid" in e for e in _qa(tmp_path, qa_valid=False)["errors"])


def test_family_mismatch_fails(tmp_path: Path) -> None:
    assert any("family mismatch" in e for e in _qa(tmp_path, family="Other")["errors"])


def test_source_pack_immutability(tmp_path: Path) -> None:
    source_pack = Path("tests/fixtures/tece/source_pack")
    before = {p: hashlib.sha256(p.read_bytes()).hexdigest() for p in source_pack.iterdir() if p.is_file()}
    _qa(tmp_path)
    after = {p: hashlib.sha256(p.read_bytes()).hexdigest() for p in source_pack.iterdir() if p.is_file()}
    assert after == before


def test_aco_canonical_baseline_remains_pass_and_stable() -> None:
    report = aco_mod.AuditReport((aco_mod.AuditCheck("baseline_guard", True, "unchanged"),), {}, {}, {})
    text = aco_mod.format_report(Path("canonical.xlsx"), report)
    assert "- PASS baseline_guard" in text
    assert "OVERALL: ACO_BASELINE_STABLE" in text
