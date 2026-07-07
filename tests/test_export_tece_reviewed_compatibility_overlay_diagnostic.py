from __future__ import annotations

import csv
import hashlib
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from tools import report_aco_final_baseline as aco_mod
from tools.export_tece_reviewed_compatibility_overlay_diagnostic import OUTPUT_COLUMNS, _validate_overlay_rows, export_overlay_diagnostic
from tools.export_tece_reviewed_compatibility_pairs import OUTPUT_COLUMNS as PAIR_COLUMNS


def _rows() -> list[dict[str, str]]:
    pairs = [
        ("600700", "600710"), ("600700", "600711"), ("600700", "600751"), ("600700", "600770"), ("600700", "600772"), ("600700", "600782"), ("600700", "600783"), ("600700", "600785"),
        ("600701", "600710"), ("600701", "600711"), ("600701", "600751"), ("600701", "600770"), ("600701", "600772"), ("600701", "600782"), ("600701", "600783"), ("600701", "600785"),
        ("600702", "600710"), ("600702", "600711"), ("600702", "600751"), ("600702", "600770"), ("600702", "600772"), ("600702", "600782"), ("600702", "600783"), ("600702", "600785"),
        ("600703", "600710"), ("600703", "600711"), ("600703", "600751"), ("600703", "600770"), ("600703", "600772"), ("600703", "600782"), ("600703", "600783"), ("600703", "600785"),
        ("600705", "600785"), ("600707", "600785"),
    ]
    rows: list[dict[str, str]] = []
    for idx, (body, cand) in enumerate(pairs, 1):
        rows.append({
            "reviewed_pair_id": f"TECE-RP-{idx:04d}", "source_shortlist_id": f"TECE-HP-{idx:04d}",
            "family": "TECEdrainline", "pair_type": "drain_body_to_cover_or_grate", "nominal_length_mm": "700",
            "drain_body_article": body, "candidate_article": cand, "candidate_role": "cover_or_grate",
            "drain_body_source_file": "body.pdf", "candidate_source_file": "candidate.pdf",
            "drain_body_evidence_text_snippet": "body evidence", "candidate_evidence_text_snippet": "candidate evidence",
            "reviewer_pair_decision": "compatible", "reviewer_notes": "Reviewed.", "safe_to_apply_automatically": "true",
            "diagnostic_only": "true", "production_safe": "false", "production_promotion_blocked": "true",
            "ready_for_benchmark": "false", "ready_for_customer_view": "false", "production_status_note": "diagnostic-only reviewed compatibility pair; not production-safe; no Products/BOM/assembly/customer-view promotion",
        })
    return rows


def _write_csv(path: Path, rows: list[dict[str, str]]) -> None:
    with path.open("w", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=PAIR_COLUMNS, extrasaction="ignore")
        writer.writeheader(); writer.writerows(rows)


def _report(valid: bool = True, family: str = "TECEdrainline") -> dict[str, object]:
    return {
        "valid": valid, "errors": [] if valid else ["bad"], "family": family,
        "exported_pair_count": 34, "expected_exported_pair_count": 34,
        "reviewed_pairs_row_count": 34, "expected_reviewed_pairs_row_count": 34,
        "compatible_safe_source_rows_count": 34, "production_safe_candidate_count": 0,
        "production_promotion_blocked": True, "ready_for_benchmark": False, "ready_for_customer_view": False,
    }


def _fixture(tmp_path: Path, rows: list[dict[str, str]] | None = None, *, pairs_valid: bool = True, qa_valid: bool = True, family: str = "TECEdrainline") -> tuple[Path, Path, Path, Path, Path]:
    pairs = tmp_path / "pairs.csv"; pairs_report = tmp_path / "pairs.json"; qa = tmp_path / "qa.json"
    out = tmp_path / "overlay.csv"; json_out = tmp_path / "overlay.json"
    _write_csv(pairs, rows or _rows())
    pairs_report.write_text(json.dumps(_report(pairs_valid, family)), encoding="utf-8")
    qa.write_text(json.dumps(_report(qa_valid, family)), encoding="utf-8")
    return pairs, pairs_report, qa, out, json_out


def _export(tmp_path: Path, rows: list[dict[str, str]] | None = None, **kw: object) -> tuple[dict[str, object], Path]:
    pairs, pairs_report, qa, out, json_out = _fixture(tmp_path, rows, **kw)
    return export_overlay_diagnostic(pairs, pairs_report, qa, out=out, json_out=json_out), out


def test_valid_reviewed_compatibility_overlay_fixture_exports_exactly_34_rows(tmp_path: Path) -> None:
    report, out = _export(tmp_path)
    assert report["valid"] is True
    assert report["source_reviewed_pair_count"] == 34
    assert report["overlay_row_count"] == 34
    exported = list(csv.DictReader(out.open(encoding="utf-8", newline="")))
    assert len(exported) == 34
    assert list(exported[0].keys()) == OUTPUT_COLUMNS


def test_overlay_rows_preserve_reviewed_pair_id_sequence(tmp_path: Path) -> None:
    report, out = _export(tmp_path)
    assert report["invalid_reviewed_pair_ids"] == []
    assert [r["reviewed_pair_id"] for r in csv.DictReader(out.open(encoding="utf-8", newline=""))] == [f"TECE-RP-{i:04d}" for i in range(1, 35)]


def test_overlay_pair_id_sequence_is_deterministic(tmp_path: Path) -> None:
    report, out = _export(tmp_path)
    assert report["invalid_overlay_pair_ids"] == []
    assert [r["overlay_pair_id"] for r in csv.DictReader(out.open(encoding="utf-8", newline=""))] == [f"TECE-DO-{i:04d}" for i in range(1, 35)]


def test_duplicate_overlay_pair_id_fails() -> None:
    rows = [dict(r, overlay_pair_id=f"TECE-DO-{i:04d}", forbidden_downstream_use="Products Comparison BOM_Options Final_Assemblies Final_Set_Details customer_view production_generation") for i, r in enumerate(_rows(), 1)]
    rows[1]["overlay_pair_id"] = rows[0]["overlay_pair_id"]
    assert _validate_overlay_rows(rows)["duplicate_overlay_pair_ids"] == ["TECE-DO-0001"]


def test_duplicate_reviewed_pair_id_fails(tmp_path: Path) -> None:
    rows = _rows(); rows[1]["reviewed_pair_id"] = rows[0]["reviewed_pair_id"]
    report, _ = _export(tmp_path, rows)
    assert report["valid"] is False
    assert report["duplicate_reviewed_pair_ids"] == ["TECE-RP-0001"]


def test_duplicate_pair_key_fails(tmp_path: Path) -> None:
    rows = _rows(); rows[1]["drain_body_article"] = rows[0]["drain_body_article"]; rows[1]["candidate_article"] = rows[0]["candidate_article"]
    report, _ = _export(tmp_path, rows)
    assert report["valid"] is False
    assert report["duplicate_pair_keys"]


def test_ambiguous_incompatible_rows_fail_if_included(tmp_path: Path) -> None:
    rows = _rows(); rows[0]["reviewer_pair_decision"] = "ambiguous"; rows[1]["reviewer_pair_decision"] = "incompatible"
    report, _ = _export(tmp_path, rows)
    assert report["valid"] is False
    assert len(report["invalid_decision_rows"]) == 2


def test_invalid_reviewed_pairs_report_fails(tmp_path: Path) -> None:
    report, _ = _export(tmp_path, pairs_valid=False)
    assert report["valid"] is False
    assert any("reviewed pairs report is invalid" in e for e in report["errors"])


def test_invalid_qa_report_fails(tmp_path: Path) -> None:
    report, _ = _export(tmp_path, qa_valid=False)
    assert report["valid"] is False
    assert any("QA report is invalid" in e for e in report["errors"])


def test_family_mismatch_fails(tmp_path: Path) -> None:
    pairs, pairs_report, qa, out, json_out = _fixture(tmp_path, family="Other")
    report = export_overlay_diagnostic(pairs, pairs_report, qa, family="TECEdrainline", out=out, json_out=json_out)
    assert report["valid"] is False
    assert any("family mismatch" in e for e in report["errors"])


def test_production_readiness_leakage_fails(tmp_path: Path) -> None:
    rows = _rows(); rows[0]["production_safe"] = "true"; rows[1]["ready_for_benchmark"] = "true"; rows[2]["ready_for_customer_view"] = "true"
    report, _ = _export(tmp_path, rows)
    assert report["valid"] is False
    assert report["production_leakage_rows"]
    assert report["readiness_leakage_rows"]


def test_missing_forbidden_downstream_use_fails() -> None:
    rows = [dict(r, overlay_pair_id=f"TECE-DO-{i:04d}", forbidden_downstream_use="Products") for i, r in enumerate(_rows(), 1)]
    assert _validate_overlay_rows(rows)["forbidden_downstream_use_missing_rows"]


def test_blocked_articles_fail(tmp_path: Path) -> None:
    rows = _rows(); rows[0]["candidate_article"] = "650700"
    report, _ = _export(tmp_path, rows)
    assert report["valid"] is False
    assert report["blocked_articles_present"] == ["650700"]


def test_source_pack_immutability(tmp_path: Path) -> None:
    source_pack = Path("tests/fixtures/tece/source_pack")
    before = {p: hashlib.sha256(p.read_bytes()).hexdigest() for p in source_pack.iterdir() if p.is_file()}
    _export(tmp_path)
    after = {p: hashlib.sha256(p.read_bytes()).hexdigest() for p in source_pack.iterdir() if p.is_file()}
    assert after == before


def test_aco_canonical_baseline_remains_pass_and_stable() -> None:
    report = aco_mod.AuditReport((aco_mod.AuditCheck("baseline_guard", True, "unchanged"),), {}, {}, {})
    text = aco_mod.format_report(Path("canonical.xlsx"), report)
    assert "- PASS baseline_guard" in text
    assert "OVERALL: ACO_BASELINE_STABLE" in text
