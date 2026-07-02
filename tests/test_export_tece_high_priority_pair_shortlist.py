from __future__ import annotations

import csv
import hashlib
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from tools import export_tece_high_priority_pair_shortlist as mod
from tools import report_aco_final_baseline as aco_mod


def _write_csv(path: Path, rows: list[dict[str, object]]) -> None:
    fields = [
        "pair_type", "diagnostic_review_priority", "status", "drain_body_article", "candidate_article",
        "drain_body_nominal_length_mm", "candidate_nominal_length_mm", "candidate_role", "drain_body_source_file",
        "candidate_source_file", "drain_body_evidence_text_snippet", "candidate_evidence_text_snippet",
        "diagnostic_only", "production_safe", "production_promotion_blocked", "ready_for_benchmark",
        "ready_for_customer_view",
    ]
    with path.open("w", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=fields)
        writer.writeheader(); writer.writerows(rows)


def _valid_report(path: Path) -> None:
    path.write_text(json.dumps({"valid": True, "errors": [], "production_safe_candidate_count": 0}), encoding="utf-8")


def _row(body: str, cand: str, length: int = 800, **kw: object) -> dict[str, object]:
    row = {
        "pair_type": "drain_body_to_cover_or_grate", "diagnostic_review_priority": "high_review_priority",
        "status": "diagnostic_exact_length_match", "drain_body_article": body, "candidate_article": cand,
        "drain_body_nominal_length_mm": length, "candidate_nominal_length_mm": length, "candidate_role": "cover_or_grate",
        "drain_body_source_file": "other.pdf", "candidate_source_file": "other.pdf",
        "drain_body_evidence_text_snippet": f"body {body}", "candidate_evidence_text_snippet": f"candidate {cand}",
        "diagnostic_only": "true", "production_safe": "false", "production_promotion_blocked": "true",
        "ready_for_benchmark": "false", "ready_for_customer_view": "false",
    }
    row.update(kw)
    return row


def test_valid_shortlist_fixture_and_manual_fields_blank(tmp_path: Path) -> None:
    csv_path = tmp_path / "audit.csv"; report = tmp_path / "audit.json"; out = tmp_path / "short.csv"; jout = tmp_path / "short.json"
    _write_csv(csv_path, [_row("600001", "700001"), _row("600001", "700002")]); _valid_report(report)
    summary = mod.build_shortlist(csv_path, report, out=out, json_out=jout)
    rows = list(csv.DictReader(out.open(encoding="utf-8")))
    assert summary["valid"] is True
    assert summary["exported_shortlist_rows"] == 2
    assert all(r["reviewer_pair_decision"] == r["reviewer_notes"] == r["safe_to_apply_automatically"] == "" for r in rows)
    assert json.loads(jout.read_text(encoding="utf-8"))["production_promotion_blocked"] is True


def test_invalid_pair_audit_report_fails_without_successful_shortlist(tmp_path: Path) -> None:
    csv_path = tmp_path / "audit.csv"; report = tmp_path / "bad.json"; out = tmp_path / "short.csv"
    _write_csv(csv_path, [_row("600001", "700001")]); report.write_text("{bad", encoding="utf-8")
    summary = mod.build_shortlist(csv_path, report, out=out)
    assert summary["valid"] is False
    assert summary["exported_shortlist_rows"] == 0
    assert not out.exists()


def test_only_high_priority_cover_grate_exact_length_exported_and_exclusions(tmp_path: Path) -> None:
    csv_path = tmp_path / "audit.csv"; report = tmp_path / "audit.json"; out = tmp_path / "short.csv"
    rows = [
        _row("600001", "700001"),
        _row("600002", "700002", diagnostic_review_priority="medium_review_priority"),
        _row("600003", "700003", pair_type="drain_body_to_accessory"),
        _row("600004", "650700"),
        _row("600005", "700005", status="unresolved_length_missing", candidate_nominal_length_mm=""),
        _row("600006", "700006", candidate_nominal_length_mm=900),
    ]
    _write_csv(csv_path, rows); _valid_report(report)
    mod.build_shortlist(csv_path, report, out=out)
    exported = list(csv.DictReader(out.open(encoding="utf-8")))
    assert [(r["drain_body_article"], r["candidate_article"]) for r in exported] == [("600001", "700001")]


def test_duplicate_pair_rows_deduplicate_by_evidence_priority(tmp_path: Path) -> None:
    csv_path = tmp_path / "audit.csv"; report = tmp_path / "audit.json"; out = tmp_path / "short.csv"
    rows = [
        _row("600001", "700001", candidate_source_file="other.pdf"),
        _row("600001", "700001", candidate_source_file=mod.SORTIMENTSLISTE, candidate_evidence_text_snippet="best"),
    ]
    _write_csv(csv_path, rows); _valid_report(report)
    mod.build_shortlist(csv_path, report, out=out)
    exported = list(csv.DictReader(out.open(encoding="utf-8")))
    assert len(exported) == 1
    assert exported[0]["candidate_source_file"] == mod.SORTIMENTSLISTE


def test_caps_work_deterministically_and_flags_stay_blocked(tmp_path: Path) -> None:
    csv_path = tmp_path / "audit.csv"; report = tmp_path / "audit.json"; out = tmp_path / "short.csv"
    rows = [_row("600001", f"70000{i}", 700) for i in range(5)] + [_row("600002", f"80000{i}", 800) for i in range(5)]
    _write_csv(csv_path, rows); _valid_report(report)
    summary = mod.build_shortlist(csv_path, report, out=out, max_rows=3, max_pairs_per_drain_body=2, max_pairs_per_length=2)
    exported = list(csv.DictReader(out.open(encoding="utf-8")))
    assert [r["candidate_article"] for r in exported] == ["700000", "700001", "800000"]
    assert summary["skipped_by_caps_count"] == 7
    assert all(r["production_safe"] == "false" and r["ready_for_benchmark"] == "false" and r["ready_for_customer_view"] == "false" for r in exported)


def test_source_pack_immutability(tmp_path: Path) -> None:
    source_pack = Path("tests/fixtures/tece/source_pack")
    before = {p: hashlib.sha256(p.read_bytes()).hexdigest() for p in source_pack.iterdir() if p.is_file()}
    csv_path = tmp_path / "audit.csv"; report = tmp_path / "audit.json"; out = tmp_path / "short.csv"
    _write_csv(csv_path, [_row("600001", "700001")]); _valid_report(report)
    mod.build_shortlist(csv_path, report, out=out)
    after = {p: hashlib.sha256(p.read_bytes()).hexdigest() for p in source_pack.iterdir() if p.is_file()}
    assert after == before


def test_aco_canonical_baseline_remains_stable() -> None:
    report = aco_mod.AuditReport((aco_mod.AuditCheck("baseline_guard", True, "unchanged"),), {}, {}, {})
    text = aco_mod.format_report(Path("canonical.xlsx"), report)
    assert "OVERALL: ACO_BASELINE_STABLE" in text
