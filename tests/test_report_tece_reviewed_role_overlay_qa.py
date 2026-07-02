from __future__ import annotations

import csv
import hashlib
import json
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from tools import report_tece_reviewed_role_overlay_qa as mod
from tools import report_aco_final_baseline as aco_mod
from tests.test_report_aco_final_baseline import _canonical_sheets


def _hash(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _write_fixture(tmp_path: Path, *, missing_blocked: str | None = None, report_updates: dict[str, object] | None = None) -> tuple[Path, Path]:
    csv_path = tmp_path / "preview.csv"
    report_path = tmp_path / "preview_report.json"
    rows: list[dict[str, str]] = []

    def add(article: str, original: str, applied: str, status: str, source: str = "tece_catalog.csv") -> None:
        rows.append({
            "article_number": article,
            "product_family": "TECEdrainline",
            "original_article_role": original,
            "applied_article_role": applied,
            "review_apply_status": status,
            "source_file": source,
        })

    # Existing non-unknown inventory roles.
    for role, count in {"accessory": 21, "complete_set": 4, "cover_or_grate": 58, "drain_body": 7}.items():
        for i in range(count):
            add(f"{role}-{i}", role, role, "not_reviewed")

    # Safe reviewed unknown rows: +19 accessory, +65 cover/grate, +49 drain body = 133.
    n = 0
    for role, count in {"accessory": 19, "cover_or_grate": 65, "drain_body": 49}.items():
        for i in range(count):
            add(f"applied-{n:03d}", "unknown", role, "applied", source=f"evidence_{role}.csv")
            n += 1

    blocked_articles = ["650700", "650800", "651500"]
    for article in blocked_articles:
        if article != missing_blocked:
            add(article, "unknown", "unknown", "skipped_blocked", source="natural_stone.csv")
    if missing_blocked:
        add("650900", "unknown", "unknown", "skipped_blocked", source="natural_stone.csv")

    for i in range(436 - len(rows)):
        rows.append({
            "article_number": f"other-{i}",
            "product_family": "OtherFamily",
            "original_article_role": "unknown",
            "applied_article_role": "unknown",
            "review_apply_status": "not_reviewed",
            "source_file": "other.csv",
        })

    with csv_path.open("w", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)

    report = {
        "valid": True,
        "total_review_rows": 136,
        "safe_to_apply_true_count": 133,
        "blocked_count": 3,
        "applied_count": 133,
        "skipped_blocked_count": 3,
        "skipped_duplicate_match_count": 0,
        "unknown_reduction": 133,
        "current_inventory_role_counts": {
            "accessory": 21,
            "complete_set": 4,
            "cover_or_grate": 58,
            "drain_body": 7,
            "unknown": 136,
        },
        "preview_role_counts_after_safe_apply": {
            "accessory": 40,
            "complete_set": 4,
            "cover_or_grate": 123,
            "drain_body": 56,
            "unknown": 3,
        },
    }
    if report_updates:
        report.update(report_updates)
    report_path.write_text(json.dumps(report, indent=2), encoding="utf-8")
    return csv_path, report_path


def test_valid_real_style_overlay_qa_fixture(tmp_path: Path) -> None:
    csv_path, report_path = _write_fixture(tmp_path)

    report = mod.build_qa_report(csv_path, report_path)

    assert report["valid"] is True
    assert report["total_preview_rows"] == 436
    assert report["applied_count"] == 133
    assert report["skipped_blocked_count"] == 3
    assert report["skipped_duplicate_match_count"] == 0
    assert report["unknown_reduction"] == 133
    assert report["preview_role_counts_after_safe_apply"] == {
        "accessory": 40,
        "complete_set": 4,
        "cover_or_grate": 123,
        "drain_body": 56,
        "unknown": 3,
    }
    assert {row["article_number"] for row in report["blocked_rows"]} == {"650700", "650800", "651500"}


def test_mismatch_between_preview_csv_and_report_fails(tmp_path: Path) -> None:
    csv_path, report_path = _write_fixture(tmp_path, report_updates={"applied_count": 132})

    report = mod.build_qa_report(csv_path, report_path)

    assert report["valid"] is False
    assert any("applied_count differs" in error for error in report["errors"])


def test_missing_blocked_natural_stone_row_fails(tmp_path: Path) -> None:
    csv_path, report_path = _write_fixture(tmp_path, missing_blocked="650700")

    report = mod.build_qa_report(csv_path, report_path)

    assert report["valid"] is False
    assert any("blocked rows must be exactly" in error for error in report["errors"])


def test_nonzero_duplicate_match_count_fails(tmp_path: Path) -> None:
    csv_path, report_path = _write_fixture(tmp_path, report_updates={"skipped_duplicate_match_count": 1})

    report = mod.build_qa_report(csv_path, report_path)

    assert report["valid"] is False
    assert any("skipped_duplicate_match_count differs" in error for error in report["errors"])


def test_production_and_readiness_flags_remain_blocked_false(tmp_path: Path) -> None:
    csv_path, report_path = _write_fixture(tmp_path)

    report = mod.build_qa_report(csv_path, report_path)

    assert report["production_promotion_blocked"] is True
    assert report["ready_for_benchmark"] is False
    assert report["ready_for_customer_view"] is False


def test_source_pack_immutability(tmp_path: Path) -> None:
    csv_path, report_path = _write_fixture(tmp_path)
    before_csv = _hash(csv_path)
    before_report = _hash(report_path)

    exit_code = mod.main(["--preview-csv", str(csv_path), "--preview-report", str(report_path), "--out", str(tmp_path / "qa.json")])

    assert exit_code == 0
    assert _hash(csv_path) == before_csv
    assert _hash(report_path) == before_report


def test_aco_canonical_export_remains_pass_and_baseline_stable() -> None:
    report = aco_mod.audit_frames(_canonical_sheets())

    assert report.overall == aco_mod.STABLE
    text = aco_mod.format_report(Path("canonical.xlsx"), report)
    assert all(check.passed for check in report.checks)
    assert "OVERALL: ACO_BASELINE_STABLE" in text
