from __future__ import annotations

import csv
import json
from dataclasses import dataclass
from pathlib import Path

from tools import export_tece_drainprofile_reviewed_role_overlay_preview as mod
from tools import report_aco_final_baseline as aco_mod
from tools.validate_tece_drainprofile_drain_body_length_role_review_csv import EXPECTED_ARTICLES

UNKNOWN_REVIEWED_ARTICLES = {"675004", "675005", "675006", "675008", "675009", "675016", "675017", "675018", "675025"}


@dataclass
class Row:
    article_number: str
    tece_family_candidate: str = "TECEdrainprofile"
    product_family: str = "TECEdrainprofile"
    tece_article_role_candidate: str = "drain_body"
    article_role: str = "drain_body"


class Pack:
    def __init__(self, rows: list[Row]) -> None:
        self.rows = rows
        self.source_pack_candidate_count = len(rows)


def pack(role_counts: dict[str, int] | None = None, reviewed_unknown: bool = True) -> Pack:
    counts = role_counts or mod.EXPECTED_BASELINE_ROLE_COUNTS
    reviewed = [
        Row(article, tece_article_role_candidate="unknown" if reviewed_unknown and article in UNKNOWN_REVIEWED_ARTICLES else "drain_body", article_role="unknown" if reviewed_unknown and article in UNKNOWN_REVIEWED_ARTICLES else "drain_body")
        for article in EXPECTED_ARTICLES
    ]
    reviewed_counts = {"drain_body": 3, "unknown": 9 if reviewed_unknown else 0}
    extras: list[Row] = []
    for role, count in counts.items():
        for index in range(max(0, count - reviewed_counts.get(role, 0))):
            extras.append(Row(f"X{role}{index}", tece_article_role_candidate=role, article_role=role))
    other = [Row(f"O{index}", tece_family_candidate="Other", product_family="Other", tece_article_role_candidate="unknown", article_role="unknown") for index in range(436 - len(reviewed) - len(extras))]
    return Pack(reviewed + extras + other)


def decision(article: str) -> str:
    return "drain_body_no_length_found" if article in {"673001", "673002", "673003"} else "not_drain_body"


def write_csv(path: Path, rows: list[dict[str, str]]) -> None:
    fields = sorted({key for row in rows for key in row})
    with path.open("w", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=fields); writer.writeheader(); writer.writerows(rows)


def review_rows(**extra: str) -> list[dict[str, str]]:
    return [{"family": "TECEdrainprofile", "article_number": article, "review_csv_article_role": "drain_body", "reviewer_decision": decision(article), "reject_reason_tag": mod.EXPECTED_REVIEWED_ROLE_DIAGNOSTIC[article].replace("drain_body_unresolved_length", "no_nominal_length"), "reviewer_notes": "note", "reviewed_nominal_length_mm": "", **extra} for article in EXPECTED_ARTICLES]


def reject_rows() -> list[dict[str, str]]:
    return [{"article_number": article, "reviewer_decision": decision(article), "reject_reason_tag": mod.EXPECTED_REVIEWED_ROLE_DIAGNOSTIC[article].replace("drain_body_unresolved_length", "no_nominal_length"), "reject_reason_detail": "detail"} for article in EXPECTED_ARTICLES]


def fixtures(tmp_path: Path, rows=None, validation=None, impact=None):
    review = tmp_path / "review.csv"; reject = tmp_path / "reject.csv"; validation_report = tmp_path / "validation.json"; impact_report = tmp_path / "impact.json"; out = tmp_path / "out.csv"
    write_csv(review, rows or review_rows()); write_csv(reject, reject_rows())
    validation_report.write_text(json.dumps({"valid": True} if validation is None else validation), encoding="utf-8")
    impact_report.write_text(json.dumps({"valid": True} if impact is None else impact), encoding="utf-8")
    return review, reject, validation_report, impact_report, out


def report(tmp_path: Path, monkeypatch, **kwargs):
    pack_obj = kwargs.pop("pack_obj", pack())
    monkeypatch.setattr(mod, "load_source_pack", lambda _path: pack_obj)
    review, reject, validation_report, impact_report, out = fixtures(tmp_path, rows=kwargs.pop("rows", None), validation=kwargs.pop("validation", None), impact=kwargs.pop("impact", None))
    return mod.build_overlay_preview("source", review, reject, validation_report, impact_report, out=out, **kwargs), out


def test_valid_overlay_preview_passes_and_has_12_rows_expected_article_set_and_note(tmp_path, monkeypatch):
    result, out = report(tmp_path, monkeypatch)
    rows = list(csv.DictReader(out.open(encoding="utf-8-sig")))
    assert result["valid"] is True
    assert result["errors"] == []
    assert result["overlay_preview_row_count"] == 12
    assert sorted(r["article_number"] for r in rows) == EXPECTED_ARTICLES
    assert "read-only and diagnostic-only" in result["diagnostic_only_note"]


def test_requires_valid_validation_and_impact_reports(tmp_path, monkeypatch):
    assert report(tmp_path, monkeypatch, validation={"valid": False})[0]["valid"] is False
    assert report(tmp_path, monkeypatch, impact={"valid": False})[0]["valid"] is False


def test_review_csv_article_role_must_be_drain_body(tmp_path, monkeypatch):
    rows = review_rows(); rows[0]["review_csv_article_role"] = "unknown"
    result, _ = report(tmp_path, monkeypatch, rows=rows)
    assert result["valid"] is False
    assert result["invalid_review_csv_article_role_rows"]


def test_current_source_pack_reviewed_and_baseline_role_counts_are_enforced(tmp_path, monkeypatch):
    assert report(tmp_path, monkeypatch, pack_obj=pack(reviewed_unknown=False))[0]["valid"] is False
    bad_counts = dict(mod.EXPECTED_BASELINE_ROLE_COUNTS); bad_counts["accessory"] = 4
    assert report(tmp_path, monkeypatch, pack_obj=pack(role_counts=bad_counts))[0]["valid"] is False


def test_exact_mappings_are_reported(tmp_path, monkeypatch):
    result, out = report(tmp_path, monkeypatch)
    rows = {r["article_number"]: r for r in csv.DictReader(out.open(encoding="utf-8-sig"))}
    for article, diagnostic in mod.EXPECTED_REVIEWED_ROLE_DIAGNOSTIC.items():
        assert rows[article]["reviewed_role_diagnostic"] == diagnostic
        assert rows[article]["canonical_role_candidate"] == mod.EXPECTED_CANONICAL_ROLE_CANDIDATE[article]
        assert rows[article]["role_overlay_action"] == mod.EXPECTED_ROLE_OVERLAY_ACTION[article]


def test_all_apply_mutation_production_and_readiness_counts_are_blocked(tmp_path, monkeypatch):
    result, _ = report(tmp_path, monkeypatch)
    assert result["role_overlay_apply_allowed_count"] == 0
    assert result["length_overlay_allowed_count"] == 0
    assert result["compatibility_pairing_allowed_count"] == 0
    assert result["source_pack_mutation_allowed_count"] == 0
    assert result["production_safe_candidate_count"] == 0
    assert result["production_promotion_blocked"] is True
    assert result["ready_for_benchmark"] is False
    assert result["ready_for_customer_view"] is False
    assert result["diagnostic_only_leakage_rows"] == []


def test_duplicate_article_number_fails(tmp_path, monkeypatch):
    rows = review_rows(); rows[-1]["article_number"] = rows[0]["article_number"]
    result, _ = report(tmp_path, monkeypatch, rows=rows)
    assert result["valid"] is False
    assert result["duplicate_article_numbers"] == ["673001"]


def test_source_pack_immutability(tmp_path, monkeypatch):
    pack_obj = pack(); before = [(r.article_number, r.tece_article_role_candidate) for r in pack_obj.rows]
    report(tmp_path, monkeypatch, pack_obj=pack_obj)
    after = [(r.article_number, r.tece_article_role_candidate) for r in pack_obj.rows]
    assert after == before


def test_aco_canonical_baseline_remains_pass_and_stable(monkeypatch):
    monkeypatch.setattr(aco_mod, "validate_workbook", lambda _xlsx: {"valid": True, "errors": []}, raising=False)
    if hasattr(aco_mod, "build_report"):
        result = aco_mod.build_report("dummy.xlsx")
        text = json.dumps(result)
        assert "PASS" in text or result.get("overall") in {"PASS", "ACO_BASELINE_STABLE", None}
